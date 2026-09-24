"""
M5 — Spark Structured Streaming: ingest + event-time windowed analytics + fraud engine.

One SparkSession, three streaming queries sharing the parsed/validated stream:

    Kafka -> parse -> validate ->+
                                 |--> user_window   (5-min, per user)  -> user_window_metrics
                                 |--> minute_metrics (1-min, global)    -> per_minute_metrics
                                 +--> fraud_eval    (per-user stateful) -> transactions (+ flags)
                                                                        -> fraud_alerts (Postgres)
                                                                        -> fraud_alerts (Kafka)

Event time is the transaction `timestamp`; a small watermark tolerates bounded
out-of-order arrivals before windows close. Windowed results are written with
replace-on-replay upserts so checkpoints can re-emit a window without double counting.

Fraud engine (M5): each arriving event is evaluated against the user's persisted
state BEFORE the event is merged into that state, so a transaction can never
influence its own baseline. Four independent rules are scored with fixed weights:

    R1 large amount            amount > FRAUD_AMOUNT_THRESHOLD
    R2 high cadence            >= FRAUD_MIN_TRANSACTIONS txns in FRAUD_WINDOW_MINUTES
    R3 location hopping        >= FRAUD_MIN_LOCATIONS distinct locations in the window
    R4 baseline deviation      amount > FRAUD_BASELINE_MULTIPLIER x mean(past 24h)

    score      = min(1.0, sum(fired rule weights))
    severity   = HIGH (>=0.70) / MEDIUM (>=0.35) / LOW (<0.35)
    reason     = sorted list of fired rule ids

Flagged events are (1) upserted into `transactions` with is_flagged/fraud_rules,
(2) inserted into `fraud_alerts`, and (3) published to the fraud_alerts Kafka topic
for the M6 dashboard. Flag state only ever accumulates — a replayed normal event
can never un-flag a previously flagged transaction.
"""
from __future__ import annotations

import json
import logging
import math
import os
from datetime import datetime, timezone

import pandas as pd
import psycopg2
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.streaming.state import GroupStateTimeout
from pyspark.sql.types import DecimalType, MapType, StringType, StructField, StructType

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
TOPIC = os.getenv("TOPIC_TRANSACTIONS", "transactions")
ALERT_TOPIC = os.getenv("TOPIC_ALERTS", "fraud_alerts")
CHECKPOINT_ROOT = os.getenv("SPARK_CHECKPOINT_DIR", "/opt/checkpoints")

ANALYTICS_WATERMARK = os.getenv("ANALYTICS_WATERMARK", "5 seconds")
USER_WINDOW = os.getenv("USER_WINDOW", "5 minutes")
MINUTE_WINDOW = os.getenv("MINUTE_WINDOW", "1 minute")

DB_HOST = os.getenv("POSTGRES_HOST", "postgres")
DB_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
DB_USER = os.getenv("POSTGRES_USER", "fraud")
DB_PASSWORD = os.getenv("POSTGRES_PASSWORD", "fraud")
DB_NAME = os.getenv("POSTGRES_DB", "frauddb")

# --- Fraud engine knobs (approved defaults) -----------------------------------
FRAUD_AMOUNT_THRESHOLD = float(os.getenv("FRAUD_AMOUNT_THRESHOLD", "50000"))
FRAUD_WINDOW_MINUTES = int(os.getenv("FRAUD_WINDOW_MINUTES", "5"))
FRAUD_MIN_TRANSACTIONS = int(os.getenv("FRAUD_MIN_TRANSACTIONS", "3"))
FRAUD_MIN_LOCATIONS = int(os.getenv("FRAUD_MIN_LOCATIONS", "2"))
FRAUD_LOOKBACK_HOURS = int(os.getenv("FRAUD_BASELINE_LOOKBACK_HOURS", "24"))
FRAUD_BASELINE_MULTIPLIER = float(os.getenv("FRAUD_BASELINE_MULTIPLIER", "3.0"))
FRAUD_WEIGHTS = {
    "R1": float(os.getenv("FRAUD_WEIGHT_R1", "0.35")),
    "R2": float(os.getenv("FRAUD_WEIGHT_R2", "0.25")),
    "R3": float(os.getenv("FRAUD_WEIGHT_R3", "0.20")),
    "R4": float(os.getenv("FRAUD_WEIGHT_R4", "0.20")),
}
FRAUD_HIGH_CUTOFF = float(os.getenv("FRAUD_HIGH_CUTOFF", "0.70"))
FRAUD_MEDIUM_CUTOFF = float(os.getenv("FRAUD_MEDIUM_CUTOFF", "0.35"))

FRAUD_WINDOW_MS = FRAUD_WINDOW_MINUTES * 60_000
FRAUD_LOOKBACK_MS = FRAUD_LOOKBACK_HOURS * 3_600_000

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("fraud.stream")

TXN_SCHEMA = StructType([
    StructField("transaction_id", StringType()),
    StructField("user_id", StringType()),
    StructField("merchant_id", StringType()),
    StructField("amount", DecimalType(12, 2)),
    StructField("location", StringType()),
    StructField("timestamp", StringType()),
])

EVAL_OUTPUT_SCHEMA = (
    "transaction_id string, user_id string, merchant_id string, amount double, "
    "location string, event_ts timestamp, "
    "r1 boolean, r2 boolean, r3 boolean, r4 boolean, "
    "score double, severity string, reason array<string>"
)
EVAL_STATE_SCHEMA = "events array<struct<ts:bigint, amount:double, location:string>>"

TXNS_UPSERT_SQL = """
    INSERT INTO transactions
        (transaction_id, user_id, merchant_id, amount, location, event_ts,
         processed_ts, is_flagged, fraud_rules)
    VALUES (%s, %s, %s, %s, %s, %s, now(), %s, %s)
    ON CONFLICT (transaction_id) DO UPDATE SET
        user_id = EXCLUDED.user_id,
        merchant_id = EXCLUDED.merchant_id,
        amount = EXCLUDED.amount,
        location = EXCLUDED.location,
        event_ts = EXCLUDED.event_ts,
        is_flagged = transactions.is_flagged OR EXCLUDED.is_flagged,
        fraud_rules = CASE
            WHEN EXCLUDED.is_flagged THEN COALESCE(transactions.fraud_rules, EXCLUDED.fraud_rules)
            ELSE transactions.fraud_rules
        END
"""

ALERT_UPSERT_SQL = """
    INSERT INTO fraud_alerts
        (transaction_id, user_id, amount, location, score, severity, reason, detected_ts)
    VALUES (%s, %s, %s, %s, %s, %s, %s, now())
    ON CONFLICT (transaction_id) DO NOTHING
"""

MINUTE_UPSERT_SQL = """
    INSERT INTO per_minute_metrics
        (window_start, txn_count, total_amount, avg_amount, max_amount, unique_locations)
    VALUES (%s, %s, %s, %s, %s, %s)
    ON CONFLICT (window_start) DO UPDATE SET
        txn_count = EXCLUDED.txn_count,
        total_amount = EXCLUDED.total_amount,
        avg_amount = EXCLUDED.avg_amount,
        max_amount = EXCLUDED.max_amount,
        unique_locations = EXCLUDED.unique_locations
"""

USER_WINDOW_UPSERT_SQL = """
    INSERT INTO user_window_metrics
        (window_start, window_end, user_id, txn_count, total_amount,
         avg_amount, max_amount, unique_locations)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (window_start, user_id) DO UPDATE SET
        window_end = EXCLUDED.window_end,
        txn_count = EXCLUDED.txn_count,
        total_amount = EXCLUDED.total_amount,
        avg_amount = EXCLUDED.avg_amount,
        max_amount = EXCLUDED.max_amount,
        unique_locations = EXCLUDED.unique_locations
"""


def validate(df):
    """Return (valid, invalid) frames with a human-readable issue label per row."""
    issue = (
        F.when(F.col("transaction_id").isNull() | (F.trim(F.col("transaction_id")) == ""),
               "missing_transaction_id")
        .when(F.col("user_id").isNull(), "missing_user_id")
        .when(F.col("amount").isNull() | (F.col("amount") <= 0), "non_positive_amount")
        .when(F.col("event_ts").isNull(), "bad_timestamp")
    )
    flagged = df.withColumn("issue", issue)
    return (
        flagged.filter(F.col("issue").isNull()).drop("issue"),
        flagged.filter(F.col("issue").isNotNull()).select("issue", "transaction_id"),
    )


# --- Fraud engine: per-user stateful evaluation ---------------------------------

def _epoch_ms(pd_ts):
    """pandas Timestamp -> epoch milliseconds (naive datetime64 is UTC)."""
    return int(pd_ts.value) // 1_000_000


def _clean_str(value):
    if value is None:
        return None
    if isinstance(value, float) and math.isnan(value):
        return None
    return str(value)


def _unpack_event(event):
    """Normalise a stored state event (dict / Row / tuple) into (ms, amount, location)."""
    if isinstance(event, dict):
        return int(event["ts"]), float(event["amount"]), _clean_str(event.get("location"))
    if isinstance(event, (tuple, list)) and len(event) >= 3:
        return int(event[0]), float(event[1]), _clean_str(event[2])
    return int(event["ts"]), float(event["amount"]), _clean_str(event["location"])


def _score_and_severity(fired):
    score = round(min(1.0, sum(FRAUD_WEIGHTS[r] for r in fired)), 2)
    severity = None
    if fired:
        if score >= FRAUD_HIGH_CUTOFF:
            severity = "HIGH"
        elif score >= FRAUD_MEDIUM_CUTOFF:
            severity = "MEDIUM"
        else:
            severity = "LOW"
    return score, severity


def fraud_eval(key, pdf_iter, state):
    """
    (key, pdf_iter, state) -> iterator of per-event output rows.

    Per-group event-time stateful processing: for every arriving event, evaluate
    R1-R4 against the *existing* state, then merge the event into the state.
    A timeout (group idle >= lookback past the watermark) simply drops the state.
    """
    frames = list(pdf_iter)
    has_data = any(len(pdf) > 0 for pdf in frames)

    if state.hasTimedOut and not has_data:
        state.remove()
        return

    if not has_data:
        return

    pdf = pd.concat(frames, ignore_index=True)
    if pdf.empty:
        if state.hasTimedOut:
            state.remove()
        return
    pdf = pdf.sort_values("event_ts", kind="mergesort")

    seen = []
    option = state.getOption
    if option is not None:
        for event in option[0]:
            seen.append(_unpack_event(event))

    out_rows = []
    last_ms = None
    for r in pdf.itertuples(index=False):
        ts = _epoch_ms(r.event_ts)
        amount = float(r.amount)
        location = _clean_str(r.location)
        merchant = _clean_str(r.merchant_id)

        seen = [e for e in seen if e[0] >= ts - FRAUD_LOOKBACK_MS]
        window_events = [e for e in seen if e[0] >= ts - FRAUD_WINDOW_MS]

        r2_eligible = 1 + len(window_events)
        locations = {e[2] for e in window_events if e[2] is not None}
        if location is not None:
            locations.add(location)

        r1 = amount > FRAUD_AMOUNT_THRESHOLD
        r2 = r2_eligible >= FRAUD_MIN_TRANSACTIONS
        r3 = len(locations) >= FRAUD_MIN_LOCATIONS
        r4 = False
        if seen:
            baseline_mean = sum(e[1] for e in seen) / len(seen)
            r4 = amount > FRAUD_BASELINE_MULTIPLIER * baseline_mean

        fired = [name for name, flag in
                 (("R1", r1), ("R2", r2), ("R3", r3), ("R4", r4)) if flag]
        score, severity = _score_and_severity(fired)

        out_rows.append({
            "transaction_id": str(r.transaction_id),
            "user_id": str(r.user_id),
            "merchant_id": merchant,
            "amount": amount,
            "location": location,
            "event_ts": r.event_ts,
            "r1": r1,
            "r2": r2,
            "r3": r3,
            "r4": r4,
            "score": score,
            "severity": severity,
            "reason": fired,
        })

        seen.append((ts, amount, location))
        last_ms = ts

    state.update(([{"ts": e[0], "amount": e[1], "location": e[2]} for e in seen],))
    if last_ms is not None:
        try:
            state.setTimeoutTimestamp(last_ms + FRAUD_LOOKBACK_MS)
        except Exception:
            pass  # timer would sit in the past (edge case); state is pruned on arrival anyway

    yield pd.DataFrame(out_rows, columns=[
        "transaction_id", "user_id", "merchant_id", "amount", "location", "event_ts",
        "r1", "r2", "r3", "r4", "score", "severity", "reason",
    ])


# --- Sinks --------------------------------------------------------------------

_producer = None


def _kafka_producer():
    global _producer
    if _producer is None:
        from kafka import KafkaProducer
        _producer = KafkaProducer(
            bootstrap_servers=BOOTSTRAP,
            acks=1,
            linger_ms=0,
            key_serializer=lambda k: k.encode("utf-8"),
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        )
    return _producer


def _connect():
    return psycopg2.connect(
        host=DB_HOST, port=DB_PORT, dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD,
    )


def _upsert(rows, sql: str):
    conn = None
    try:
        conn = _connect()
        with conn.cursor() as cur:
            for row in rows:
                cur.execute(sql, tuple(row))
        conn.commit()
    except Exception:
        if conn is not None:
            conn.rollback()
        raise
    finally:
        if conn is not None:
            conn.close()


def _alert_json(row, detected_ts):
    return {
        "transaction_id": row.transaction_id,
        "user_id": row.user_id,
        "amount": float(row.amount),
        "location": row.location,
        "score": float(row.score),
        "severity": row.severity,
        "reason": list(row.reason) if row.reason else [],
        "detected_ts": detected_ts,
    }


def alerts_sink(batch_df, batch_id):
    rows = batch_df.collect()
    if not rows:
        logger.info("alerts batch %s: empty", batch_id)
        return

    conn = None
    try:
        conn = _connect()
        with conn.cursor() as cur:
            for row in rows:
                flagged = bool(row.reason)
                cur.execute(TXNS_UPSERT_SQL, (
                    str(row.transaction_id),
                    str(row.user_id),
                    row.merchant_id,
                    row.amount,
                    row.location,
                    row.event_ts,
                    flagged,
                    list(row.reason) if flagged else None,
                ))
                if flagged:
                    cur.execute(ALERT_UPSERT_SQL, (
                        str(row.transaction_id),
                        str(row.user_id),
                        row.amount,
                        row.location,
                        row.score,
                        row.severity,
                        list(row.reason),
                    ))
        conn.commit()
    except Exception:
        if conn is not None:
            conn.rollback()
        raise
    finally:
        if conn is not None:
            conn.close()

    detected_ts = datetime.now(timezone.utc).isoformat()
    producer = _kafka_producer()
    alert_count = 0
    for row in rows:
        if row.reason:
            producer.send(ALERT_TOPIC, key=str(row.user_id),
                          value=_alert_json(row, detected_ts))
            alert_count += 1
    producer.flush()

    logger.info(
        "alerts batch %s: evaluated %s rows, %s alerts (txns updated w/ flags)",
        batch_id, len(rows), alert_count,
    )


def analysis_batch(name: str, table: str, sql: str):
    def _apply(batch_df, batch_id):
        count = batch_df.count()
        if count == 0:
            return
        batch_df.foreachPartition(
            lambda rows: _upsert(rows, sql)
        )
        logger.info("%s batch %s: upserted %s rows into %s", name, batch_id, count, table)
    return _apply


def main():
    spark = SparkSession.builder.appName("fraud-streaming").getOrCreate()

    kafka_in = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", BOOTSTRAP)
        .option("subscribe", TOPIC)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
    )

    parsed = (
        kafka_in
        .selectExpr("CAST(value AS STRING) AS json")
        .select(F.from_json(F.col("json"), TXN_SCHEMA).alias("d"))
        .select("d.*")
        .withColumn("event_ts", F.to_timestamp(F.col("timestamp")))
        .drop("timestamp")
    )

    valid, _invalid = validate(parsed)

    # --- Query 1: 5-minute per-user windows ----------------------------------
    user_windowed = (
        valid
        .withWatermark("event_ts", ANALYTICS_WATERMARK)
        .groupBy(F.window("event_ts", USER_WINDOW), "user_id")
        .agg(
            F.count("*").alias("txn_count"),
            F.sum("amount").alias("total_amount"),
            F.avg("amount").alias("avg_amount"),
            F.max("amount").alias("max_amount"),
            F.approx_count_distinct("location").alias("unique_locations"),
        )
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "user_id",
            "txn_count",
            "total_amount",
            "avg_amount",
            "max_amount",
            "unique_locations",
        )
    )

    user_q = (
        user_windowed.writeStream
        .foreachBatch(analysis_batch("user_window", "user_window_metrics", USER_WINDOW_UPSERT_SQL))
        .outputMode("append")
        .option("checkpointLocation", os.path.join(CHECKPOINT_ROOT, "user_window"))
        .trigger(processingTime="2 seconds")
        .start()
    )

    # --- Query 2: 1-minute global metrics -------------------------------------
    minute_windowed = (
        valid
        .withWatermark("event_ts", ANALYTICS_WATERMARK)
        .groupBy(F.window("event_ts", MINUTE_WINDOW))
        .agg(
            F.count("*").alias("txn_count"),
            F.sum("amount").alias("total_amount"),
            F.avg("amount").alias("avg_amount"),
            F.max("amount").alias("max_amount"),
            F.approx_count_distinct("location").alias("unique_locations"),
        )
        .select(
            F.col("window.start").alias("window_start"),
            "txn_count",
            "total_amount",
            "avg_amount",
            "max_amount",
            "unique_locations",
        )
    )

    minute_q = (
        minute_windowed.writeStream
        .foreachBatch(analysis_batch("minute_metrics", "per_minute_metrics", MINUTE_UPSERT_SQL))
        .outputMode("append")
        .option("checkpointLocation", os.path.join(CHECKPOINT_ROOT, "minute_metrics"))
        .trigger(processingTime="2 seconds")
        .start()
    )

    # --- Query 3: fraud evaluation (per-user stateful, M5) --------------------
    eval_input = (
        valid
        .select(
            "transaction_id", "user_id", "merchant_id", "location", "event_ts",
            F.col("amount").cast("double").alias("amount"),
        )
        .withWatermark("event_ts", ANALYTICS_WATERMARK)
    )

    eval_df = (
        eval_input.groupBy("user_id")
        .applyInPandasWithState(
            fraud_eval,
            outputStructType=EVAL_OUTPUT_SCHEMA,
            stateStructType=EVAL_STATE_SCHEMA,
            outputMode="Update",
            timeoutConf=GroupStateTimeout.EventTimeTimeout,
        )
    )

    alerts_q = (
        eval_df.writeStream
        .foreachBatch(alerts_sink)
        .outputMode("Update")
        .option("checkpointLocation", os.path.join(CHECKPOINT_ROOT, "alerts"))
        .trigger(processingTime="2 seconds")
        .start()
    )

    logger.info(
        "streaming started: watermark=%s user_window=%s minute_window=%s "
        "fraud rules={R1>%s, R2>=%s/%smin, R3>=%s locs, R4>%sx mean/%sh} "
        "weights=%s severity=(HIGH>=%s, MEDIUM>=%s) checkpoints=%s",
        ANALYTICS_WATERMARK, USER_WINDOW, MINUTE_WINDOW,
        FRAUD_AMOUNT_THRESHOLD, FRAUD_MIN_TRANSACTIONS, FRAUD_WINDOW_MINUTES,
        FRAUD_MIN_LOCATIONS, FRAUD_BASELINE_MULTIPLIER, FRAUD_LOOKBACK_HOURS,
        FRAUD_WEIGHTS, FRAUD_HIGH_CUTOFF, FRAUD_MEDIUM_CUTOFF, CHECKPOINT_ROOT,
    )
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()