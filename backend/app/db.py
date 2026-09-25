import asyncpg

from . import config


async def create_pool() -> asyncpg.Pool:
    return await asyncpg.create_pool(
        host=config.POSTGRES_HOST,
        port=config.POSTGRES_PORT,
        user=config.POSTGRES_USER,
        password=config.POSTGRES_PASSWORD,
        database=config.POSTGRES_DB,
        min_size=1,
        max_size=10,
    )


def _f(value):
    return float(value) if value is not None else None


async def summary(conn):
    row = await conn.fetchrow(
        """
        SELECT
          (SELECT count(*) FROM transactions) AS transactions_total,
          (SELECT count(*) FROM transactions WHERE is_flagged) AS transactions_flagged,
          (SELECT COALESCE(sum(amount), 0) FROM transactions) AS amount_total,
          (SELECT COALESCE(avg(amount), 0) FROM transactions) AS amount_avg,
          (SELECT count(*) FROM fraud_alerts) AS alerts_total,
          (SELECT count(*) FROM transactions
             WHERE event_ts >= now() - interval '1 minute') AS txns_last_minute,
          (SELECT count(*) FROM fraud_alerts
             WHERE detected_ts >= now() - interval '1 minute') AS alerts_last_minute
        """
    )
    sev_rows = await conn.fetch(
        "SELECT severity, count(*)::int AS n FROM fraud_alerts GROUP BY severity"
    )
    by_severity = {"HIGH": 0, "MEDIUM": 0, "LOW": 0}
    for s in sev_rows:
        by_severity[s["severity"]] = s["n"]
    total = row["transactions_total"]
    return {
        "transactions_total": total,
        "transactions_flagged": row["transactions_flagged"],
        "flagged_rate": round(row["transactions_flagged"] / total, 4) if total else 0.0,
        "amount_total": _f(row["amount_total"]),
        "amount_avg": _f(row["amount_avg"]),
        "alerts_total": row["alerts_total"],
        "alerts_by_severity": by_severity,
        "txns_last_minute": row["txns_last_minute"],
        "alerts_last_minute": row["alerts_last_minute"],
    }


async def transactions(conn, limit: int, offset: int, flagged=None, user: str | None = None):
    clauses: list[str] = []
    args: list = []
    if flagged is not None:
        args.append(flagged)
        clauses.append(f"is_flagged = ${len(args)}")
    if user:
        args.append(user)
        clauses.append(f"user_id = ${len(args)}")
    where = f" WHERE {' AND '.join(clauses)}" if clauses else ""
    total = await conn.fetchval(f"SELECT count(*) FROM transactions{where}", *args)
    rows = await conn.fetch(
        f"""
        SELECT transaction_id, user_id, merchant_id, amount, location,
               event_ts, is_flagged, fraud_rules
        FROM transactions{where}
        ORDER BY event_ts DESC
        LIMIT ${len(args) + 1} OFFSET ${len(args) + 2}
        """,
        *args,
        limit,
        offset,
    )
    return {
        "total": total,
        "limit": limit,
        "offset": offset,
        "items": [
            {
                "transaction_id": r["transaction_id"],
                "user_id": r["user_id"],
                "merchant_id": r["merchant_id"],
                "amount": _f(r["amount"]),
                "location": r["location"],
                "event_ts": r["event_ts"],
                "is_flagged": r["is_flagged"],
                "fraud_rules": r["fraud_rules"],
            }
            for r in rows
        ],
    }


async def alerts(conn, limit: int, offset: int, severity: str | None = None, reason: str | None = None):
    clauses: list[str] = []
    args: list = []
    if severity:
        args.append(severity)
        clauses.append(f"severity = ${len(args)}")
    if reason:
        rules = [r.strip() for r in reason.split(",") if r.strip()]
        if rules:
            args.append(rules)
            clauses.append(f"reason && ${len(args)}::text[]")
    where = f" WHERE {' AND '.join(clauses)}" if clauses else ""
    total = await conn.fetchval(f"SELECT count(*) FROM fraud_alerts{where}", *args)
    rows = await conn.fetch(
        f"""
        SELECT alert_id, transaction_id, user_id, amount, location,
               score, severity, reason, detected_ts
        FROM fraud_alerts{where}
        ORDER BY detected_ts DESC, alert_id DESC
        LIMIT ${len(args) + 1} OFFSET ${len(args) + 2}
        """,
        *args,
        limit,
        offset,
    )
    return {
        "total": total,
        "limit": limit,
        "offset": offset,
        "items": [
            {
                "alert_id": r["alert_id"],
                "transaction_id": r["transaction_id"],
                "user_id": r["user_id"],
                "amount": _f(r["amount"]),
                "location": r["location"],
                "score": _f(r["score"]),
                "severity": r["severity"],
                "reason": r["reason"],
                "detected_ts": r["detected_ts"],
            }
            for r in rows
        ],
    }


async def minute_metrics(conn, minutes: int, cap: int = 5000):
    rows = await conn.fetch(
        """
        SELECT window_start, txn_count, total_amount, avg_amount,
               max_amount, unique_locations
        FROM per_minute_metrics
        WHERE window_start >= now() - make_interval(mins => $1)
        ORDER BY window_start
        LIMIT $2
        """,
        minutes,
        cap,
    )
    return [
        {
            "window_start": r["window_start"],
            "txn_count": r["txn_count"],
            "total_amount": _f(r["total_amount"]),
            "avg_amount": _f(r["avg_amount"]),
            "max_amount": _f(r["max_amount"]),
            "unique_locations": r["unique_locations"],
        }
        for r in rows
    ]


async def user_window_metrics(conn, minutes: int, cap: int = 5000):
    rows = await conn.fetch(
        """
        SELECT window_start, window_end, user_id, txn_count, total_amount,
               avg_amount, max_amount, unique_locations
        FROM user_window_metrics
        WHERE window_start >= now() - make_interval(mins => $1)
        ORDER BY window_start DESC, user_id
        LIMIT $2
        """,
        minutes,
        cap,
    )
    return [
        {
            "window_start": r["window_start"],
            "window_end": r["window_end"],
            "user_id": r["user_id"],
            "txn_count": r["txn_count"],
            "total_amount": _f(r["total_amount"]),
            "avg_amount": _f(r["avg_amount"]),
            "max_amount": _f(r["max_amount"]),
            "unique_locations": r["unique_locations"],
        }
        for r in rows
    ]