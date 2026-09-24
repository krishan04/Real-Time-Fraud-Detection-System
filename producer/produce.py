"""
Deterministic real-time transaction generator.

Emits a steady stream of realistic transactions to the `transactions` Kafka
topic. Mix:
  * normal-user-*   -> home-city, personal-spend patterns at a low per-user
                       cadence (>= 300s gap), so the fraud rules' 5-minute
                       window assumptions hold for genuine traffic
  * fraud-user-*    -> a vetted user does small spends most of the time, then a
                       deterministic BURST of large amounts across different
                       locations (rules 1-4 fire reliably during every burst).

Event schema:
    {
      "transaction_id": "txn-000001",
      "user_id":        "user-042",
      "merchant_id":    "merchant-17",
      "amount":         12450.50,
      "location":       "Bengaluru",
      "timestamp":      "2026-09-23T14:50:12.123456+00:00"
    }
"""
from __future__ import annotations

import argparse
import heapq
import json
import os
import random
import signal
import sys
import time
from datetime import datetime, timedelta, timezone

from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
TOPIC = os.getenv("TOPIC_TRANSACTIONS", "transactions")

DEFAULT_RATE = int(os.getenv("PRODUCER_RATE", "20"))
MAX_EVENTS = int(os.getenv("PRODUCER_MAX_EVENTS", "20000"))
BURST_EVERY = int(os.getenv("FRAUD_BURST_EVERY", "1500"))

N_NORMAL_USERS = int(os.getenv("PRODUCER_NORMAL_USERS", "14000"))
N_FRAUD_USERS = 10
SEED = int(os.getenv("PRODUCER_SEED", "42"))

NORMAL_MIN_GAP_S = float(os.getenv("PRODUCER_MIN_GAP_S", "300"))
NORMAL_MAX_GAP_S = float(os.getenv("PRODUCER_MAX_GAP_S", "900"))
HOME_PROB = float(os.getenv("PRODUCER_HOME_PROB", "0.85"))
MEDIUM_SPEND_PROB = float(os.getenv("PRODUCER_MEDIUM_SPEND_PROB", "0.04"))

LOCATIONS = [
    "Bengaluru", "Mumbai", "Delhi", "Hyderabad",
    "Chennai", "Pune", "Kolkata", "Jaipur",
]
SMALL_LOCATIONS = ["Bengaluru", "Mumbai", "Hyderabad", "Chennai"]
MERCHANTS = [f"merchant-{i:02d}" for i in range(1, 31)]


def now() -> datetime:
    return datetime.now(timezone.utc)


def iso(ts: datetime) -> str:
    return ts.isoformat(timespec="microseconds")


class Generator:
    def __init__(self, seed: int = SEED):
        self.rng = random.Random(seed)
        self.counter = 0
        self.fraud_index = 0
        # Process-scoped prefix -> transaction_ids stay globally unique even
        # when the producer restarts with the same deterministic seed.
        self.run_id = f"{os.getpid():x}{int(time.time()):x}"

        # Per-user deterministic profile: home city, personal spend mean and a
        # minimum inter-transaction gap. The gap is >= 5 minutes, so genuine
        # users can never trip the R2/R3 5-minute window rules by construction.
        # Fraud users get the same sparse, low-value profile (their "genuine"
        # history) so their 24h baseline stays small and burst amounts are
        # anomalous against it (rule R4).
        self.home = {}
        self.mean = {}
        self.gap = {}
        self._user_heap = []
        for u in range(1, N_NORMAL_USERS + 1):
            uid = f"normal-user-{u:04d}"
            profile = random.Random(seed + u * 1_000_003)
            self.home[uid] = profile.choice(LOCATIONS)
            self.mean[uid] = profile.uniform(1500, 3500)
            self.gap[uid] = profile.uniform(NORMAL_MIN_GAP_S, NORMAL_MAX_GAP_S)
            heapq.heappush(self._user_heap, (0.0, uid))
        for i in range(1, N_FRAUD_USERS + 1):
            uid = f"fraud-user-{i:03d}"
            profile = random.Random(seed + 10_000_000 + i * 7919)
            self.home[uid] = profile.choice(SMALL_LOCATIONS)
            self.mean[uid] = 0.0
            self.gap[uid] = profile.uniform(500, 900)
            heapq.heappush(self._user_heap, (0.0, uid))

    def _txid(self) -> str:
        self.counter += 1
        return f"txn-{self.run_id}-{self.counter:08d}"

    def _pick_normal_user(self, t: float) -> str:
        """Return the user_id of a user that is due to transact at time `t`.

        Enforces each user's minimum gap: already-due users are preferred; a
        user is only ever compacted early (rare) if nobody at all is due.
        """
        popped = []
        best = None
        for _ in range(len(self._user_heap)):
            next_at, uid = heapq.heappop(self._user_heap)
            popped.append((next_at, uid))
            if next_at <= t:
                best = (next_at, uid)
                break
        if best is None:
            best = min(popped)
        for next_at, uid in popped:
            if uid != best[1]:
                heapq.heappush(self._user_heap, (next_at, uid))
        heapq.heappush(self._user_heap, (t + self.gap[best[1]], best[1]))
        return best[1]

    def normal_transaction(self, t: float) -> dict:
        user = self._pick_normal_user(t)
        home = self.home[user]
        if user.startswith("fraud-user"):  # off-duty fraud user, sparse small spend
            return self._new_tx(user, round(self.rng.uniform(1200, 3000), 2), home)
        mean = self.mean[user]
        if self.rng.random() < HOME_PROB:
            location = home
        else:
            location = self.rng.choice([c for c in LOCATIONS if c != home])
        r = self.rng.random()
        if r < MEDIUM_SPEND_PROB:  # occasional big splurge -> exercises rule-4
            amount = round(self.rng.uniform(15000, 42000), 2)
        else:
            amount = round(abs(self.rng.gauss(mean, mean * 0.45)) + 50, 2)
        return self._new_tx(user, amount, location)

    def _new_tx(self, user, amount, location):
        return {
            "transaction_id": self._txid(),
            "user_id": user,
            "merchant_id": self.rng.choice(MERCHANTS),
            "amount": amount,
            "location": location,
            "timestamp": iso(now()),
        }

    def fraud_burst(self) -> list[dict]:
        """3-4 large transactions from one fraud user inside the same window."""
        user = f"fraud-user-{self.fraud_index + 1:03d}"
        self.fraud_index = (self.fraud_index + 1) % N_FRAUD_USERS

        # Event timestamps stay ~near "now" (0-2s in the past) so the 5s
        # streaming watermark does not discard the burst as late data.
        base = now() - timedelta(seconds=self.rng.randint(0, 2))
        burst = [
            {"amount": [1500.0, 2200.0, 1850.0], "loc": self.rng.choice(SMALL_LOCATIONS)},
            {"amount": [85000.0, 91000.0, 78000.0, 120000.0], "loc": "Delhi"},
            {"amount": [64000.0, 99000.0, 87000.0, 73000.0], "loc": "Mumbai"},
        ]
        events = []
        step = 0
        for slot in burst:
            for amount in slot["amount"]:
                base_ts = base + timedelta(seconds=step)
                events.append({
                    "transaction_id": self._txid(),
                    "user_id": user,
                    "merchant_id": self.rng.choice(MERCHANTS),
                    "amount": amount,
                    "location": slot["loc"],
                    "timestamp": iso(base_ts),
                })
                step += 1
        return events


def build_producer() -> KafkaProducer:
    def _connect():
        return KafkaProducer(
            bootstrap_servers=BOOTSTRAP,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            key_serializer=lambda k: k.encode("utf-8"),
            acks="all",
            linger_ms=20,
            retries=5,
            request_timeout_ms=5000,
        )

    for attempt in range(1, 61):
        try:
            return _connect()
        except NoBrokersAvailable:
            print(f"[producer] kafka not reachable yet (attempt {attempt}/60), retrying...")
            time.sleep(3)
    raise SystemExit("could not connect to kafka")


def run(rate: int, max_events: int, oneshot: bool) -> None:
    gen = Generator()
    producer = build_producer()
    stop = False

    def _handle(*_):
        nonlocal stop
        stop = True

    signal.signal(signal.SIGTERM, _handle)
    signal.signal(signal.SIGINT, _handle)

    if oneshot:
        for ev in gen.fraud_burst():
            producer.send(TOPIC, key=ev["user_id"], value=ev)
        producer.flush()
        print(f"[producer] oneshot burst sent ({gen.counter} events)", flush=True)
        return

    interval = 1.0 / rate if rate > 0 else 0.0
    sent = 0
    next_burst_at = max(10, BURST_EVERY)
    next_report = time.monotonic() + 5.0

    print(f"[producer] streaming to {TOPIC} at {rate} ev/s "
          f"(burst every ~{BURST_EVERY} slots, {N_NORMAL_USERS} normal users)",
          flush=True)
    while not stop and sent < max_events:
        t = sent * interval
        if sent >= next_burst_at:
            chunk = gen.fraud_burst()
            next_burst_at = next_burst_at + BURST_EVERY + gen.rng.randint(-20, 40)
            for ev in chunk:
                producer.send(TOPIC, key=ev["user_id"], value=ev)
            sent += len(chunk)
            print(f"[producer] fraud burst: {chunk[0]['user_id']} x{len(chunk)} "
                  f"(up to \u20b9{int(max(e['amount'] for e in chunk)):,})", flush=True)
        else:
            producer.send(TOPIC, key="normal-user", value=gen.normal_transaction(t))
            sent += 1

        now_m = time.monotonic()
        if now_m >= next_report:
            print(f"[producer] {sent}/{max_events} events sent", flush=True)
            next_report = now_m + 5.0

        producer.flush()
        if interval:
            time.sleep(interval)

    producer.flush()
    print(f"[producer] done: {sent} events sent to {TOPIC}", flush=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Fake transaction producer")
    parser.add_argument("--rate", type=int, default=DEFAULT_RATE)
    parser.add_argument("--max-events", type=int, default=MAX_EVENTS)
    parser.add_argument("--oneshot", action="store_true",
                        help="emit a single fraud burst then exit")
    args = parser.parse_args()

    run(rate=args.rate, max_events=args.max_events, oneshot=args.oneshot)