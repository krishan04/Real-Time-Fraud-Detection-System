-- Real-Time Fraud Detection System — schema init (runs once on first startup)

CREATE TABLE IF NOT EXISTS transactions (
    transaction_id VARCHAR(64) PRIMARY KEY,
    user_id        VARCHAR(64) NOT NULL,
    merchant_id    VARCHAR(64),
    amount         NUMERIC(12, 2) NOT NULL,
    location       VARCHAR(64),
    event_ts       TIMESTAMPTZ NOT NULL,
    processed_ts   TIMESTAMPTZ NOT NULL DEFAULT now(),
    is_flagged     BOOLEAN NOT NULL DEFAULT FALSE,
    fraud_rules    TEXT[]
);

CREATE INDEX IF NOT EXISTS idx_transactions_user_ts
    ON transactions (user_id, event_ts);

CREATE TABLE IF NOT EXISTS fraud_alerts (
    alert_id       BIGSERIAL PRIMARY KEY,
    transaction_id VARCHAR(64) NOT NULL UNIQUE,
    user_id        VARCHAR(64) NOT NULL,
    amount         NUMERIC(12, 2) NOT NULL,
    location       VARCHAR(64),
    score          NUMERIC(4, 2) NOT NULL,
    severity       VARCHAR(16) NOT NULL,
    reason         TEXT[] NOT NULL,
    detected_ts    TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_fraud_alerts_detected_ts
    ON fraud_alerts (detected_ts DESC);

CREATE TABLE IF NOT EXISTS per_minute_metrics (
    window_start     TIMESTAMPTZ PRIMARY KEY,
    txn_count        BIGINT NOT NULL,
    total_amount     NUMERIC(14, 2) NOT NULL,
    avg_amount       NUMERIC(12, 2) NOT NULL,
    max_amount       NUMERIC(12, 2) NOT NULL,
    unique_locations INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS user_window_metrics (
    window_start     TIMESTAMPTZ NOT NULL,
    window_end       TIMESTAMPTZ NOT NULL,
    user_id          VARCHAR(64) NOT NULL,
    txn_count        BIGINT NOT NULL,
    total_amount     NUMERIC(12, 2) NOT NULL,
    avg_amount       NUMERIC(12, 2) NOT NULL,
    max_amount       NUMERIC(12, 2) NOT NULL,
    unique_locations INTEGER NOT NULL,
    PRIMARY KEY (window_start, user_id)
);