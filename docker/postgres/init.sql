-- ── جدول ClientConfig ──────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS client_configs (
    id                              BIGSERIAL    PRIMARY KEY,
    system_id                       VARCHAR(64)  NOT NULL UNIQUE,
    password_hash                   VARCHAR(128) NOT NULL,
    max_connections                 INT          NOT NULL DEFAULT 10,
    tps_limit                       INT          NOT NULL DEFAULT 100,
    submit_resp_message_id_type     VARCHAR(32)  NOT NULL DEFAULT 'hex',
    delivery_report_message_id_type VARCHAR(32)  NOT NULL DEFAULT 'hex',
    active                          BOOLEAN      NOT NULL DEFAULT TRUE,
    created_at                      TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

-- ── داده‌های نمونه ───────────────────────────────────────────────────────────
INSERT INTO client_configs
    (system_id, password_hash, max_connections, tps_limit)
VALUES
    ('smpp-client-1', 'hash_client1', 5,   50),
    ('smpp-client-2', 'hash_client2', 10, 100),
    ('smpp-client-3', 'hash_client3', 3,   30)
ON CONFLICT (system_id) DO NOTHING;
