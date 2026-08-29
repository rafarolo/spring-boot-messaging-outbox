CREATE TABLE IF NOT EXISTS orders (
    id VARCHAR(120) PRIMARY KEY
);

CREATE TABLE IF NOT EXISTS outbox (
    id           UUID PRIMARY KEY,
    aggregate    VARCHAR(120) NOT NULL,
    topic        VARCHAR(120) NOT NULL,
    payload      VARCHAR(4000) NOT NULL,
    created_at   TIMESTAMP NOT NULL,
    published_at TIMESTAMP
);

CREATE INDEX IF NOT EXISTS outbox_unpublished ON outbox (published_at, created_at);

CREATE TABLE IF NOT EXISTS inbox (
    message_id   UUID NOT NULL,
    consumer     VARCHAR(120) NOT NULL,
    processed_at TIMESTAMP NOT NULL,
    PRIMARY KEY (message_id, consumer)
);
