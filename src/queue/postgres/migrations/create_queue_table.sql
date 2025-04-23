-- Defines the table structure for the outbox pattern queue.
-- This table stores events temporarily before they are relayed to the message broker.

CREATE TABLE IF NOT EXISTS outbox_queue
(
    id                  uuid        NOT NULL PRIMARY KEY,
    aggregate_id        uuid        NOT NULL,
    event_type          text        NOT NULL,
    payload             jsonb       NOT NULL,
    status              text        NOT NULL DEFAULT 'PENDING',
    created_at          timestamptz NOT NULL DEFAULT NOW(),
    updated_at          timestamptz NOT NULL DEFAULT NOW(),
    processing_attempts integer     NOT NULL DEFAULT 0,
    last_error          text        NULL
);

ALTER TABLE outbox_queue
    DROP CONSTRAINT IF EXISTS status_check;
ALTER TABLE outbox_queue
    ADD CONSTRAINT status_check CHECK (status IN ('PENDING', 'PROCESSING', 'SENT', 'FAILED'));

CREATE INDEX IF NOT EXISTS idx_outbox_queue_polling ON outbox_queue (status, updated_at);
