CREATE TABLE IF NOT EXISTS outbox_queue
(
    id           uuid  NOT NULL,
    aggregate_id uuid  NOT NULL,
    event_type   text  NOT NULL,
    payload      jsonb NOT NULL
)
