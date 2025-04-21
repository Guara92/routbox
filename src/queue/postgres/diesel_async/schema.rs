diesel::table! {
    outbox_queue (id) {
        id -> Uuid,
        aggregate_id -> Uuid,
        event_type -> Text,
        payload -> Jsonb,
    }
}
