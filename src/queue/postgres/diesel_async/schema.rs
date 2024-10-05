diesel::table! {
    outbox_queue (id) {
        id -> Uuid,
        aggregate_id -> Uuid,
        event_name -> Text,
        payload -> Jsonb,
    }
}
