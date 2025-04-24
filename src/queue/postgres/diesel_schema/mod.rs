diesel::table! {
    outbox_queue (id) {
        id -> Uuid,
        aggregate_id -> Uuid,
        event_type -> Text,
        payload -> Jsonb,
        status -> Text,
        created_at -> Timestamptz,
        updated_at -> Timestamptz,
        processing_attempts -> Integer,
        last_error -> Nullable<Text>,
    }
}
