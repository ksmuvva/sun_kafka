-- One row per Kafka record that was fully processed (validated and linked).
-- message_key = topic-partition-offset, so a redelivered record is recognised and not linked twice.
CREATE TABLE processed_message (
    message_key  VARCHAR(255) PRIMARY KEY,
    order_id     VARCHAR(255) NOT NULL,
    processed_at TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX idx_processed_message_order_id ON processed_message (order_id);
