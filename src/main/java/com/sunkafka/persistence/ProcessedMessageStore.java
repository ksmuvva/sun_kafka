package com.sunkafka.persistence;

import org.springframework.dao.DuplicateKeyException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

/** Remembers which Kafka records were fully processed, so a redelivered record is not linked twice. */
@Repository
public class ProcessedMessageStore {

    private final JdbcTemplate jdbc;

    public ProcessedMessageStore(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    public static String keyOf(String topic, int partition, long offset) {
        return topic + "-" + partition + "-" + offset;
    }

    public boolean isProcessed(String messageKey) {
        Integer count = jdbc.queryForObject(
                "SELECT COUNT(*) FROM processed_message WHERE message_key = ?", Integer.class, messageKey);
        return count != null && count > 0;
    }

    /** Safe to call twice for the same key. */
    public void record(String messageKey, String orderId) {
        try {
            jdbc.update("INSERT INTO processed_message (message_key, order_id) VALUES (?, ?)", messageKey, orderId);
        } catch (DuplicateKeyException alreadyRecorded) {
            // another delivery of the same record got there first: nothing to do
        }
    }

    public long countForOrder(String orderId) {
        Long count = jdbc.queryForObject(
                "SELECT COUNT(*) FROM processed_message WHERE order_id = ?", Long.class, orderId);
        return count == null ? 0 : count;
    }
}
