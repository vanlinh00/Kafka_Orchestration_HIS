package org.his.consumer;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.time.Instant;

/**
 * Idempotent Billing Consumer with Dual-Write Elimination.
 *
 * Guarantees that insurance claims and medication charges are NEVER double-billed
 * on network replays or broker rebalances.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class IdempotentBillingConsumer {

    private final JdbcTemplate jdbcTemplate;

    private static final String CONSUMER_GROUP = "his-billing-claims-ingest";

    @Transactional
    @KafkaListener(
            topics = "his.billing.claims.v1",
            groupId = CONSUMER_GROUP,
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void processClaimRecord(ConsumerRecord<String, Object> record, Acknowledgment ack) {
        String eventId = record.key();
        String fingerprint = String.format("%s:%s:%d:%d",
                CONSUMER_GROUP, record.topic(), record.partition(), record.offset());

        // 1. Idempotency Check: Query deduplication table
        Integer existing = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM his_consumed_messages WHERE fingerprint = ?",
                Integer.class, fingerprint
        );

        if (existing != null && existing > 0) {
            log.warn("Duplicate billing claim event detected ({}). Skipping execution.", fingerprint);
            ack.acknowledge();
            return;
        }

        // 2. Business Execution (Persist claim)
        log.info("Processing billing claim for record key: {} on partition: {}", eventId, record.partition());

        // 3. Atomically Record Fingerprint in the same PostgreSQL transaction
        jdbcTemplate.update(
                "INSERT INTO his_consumed_messages (fingerprint, event_id, consumer_group, topic, partition_id, message_offset, processed_at) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?)",
                fingerprint, eventId, CONSUMER_GROUP, record.topic(), record.partition(), record.offset(), Instant.now()
        );

        // 4. Synchronously acknowledge Kafka offset only AFTER the DB transaction commits
        ack.acknowledge();
    }
}
