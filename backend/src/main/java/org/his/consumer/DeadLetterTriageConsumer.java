package org.his.consumer;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.UUID;

/**
 * Dead Letter Queue (DLQ) Audit & Incident Remediation Consumer.
 *
 * Captures poisoned HL7 v2 and malformed FHIR records routed to *.DLT topics,
 * stores the exception headers, and maintains regulatory audit trails.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class DeadLetterTriageConsumer {

    private final JdbcTemplate jdbcTemplate;

    @KafkaListener(
            topicPattern = "his\\..*\\.DLT",
            groupId = "his-dlq-audit-collector",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void consumeDlqRecord(ConsumerRecord<String, Object> record, Acknowledgment ack) {
        String dlqId = "DLQ-" + UUID.randomUUID();
        String originalTopic = extractHeader(record.headers(), "kafka_original-topic", record.topic().replace(".DLT", ""));
        String exceptionClass = extractHeader(record.headers(), "kafka_exception-fqcn", "UnknownException");
        String exceptionMessage = extractHeader(record.headers(), "kafka_exception-message", "No message provided");
        String stackTrace = extractHeader(record.headers(), "kafka_exception-stacktrace", "");

        log.error("POISON RECORD CAPTURED: ID: {}, Original Topic: {}, Exception: {}",
                dlqId, originalTopic, exceptionMessage);

        jdbcTemplate.update(
                "INSERT INTO his_dlq_audit_log (dlq_id, original_topic, original_partition, original_offset, " +
                "message_key, exception_class, exception_message, stack_trace, raw_payload, status, created_at) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'QUARANTINED', ?)",
                dlqId, originalTopic, record.partition(), record.offset(),
                record.key(), exceptionClass, exceptionMessage, stackTrace,
                String.valueOf(record.value()), Instant.now()
        );

        ack.acknowledge();
    }

    private String extractHeader(Headers headers, String key, String defaultValue) {
        if (headers == null) return defaultValue;
        Header header = headers.lastHeader(key);
        return header != null ? new String(header.value(), StandardCharsets.UTF_8) : defaultValue;
    }
}
