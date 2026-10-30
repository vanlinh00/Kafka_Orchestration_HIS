package org.his.consumer;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.his.saga.PharmacyOrderSagaOrchestrator;
import org.his.saga.dto.SagaCommandsAndEvents.RecordPatientTimelineCommand;
import org.his.saga.dto.SagaCommandsAndEvents.TimelineUpdatedEvent;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.util.UUID;

/**
 * Patient Service Timeline Consumer.
 *
 * Implements Step 3 of the Saga:
 * - Listens for RecordPatientTimelineCommand
 * - Partitioned by patient_id via PatientTimelinePartitioner for strict chronological order
 * - Writes clinical entry into patient_timeline_events
 * - Emits TimelineUpdatedEvent back to the orchestrator.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class PatientTimelineConsumer {

    private final JdbcTemplate jdbcTemplate;
    private final KafkaTemplate<String, Object> kafkaTemplate;

    @Transactional
    @KafkaListener(
            topics = PharmacyOrderSagaOrchestrator.TOPIC_PATIENT_COMMANDS,
            groupId = "patient-timeline-service-group",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void processPatientTimelineCommand(ConsumerRecord<String, Object> record, Acknowledgment ack) {
        Object payload = record.value();

        if (payload instanceof RecordPatientTimelineCommand cmd) {
            handleRecordTimeline(record.key(), cmd);
        }

        ack.acknowledge();
    }

    private void handleRecordTimeline(String patientKey, RecordPatientTimelineCommand cmd) {
        log.info("[Patient Service] Recording clinical timeline entry for Patient: {} on partition: {}",
                patientKey, cmd.getPatientId());

        try {
            // 1. Ensure patient master record exists in patients table
            jdbcTemplate.update(
                    "INSERT INTO patients (patient_id, full_name, date_of_birth, gender, created_at) " +
                    "VALUES (?, 'Hospitalized Patient', '1980-01-01', 'UNKNOWN', NOW()) " +
                    "ON CONFLICT (patient_id) DO NOTHING",
                    cmd.getPatientId()
            );

            // 2. Write entry into patient_timeline_events table
            String timelineId = "TL-" + UUID.randomUUID().toString().substring(0, 8).toUpperCase();
            jdbcTemplate.update(
                    "INSERT INTO patient_timeline_events (timeline_id, patient_id, saga_id, event_type, description, occurred_at) " +
                    "VALUES (?, ?, ?, ?, ?, ?)",
                    timelineId, cmd.getPatientId(), cmd.getSagaId(), cmd.getEventType(), cmd.getDescription(), cmd.getOccurredAt()
            );

            log.info("[Patient Service] Timeline event {} recorded chronologically for Patient: {}. Emitting TimelineUpdatedEvent.",
                    timelineId, cmd.getPatientId());

            // 3. Emit TimelineUpdatedEvent back to orchestrator
            TimelineUpdatedEvent event = TimelineUpdatedEvent.builder()
                    .sagaId(cmd.getSagaId())
                    .timelineId(timelineId)
                    .patientId(cmd.getPatientId())
                    .orderId(cmd.getOrderId())
                    .success(true)
                    .build();

            kafkaTemplate.send(new ProducerRecord<>(
                    PharmacyOrderSagaOrchestrator.TOPIC_PATIENT_EVENTS, cmd.getSagaId(), event));

        } catch (Exception ex) {
            log.error("[Patient Service] Failed to write timeline for saga {}", cmd.getSagaId(), ex);
            TimelineUpdatedEvent failedEvent = TimelineUpdatedEvent.builder()
                    .sagaId(cmd.getSagaId())
                    .patientId(cmd.getPatientId())
                    .orderId(cmd.getOrderId())
                    .success(false)
                    .build();

            kafkaTemplate.send(new ProducerRecord<>(
                    PharmacyOrderSagaOrchestrator.TOPIC_PATIENT_EVENTS, cmd.getSagaId(), failedEvent));
        }
    }
}
