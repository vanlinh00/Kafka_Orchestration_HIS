package org.his.producer;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.UUID;

/**
 * Patient ADT Producer.
 *
 * Emits Patient Admission, Transfer, and Discharge events.
 * Uses patient_id (MRN) as partition key to guarantee strict in-order chronological timeline.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class PatientAdtProducer {

    private final KafkaTemplate<String, Object> kafkaTemplate;

    private static final String TOPIC_ADT = "his.patient.adt.v1";

    public void publishAdtEvent(String patientId, String encounterId, String eventType, Object payload) {
        String eventId = UUID.randomUUID().toString();

        ProducerRecord<String, Object> record = new ProducerRecord<>(TOPIC_ADT, patientId, payload);

        // Standard CloudEvents v1.0 & Regulatory Headers
        record.headers().add("ce_id", eventId.getBytes(StandardCharsets.UTF_8));
        record.headers().add("ce_specversion", "1.0".getBytes(StandardCharsets.UTF_8));
        record.headers().add("ce_type", ("org.his.patient.adt." + eventType.toLowerCase()).getBytes(StandardCharsets.UTF_8));
        record.headers().add("ce_source", "his/pas-adt-service".getBytes(StandardCharsets.UTF_8));
        record.headers().add("ce_time", Instant.now().toString().getBytes(StandardCharsets.UTF_8));
        record.headers().add("ce_patient_id", patientId.getBytes(StandardCharsets.UTF_8));
        record.headers().add("x_data_classification", "RESTRICTED_PHI".getBytes(StandardCharsets.UTF_8));

        kafkaTemplate.send(record).whenComplete((result, ex) -> {
            if (ex == null) {
                log.info("Published ADT [{}] for patient [{}] to partition [{}], offset [{}]",
                        eventType, patientId, result.getRecordMetadata().partition(), result.getRecordMetadata().offset());
            } else {
                log.error("Failed to publish ADT [{}] for patient [{}]", eventType, patientId, ex);
            }
        });
    }
}
