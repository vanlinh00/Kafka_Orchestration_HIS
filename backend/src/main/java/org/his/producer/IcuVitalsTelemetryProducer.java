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
 * ICU Vitals Telemetry Producer.
 *
 * Streams high-frequency sensor frames (SpO2, 12-lead ECG waveforms, arterial pressure).
 * Employs composite temporal partition salting: device_id#minute_bucket
 * to eliminate hot partition skew across 64 cluster partitions.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class IcuVitalsTelemetryProducer {

    private final KafkaTemplate<String, Object> kafkaTemplate;

    private static final String TOPIC_TELEMETRY = "his.clinical.vitals-telemetry.v1";

    public void streamTelemetryFrame(String deviceId, String patientId, Object telemetryFrame) {
        long minuteBucket = System.currentTimeMillis() / 60000;
        String compositeSaltedKey = deviceId + "#" + minuteBucket;

        ProducerRecord<String, Object> record = new ProducerRecord<>(TOPIC_TELEMETRY, compositeSaltedKey, telemetryFrame);

        record.headers().add("ce_id", UUID.randomUUID().toString().getBytes(StandardCharsets.UTF_8));
        record.headers().add("ce_type", "org.his.clinical.vitals.frame.v1".getBytes(StandardCharsets.UTF_8));
        record.headers().add("ce_source", ("his/icu-monitor/" + deviceId).getBytes(StandardCharsets.UTF_8));
        record.headers().add("ce_time", Instant.now().toString().getBytes(StandardCharsets.UTF_8));
        record.headers().add("ce_patient_id", patientId.getBytes(StandardCharsets.UTF_8));

        kafkaTemplate.send(record);
    }
}
