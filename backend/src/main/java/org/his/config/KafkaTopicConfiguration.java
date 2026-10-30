package org.his.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.config.TopicConfig;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

import java.util.Map;

/**
 * Programmatic Kafka Topic Definitions for Hospital Information System (HIS).
 * Configures exact partitions, replication factor 3, min.insync.replicas 2, and cleanup policies.
 */
@Configuration
public class KafkaTopicConfiguration {

    private static final int DEFAULT_REPLICATION_FACTOR = 3;
    private static final String MIN_INSYNC_REPLICAS = "2";

    // 1. PATIENT DOMAIN TOPICS
    @Bean
    public NewTopic patientAdtTopic() {
        return TopicBuilder.name("his.patient.adt.v1")
                .partitions(12)
                .replicas(DEFAULT_REPLICATION_FACTOR)
                .config(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, MIN_INSYNC_REPLICAS)
                .config(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_DELETE)
                .config(TopicConfig.RETENTION_MS_CONFIG, String.valueOf(90L * 24 * 3600 * 1000)) // 90 days
                .config(TopicConfig.MAX_MESSAGE_BYTES_CONFIG, "1048576") // 1MB
                .build();
    }

    @Bean
    public NewTopic patientMasterIndexTopic() {
        return TopicBuilder.name("his.patient.master-index.v1")
                .partitions(12)
                .replicas(DEFAULT_REPLICATION_FACTOR)
                .config(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, MIN_INSYNC_REPLICAS)
                .config(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_COMPACT)
                .config(TopicConfig.DELETE_RETENTION_MS_CONFIG, "86400000") // 24h tombstone window
                .config(TopicConfig.MIN_CLEANABLE_DIRTY_RATIO_CONFIG, "0.5")
                .config(TopicConfig.MIN_COMPACTION_LAG_MS_CONFIG, "3600000") // 1 hour
                .build();
    }

    // 2. CLINICAL DOMAIN TOPICS
    @Bean
    public NewTopic clinicalOrdersTopic() {
        return TopicBuilder.name("his.clinical.cpoe-orders.v1")
                .partitions(16)
                .replicas(DEFAULT_REPLICATION_FACTOR)
                .config(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, MIN_INSYNC_REPLICAS)
                .config(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_DELETE)
                .config(TopicConfig.RETENTION_MS_CONFIG, String.valueOf(180L * 24 * 3600 * 1000))
                .build();
    }

    @Bean
    public NewTopic clinicalLabResultsTopic() {
        return TopicBuilder.name("his.clinical.lab-results.v1")
                .partitions(16)
                .replicas(DEFAULT_REPLICATION_FACTOR)
                .config(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, MIN_INSYNC_REPLICAS)
                .config(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_DELETE)
                .config(TopicConfig.RETENTION_MS_CONFIG, String.valueOf(365L * 24 * 3600 * 1000))
                .config(TopicConfig.MAX_MESSAGE_BYTES_CONFIG, "4194304") // 4MB for high-res microbiology
                .build();
    }

    @Bean
    public NewTopic vitalsTelemetryTopic() {
        return TopicBuilder.name("his.clinical.vitals-telemetry.v1")
                .partitions(64) // High partition count for 50Hz ICU sensors
                .replicas(DEFAULT_REPLICATION_FACTOR)
                .config(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, MIN_INSYNC_REPLICAS)
                .config(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_DELETE)
                .config(TopicConfig.RETENTION_MS_CONFIG, String.valueOf(3L * 24 * 3600 * 1000)) // 3 days
                .config(TopicConfig.COMPRESSION_TYPE_CONFIG, "zstd")
                .build();
    }

    // 3. PHARMACY DOMAIN TOPICS
    @Bean
    public NewTopic pharmacyPrescriptionsTopic() {
        return TopicBuilder.name("his.pharmacy.prescriptions.v1")
                .partitions(12)
                .replicas(DEFAULT_REPLICATION_FACTOR)
                .config(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, MIN_INSYNC_REPLICAS)
                .config(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_DELETE)
                .config(TopicConfig.RETENTION_MS_CONFIG, String.valueOf(180L * 24 * 3600 * 1000))
                .build();
    }

    @Bean
    public NewTopic pharmacyDispenseEventsTopic() {
        return TopicBuilder.name("his.pharmacy.dispense-events.v1")
                .partitions(12)
                .replicas(DEFAULT_REPLICATION_FACTOR)
                .config(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, MIN_INSYNC_REPLICAS)
                .config(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_DELETE)
                .config(TopicConfig.RETENTION_MS_CONFIG, String.valueOf(365L * 24 * 3600 * 1000))
                .build();
    }

    @Bean
    public NewTopic pharmacySagaCommandsTopic() {
        return TopicBuilder.name("his.pharmacy.commands.v1")
                .partitions(12)
                .replicas(DEFAULT_REPLICATION_FACTOR)
                .config(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, MIN_INSYNC_REPLICAS)
                .config(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_DELETE)
                .config(TopicConfig.RETENTION_MS_CONFIG, String.valueOf(7L * 24 * 3600 * 1000))
                .build();
    }

    @Bean
    public NewTopic pharmacyEventsTopic() {
        return TopicBuilder.name("his.pharmacy.events.v1")
                .partitions(12)
                .replicas(DEFAULT_REPLICATION_FACTOR)
                .config(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, MIN_INSYNC_REPLICAS)
                .build();
    }

    @Bean
    public NewTopic billingCommandsTopic() {
        return TopicBuilder.name("his.billing.commands.v1")
                .partitions(12)
                .replicas(DEFAULT_REPLICATION_FACTOR)
                .config(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, MIN_INSYNC_REPLICAS)
                .build();
    }

    @Bean
    public NewTopic billingEventsTopic() {
        return TopicBuilder.name("his.billing.events.v1")
                .partitions(12)
                .replicas(DEFAULT_REPLICATION_FACTOR)
                .config(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, MIN_INSYNC_REPLICAS)
                .build();
    }

    @Bean
    public NewTopic patientTimelineCommandsTopic() {
        return TopicBuilder.name("his.patient.timeline-commands.v1")
                .partitions(12)
                .replicas(DEFAULT_REPLICATION_FACTOR)
                .config(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, MIN_INSYNC_REPLICAS)
                .build();
    }

    @Bean
    public NewTopic patientEventsTopic() {
        return TopicBuilder.name("his.patient.events.v1")
                .partitions(12)
                .replicas(DEFAULT_REPLICATION_FACTOR)
                .config(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, MIN_INSYNC_REPLICAS)
                .build();
    }

    // 4. BILLING DOMAIN TOPICS
    @Bean
    public NewTopic billingClaimsTopic() {
        return TopicBuilder.name("his.billing.claims.v1")
                .partitions(12)
                .replicas(DEFAULT_REPLICATION_FACTOR)
                .config(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, MIN_INSYNC_REPLICAS)
                .config(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_DELETE)
                .config(TopicConfig.RETENTION_MS_CONFIG, String.valueOf(365L * 24 * 3600 * 1000))
                .build();
    }

    @Bean
    public NewTopic billingPreAuthTopic() {
        return TopicBuilder.name("his.billing.preauth-requests.v1")
                .partitions(8)
                .replicas(DEFAULT_REPLICATION_FACTOR)
                .config(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, MIN_INSYNC_REPLICAS)
                .config(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_DELETE)
                .config(TopicConfig.RETENTION_MS_CONFIG, String.valueOf(90L * 24 * 3600 * 1000))
                .build();
    }

    // 5. DEAD LETTER TOPICS (.DLT)
    @Bean
    public NewTopic patientAdtDltTopic() {
        return TopicBuilder.name("his.patient.adt.v1.DLT")
                .partitions(12)
                .replicas(DEFAULT_REPLICATION_FACTOR)
                .config(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, MIN_INSYNC_REPLICAS)
                .config(TopicConfig.RETENTION_MS_CONFIG, String.valueOf(30L * 24 * 3600 * 1000))
                .build();
    }

    @Bean
    public NewTopic pharmacyPrescriptionsDltTopic() {
        return TopicBuilder.name("his.pharmacy.prescriptions.v1.DLT")
                .partitions(12)
                .replicas(DEFAULT_REPLICATION_FACTOR)
                .config(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, MIN_INSYNC_REPLICAS)
                .config(TopicConfig.RETENTION_MS_CONFIG, String.valueOf(30L * 24 * 3600 * 1000))
                .build();
    }
}
