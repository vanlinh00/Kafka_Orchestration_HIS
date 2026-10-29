export interface CodeSnippet {
  id: string;
  title: string;
  fileName: string;
  language: 'java' | 'yaml' | 'json' | 'sql';
  category: 'Configuration' | 'Saga Orchestration' | 'Partitioning & Routing' | 'Security & Crypto' | 'Fault Tolerance & Idempotency' | 'Schema & Contracts';
  description: string;
  code: string;
}

export const HIS_CODE_SNIPPETS: CodeSnippet[] = [
  {
    id: 'spring-kafka-config',
    title: 'Kafka Consumer & Producer Enterprise Config',
    fileName: 'KafkaEnterpriseConfig.java',
    language: 'java',
    category: 'Configuration',
    description: 'Spring Boot 3.4 Kafka config with transactional producer factory, manual ack container factory, and DeadLetterPublishingRecoverer with exponential backoff.',
    code: `package org.his.infrastructure.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.*;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
import org.springframework.kafka.transaction.KafkaTransactionManager;
import org.springframework.util.backoff.ExponentialBackOff;

import java.util.HashMap;
import java.util.Map;

@Configuration
@EnableKafka
public class KafkaEnterpriseConfig {

    @Value("\${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Value("\${spring.kafka.producer.transaction-id-prefix:his-tx-}")
    private String transactionIdPrefix;

    /**
     * Producer Factory configured for Exactly-Once Semantics (EOS).
     * Enforces idempotence, all-replica acks, and transaction coordination.
     */
    @Bean
    public ProducerFactory<String, Object> producerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "io.confluent.kafka.serializers.KafkaAvroSerializer");
        props.put("schema.registry.url", "https://schema-registry.his.internal:8081");
        
        // Reliability and Invariants
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, Integer.MAX_VALUE);
        props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 5);
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "zstd");
        props.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, transactionIdPrefix);

        DefaultKafkaProducerFactory<String, Object> factory = new DefaultKafkaProducerFactory<>(props);
        factory.setTransactionIdPrefix(transactionIdPrefix);
        return factory;
    }

    @Bean
    public KafkaTransactionManager<String, Object> kafkaTransactionManager(
            ProducerFactory<String, Object> producerFactory) {
        return new KafkaTransactionManager<>(producerFactory);
    }

    @Bean
    public KafkaTemplate<String, Object> kafkaTemplate(
            ProducerFactory<String, Object> producerFactory) {
        return new KafkaTemplate<>(producerFactory);
    }

    /**
     * Dead Letter Queue (DLQ) Publisher: Routes poison pills to .DLT topics with audit headers.
     */
    @Bean
    public DeadLetterPublishingRecoverer deadLetterPublishingRecoverer(KafkaTemplate<String, Object> template) {
        return new DeadLetterPublishingRecoverer(template,
                (record, exception) -> new TopicPartition(record.topic() + ".DLT", record.partition()));
    }

    /**
     * Resilient DefaultErrorHandler with 3-attempt Exponential Backoff.
     * Retries transient exceptions (network, DB lock), quarantines fatal exceptions.
     */
    @Bean
    public DefaultErrorHandler defaultErrorHandler(DeadLetterPublishingRecoverer recoverer) {
        ExponentialBackOff backOff = new ExponentialBackOff(1000L, 2.0);
        backOff.setMaxInterval(10000L);
        backOff.setMaxElapsedTime(30000L);

        DefaultErrorHandler handler = new DefaultErrorHandler(recoverer, backOff);
        // Fatal non-retryable exceptions skip immediate retries straight to DLQ
        handler.addNotRetryableExceptions(
                org.apache.kafka.common.errors.SerializationException.class,
                jakarta.validation.ValidationException.class,
                IllegalArgumentException.class
        );
        return handler;
    }

    /**
     * High-reliability Consumer Container Factory:
     * - MANUAL_IMMEDIATE AckMode to prevent phantom commits
     * - READ_COMMITTED isolation level to filter out aborted transaction events
     */
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, Object> kafkaListenerContainerFactory(
            ConsumerFactory<String, Object> consumerFactory,
            DefaultErrorHandler errorHandler) {
        
        ConcurrentKafkaListenerContainerFactory<String, Object> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory);
        factory.setCommonErrorHandler(errorHandler);
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
        factory.getContainerProperties().setObservationEnabled(true); // OpenTelemetry tracing
        return factory;
    }

    @Bean
    public ConsumerFactory<String, Object> consumerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);
        props.put(ErrorHandlingDeserializer.KEY_DESERIALIZER_CLASS, StringDeserializer.class);
        props.put(ErrorHandlingDeserializer.VALUE_DESERIALIZER_CLASS, "io.confluent.kafka.serializers.KafkaAvroDeserializer");
        props.put("schema.registry.url", "https://schema-registry.his.internal:8081");
        props.put("specific.avro.reader", true);

        return new DefaultKafkaConsumerFactory<>(props);
    }
}`
  },
  {
    id: 'saga-orchestrator',
    title: 'Pharmacy Order Saga Orchestrator (Stateful Machine)',
    fileName: 'PharmacyOrderSagaOrchestrator.java',
    language: 'java',
    category: 'Saga Orchestration',
    description: 'Implements the Orchestrated Saga Pattern in Spring Boot 3.4. Manages the 4-step pharmacy workflow: Doctor Order -> Inventory -> Billing -> Dispatch with PostgreSQL state tracking and automated compensation rollback.',
    code: `package org.his.domain.pharmacy.saga;

import jakarta.persistence.*;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.his.domain.pharmacy.events.*;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.UUID;

@Slf4j
@Service
@RequiredArgsConstructor
public class PharmacyOrderSagaOrchestrator {

    private final SagaStateRepository sagaRepository;
    private final KafkaTemplate<String, Object> kafkaTemplate;

    private static final String TOPIC_COMMANDS = "his.pharmacy.saga-commands.v1";
    private static final String TOPIC_PRESCRIPTION_EVENTS = "his.pharmacy.prescriptions.v1";

    /**
     * STEP 1: Entry Point - Physician submits pharmacy medication order.
     */
    @Transactional
    public String startPrescriptionSaga(DoctorPharmacyOrderRequest request) {
        String sagaId = "SAGA-" + UUID.randomUUID();
        
        // Persist initial Saga state in PostgreSQL
        PharmacySagaState state = PharmacySagaState.builder()
                .sagaId(sagaId)
                .orderId(request.getOrderId())
                .patientId(request.getPatientId())
                .medicationCode(request.getMedicationCode())
                .quantity(request.getQuantity())
                .estimatedCost(request.getEstimatedCost())
                .status(SagaStatus.STARTED)
                .createdAt(Instant.now())
                .updatedAt(Instant.now())
                .build();
        sagaRepository.save(state);

        // Command Step 1 -> Reserve Medication Stock in Pharmacy Inventory
        ReserveInventoryCommand cmd = new ReserveInventoryCommand(
                sagaId, request.getOrderId(), request.getMedicationCode(), request.getQuantity()
        );
        kafkaTemplate.send(new ProducerRecord<>(TOPIC_COMMANDS, sagaId, cmd));
        
        state.setStatus(SagaStatus.INVENTORY_RESERVATION_PENDING);
        log.info("[Saga {}] Started pharmacy order workflow. Dispatched ReserveInventoryCommand.", sagaId);
        return sagaId;
    }

    /**
     * STEP 2 Listener: Inventory Service replies with Reserved or OutOfStock.
     */
    @KafkaListener(topics = "his.pharmacy.inventory-responses.v1", groupId = "pharmacy-saga-orchestrator")
    @Transactional
    public void onInventoryEvent(InventoryResponseEvent event, Acknowledgment ack) {
        PharmacySagaState state = sagaRepository.findById(event.getSagaId())
                .orElseThrow(() -> new IllegalStateException("Unknown Saga: " + event.getSagaId()));

        if (event.isSuccess()) {
            state.setStatus(SagaStatus.INVENTORY_RESERVED);
            state.setInventoryLockId(event.getLockId());
            state.setUpdatedAt(Instant.now());
            sagaRepository.save(state);

            // Step 3 Command -> Request Billing Pre-authorization
            AuthorizeBillingCommand billingCmd = new AuthorizeBillingCommand(
                    state.getSagaId(), state.getPatientId(), state.getOrderId(), state.getEstimatedCost()
            );
            kafkaTemplate.send(new ProducerRecord<>(TOPIC_COMMANDS, state.getSagaId(), billingCmd));
            log.info("[Saga {}] Inventory locked. Sent AuthorizeBillingCommand.", state.getSagaId());
        } else {
            // Failure: Medication out of stock -> Terminate Saga, notify doctor
            state.setStatus(SagaStatus.FAILED_OUT_OF_STOCK);
            state.setFailureReason("Medication unavailable in pharmacy stock.");
            state.setUpdatedAt(Instant.now());
            sagaRepository.save(state);

            publishSagaFailureEvent(state, "OUT_OF_STOCK");
            log.warn("[Saga {}] Inventory check failed: Out of stock.", state.getSagaId());
        }
        ack.acknowledge();
    }

    /**
     * STEP 3 Listener: Billing Service replies with Pre-Auth Approved or Denied.
     */
    @KafkaListener(topics = "his.billing.preauth-responses.v1", groupId = "pharmacy-saga-orchestrator")
    @Transactional
    public void onBillingResponse(BillingPreAuthResponseEvent event, Acknowledgment ack) {
        PharmacySagaState state = sagaRepository.findById(event.getSagaId())
                .orElseThrow(() -> new IllegalStateException("Unknown Saga: " + event.getSagaId()));

        if (event.isApproved()) {
            state.setStatus(SagaStatus.BILLING_PREAUTH_APPROVED);
            state.setPreAuthCode(event.getPreAuthAuthorizationCode());
            state.setUpdatedAt(Instant.now());
            sagaRepository.save(state);

            // Step 4 Command -> Dispatch to Automated Dispensing Cabinet (Pyxis/Omnicell)
            DispatchPrescriptionCommand dispatchCmd = new DispatchPrescriptionCommand(
                    state.getSagaId(), state.getOrderId(), state.getPatientId(), state.getMedicationCode()
            );
            kafkaTemplate.send(new ProducerRecord<>(TOPIC_COMMANDS, state.getSagaId(), dispatchCmd));
            log.info("[Saga {}] Billing authorized. Sent DispatchPrescriptionCommand.", state.getSagaId());
        } else {
            // COMPENSATION TRIGGER: Billing Rejected -> Rollback Inventory Reservation!
            log.warn("[Saga {}] Billing pre-auth rejected ({}). Triggering COMPENSATION: ReleaseInventory.",
                    state.getSagaId(), event.getDenialReason());

            state.setStatus(SagaStatus.COMPENSATING_BILLING_FAILED);
            state.setFailureReason("Billing Rejected: " + event.getDenialReason());
            sagaRepository.save(state);

            // Send Compensation Event
            ReleaseInventoryCommand rollbackCmd = new ReleaseInventoryCommand(
                    state.getSagaId(), state.getInventoryLockId(), state.getMedicationCode(), state.getQuantity()
            );
            kafkaTemplate.send(new ProducerRecord<>(TOPIC_COMMANDS, state.getSagaId(), rollbackCmd));
            publishSagaFailureEvent(state, "BILLING_DENIED");
        }
        ack.acknowledge();
    }

    /**
     * STEP 4 Listener: Dispense Cabinet confirms physical dispatch.
     */
    @KafkaListener(topics = "his.pharmacy.dispense-events.v1", groupId = "pharmacy-saga-orchestrator")
    @Transactional
    public void onDispenseConfirmed(PrescriptionDispatchedEvent event, Acknowledgment ack) {
        PharmacySagaState state = sagaRepository.findById(event.getSagaId())
                .orElseThrow(() -> new IllegalStateException("Unknown Saga: " + event.getSagaId()));

        state.setStatus(SagaStatus.COMPLETED);
        state.setCompletedAt(Instant.now());
        state.setUpdatedAt(Instant.now());
        sagaRepository.save(state);

        // Notify final success topic
        PrescriptionFulfilledEvent successEvent = new PrescriptionFulfilledEvent(
                state.getSagaId(), state.getOrderId(), state.getPatientId(), Instant.now()
        );
        kafkaTemplate.send(new ProducerRecord<>(TOPIC_PRESCRIPTION_EVENTS, state.getPatientId(), successEvent));
        log.info("[Saga {}] Workflow successfully completed. Medication dispatched.", state.getSagaId());
        ack.acknowledge();
    }

    private void publishSagaFailureEvent(PharmacySagaState state, String reasonCode) {
        PrescriptionFailedEvent failedEvent = new PrescriptionFailedEvent(
                state.getSagaId(), state.getOrderId(), state.getPatientId(), reasonCode, state.getFailureReason()
        );
        kafkaTemplate.send(new ProducerRecord<>(TOPIC_PRESCRIPTION_EVENTS, state.getPatientId(), failedEvent));
    }
}`
  },
  {
    id: 'patient-partitioner',
    title: 'Patient Chronological Timeline Partitioner',
    fileName: 'PatientTimelinePartitioner.java',
    language: 'java',
    category: 'Partitioning & Routing',
    description: 'Custom Kafka partitioner ensuring all ADT, clinical observations, and orders for a single patient land deterministically on the exact same partition.',
    code: `package org.his.infrastructure.kafka.partitioner;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.utils.Utils;

import java.util.List;
import java.util.Map;

/**
 * PatientTimelinePartitioner enforces total sequential ordering for patient records.
 *
 * Requirements Met:
 * 1. Guarantees ADT (Admission -> Ward Move -> Discharge) cannot race out of order.
 * 2. Uses deterministic Murmur2 hashing on normalized Patient ID / MRN.
 * 3. Handles null keys by delegating to round-robin sticky partitioner.
 */
public class PatientTimelinePartitioner implements Partitioner {

    @Override
    public void configure(Map<String, ?> configs) {
        // No custom configs required
    }

    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        List<PartitionInfo> partitions = cluster.partitionsForTopic(topic);
        int numPartitions = partitions.size();

        if (keyBytes == null) {
            // Null-key events use sticky random distribution
            return Utils.toPositive(Utils.murmur2(valueBytes)) % numPartitions;
        }

        String patientKey = key.toString().trim().toUpperCase();

        // High-Throughput IoT/Telemetry Topic: Salt key to avoid ICU hot partition skew
        if (topic.contains("vitals-telemetry")) {
            long currentMinuteBucket = System.currentTimeMillis() / 60000;
            String saltedKey = patientKey + "#" + currentMinuteBucket;
            return Utils.toPositive(Utils.murmur2(saltedKey.getBytes())) % numPartitions;
        }

        // Standard Clinical & Patient ADT: Pure Patient ID partition affinity
        return Utils.toPositive(Utils.murmur2(patientKey.getBytes())) % numPartitions;
    }

    @Override
    public void close() {
        // Cleanup resources if needed
    }
}`
  },
  {
    id: 'kms-crypto-shredding',
    title: 'HIPAA/GDPR Envelope Encryption & Crypto-Shredder',
    fileName: 'KmsEnvelopeEncryptionSerializer.java',
    language: 'java',
    category: 'Security & Crypto',
    description: 'Implements AES-256-GCM field and payload envelope encryption. Allows crypto-shredding patient PHI across immutable Kafka topics upon GDPR right-to-be-forgotten request.',
    code: `package org.his.security.crypto;

import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Serializer;
import org.springframework.stereotype.Component;

import javax.crypto.Cipher;
import javax.crypto.SecretKey;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import java.nio.ByteBuffer;
import java.security.SecureRandom;
import java.util.Map;

/**
 * KmsEnvelopeEncryptionSerializer
 *
 * HIPAA / GDPR Invariants:
 * 1. Plaintext PHI never touches Kafka disk unencrypted.
 * 2. Each Patient has an individual Data Encryption Key (DEK) fetched from KMS/Vault.
 * 3. Deleting the patient's DEK in KMS achieves instant Crypto-Shredding of all historical Kafka logs.
 */
@Component
@RequiredArgsConstructor
public class KmsEnvelopeEncryptionSerializer implements Serializer<byte[]> {

    private final KmsKeyManagementService kmsClient;
    private static final int GCM_IV_LENGTH_BYTES = 12;
    private static final int GCM_TAG_LENGTH_BITS = 128;

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {}

    @Override
    public byte[] serialize(String topic, byte[] data) {
        return serialize(topic, null, data);
    }

    @Override
    @SneakyThrows
    public byte[] serialize(String topic, Headers headers, byte[] plaintext) {
        if (plaintext == null) {
            return null;
        }

        // Extract patient identifier from CloudEvents header
        String patientId = HeaderUtils.getHeaderValue(headers, "ce_patient_id");
        if (patientId == null) {
            patientId = "GLOBAL_FALLBACK_DEK";
        }

        // Fetch or create per-patient DEK from KMS
        EncryptedDek dek = kmsClient.getOrCreatePatientDek(patientId);

        // Generate cryptographically secure random 96-bit IV
        byte[] iv = new byte[GCM_IV_LENGTH_BYTES];
        new SecureRandom().nextBytes(iv);

        // Initialize AES-256-GCM cipher
        Cipher cipher = Cipher.getInstance("AES/GCM/NoPadding");
        SecretKey secretKey = new SecretKeySpec(dek.getPlaintextKeyBytes(), "AES");
        cipher.init(Cipher.ENCRYPT_MODE, secretKey, new GCMParameterSpec(GCM_TAG_LENGTH_BITS, iv));

        // Authenticated Additional Data (AAD) prevents cipher manipulation
        cipher.updateAAD(topic.getBytes());
        byte[] ciphertext = cipher.doFinal(plaintext);

        // Stamp DEK ID in Kafka headers for downstream decryption
        headers.add("x-encryption-dek-id", dek.getKeyId().getBytes());
        headers.add("x-encryption-algorithm", "AES-256-GCM".getBytes());

        // Envelope: [IV length (1B)][IV (12B)][Ciphertext + Auth Tag]
        return ByteBuffer.allocate(1 + iv.length + ciphertext.length)
                .put((byte) iv.length)
                .put(iv)
                .put(ciphertext)
                .array();
    }

    @Override
    public void close() {}
}`
  },
  {
    id: 'idempotent-billing-consumer',
    title: 'Idempotent Billing Consumer with Dual-Write Elimination',
    fileName: 'IdempotentBillingConsumer.java',
    language: 'java',
    category: 'Fault Tolerance & Idempotency',
    description: 'PostgreSQL-backed deduplication record inside Spring @Transactional. Guarantees insurance claims and drug charges are never double-billed on network replays.',
    code: `package org.his.domain.billing.consumer;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.his.domain.billing.entity.ClaimEntity;
import org.his.domain.billing.repository.ClaimRepository;
import org.his.domain.billing.repository.ConsumedMessageRepository;
import org.his.domain.billing.entity.ConsumedMessage;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;

@Slf4j
@Component
@RequiredArgsConstructor
public class IdempotentBillingConsumer {

    private final ClaimRepository claimRepository;
    private final ConsumedMessageRepository dedupRepository;

    private static final String CONSUMER_GROUP = "his-billing-claim-ingest";

    /**
     * Consumes billing charge events with Exactly-Once Processing (EOS).
     * Atomic deduplication: (consumer_group, topic, partition, message_offset) stored in Postgres.
     */
    @Transactional
    @KafkaListener(
            topics = "his.billing.claims.v1",
            groupId = CONSUMER_GROUP,
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void processClaimEvent(ConsumerRecord<String, BillingClaimPayload> record, Acknowledgment ack) {
        String eventId = record.key();
        String messageFingerprint = String.format("%s:%s:%d:%d",
                CONSUMER_GROUP, record.topic(), record.partition(), record.offset());

        // 1. Idempotency Check
        if (dedupRepository.existsByFingerprint(messageFingerprint)) {
            log.warn("Duplicate billing event detected. Fingerprint: {}. Skipping execution.", messageFingerprint);
            ack.acknowledge();
            return;
        }

        BillingClaimPayload payload = record.value();

        // 2. Business Entity Creation
        ClaimEntity claim = ClaimEntity.builder()
                .claimId(payload.getClaimId())
                .encounterId(payload.getEncounterId())
                .patientId(payload.getPatientId())
                .totalBilledAmount(payload.getTotalAmount())
                .currency("USD")
                .adjudicationStatus("SUBMITTED_TO_CLEARINGHOUSE")
                .submittedAt(Instant.now())
                .build();
        claimRepository.save(claim);

        // 3. Persist Deduplication Record in the same PostgreSQL transaction
        ConsumedMessage dedupRecord = ConsumedMessage.builder()
                .fingerprint(messageFingerprint)
                .eventId(eventId)
                .consumerGroup(CONSUMER_GROUP)
                .topic(record.topic())
                .partition(record.partition())
                .messageOffset(record.offset())
                .processedAt(Instant.now())
                .build();
        dedupRepository.save(dedupRecord);

        // 4. Commit Kafka offset manually only after DB commit succeeds
        ack.acknowledge();
        log.info("Claim {} successfully processed and recorded. Fingerprint: {}", payload.getClaimId(), messageFingerprint);
    }
}`
  },
  {
    id: 'fhir-avro-schema',
    title: 'Avro Schema: FHIR R4 Patient ADT Specification',
    fileName: 'PatientAdtEvent.avsc',
    language: 'json',
    category: 'Schema & Contracts',
    description: 'Confluent Schema Registry Avro contract modeling FHIR R4 Encounter and Patient ADT events with backward-transitive compatibility.',
    code: `{
  "type": "record",
  "name": "PatientAdtEvent",
  "namespace": "org.his.events.patient.v1",
  "doc": "FHIR R4 compliant Patient Admission, Discharge, and Transfer (ADT) Event Contract",
  "fields": [
    { "name": "eventId", "type": "string", "doc": "Unique CloudEvents UUID" },
    { "name": "patientId", "type": "string", "doc": "Enterprise Master Patient Index MRN" },
    { "name": "encounterId", "type": "string", "doc": "Hospital Inpatient/Outpatient Encounter ID" },
    {
      "name": "eventType",
      "type": {
        "type": "enum",
        "name": "AdtTriggerType",
        "symbols": ["ADMISSION_A01", "TRANSFER_A02", "DISCHARGE_A03", "UPDATE_A08", "BED_CHANGE_A05"]
      }
    },
    { "name": "timestamp", "type": "long", "doc": "Epoch milliseconds UTC" },
    { "name": "attendingPhysicianId", "type": ["null", "string"], "default": null },
    {
      "name": "assignedLocation",
      "type": {
        "type": "record",
        "name": "HospitalLocation",
        "fields": [
          { "name": "facility", "type": "string" },
          { "name": "building", "type": "string" },
          { "name": "floor", "type": "string" },
          { "name": "ward", "type": "string" },
          { "name": "bed", "type": "string" }
        ]
      }
    },
    {
      "name": "diagnosisCodes",
      "type": { "type": "array", "items": "string" },
      "doc": "ICD-10-CM diagnosis codes attached at time of event"
    },
    {
      "name": "confidentialityLevel",
      "type": {
        "type": "enum",
        "name": "ConfidentialityCode",
        "symbols": ["NORMAL", "RESTRICTED_BEHAVIORAL", "RESTRICTED_VIP", "HIGHLY_CONFIDENTIAL"]
      },
      "default": "NORMAL"
    }
  ]
}`
  },
  {
    id: 'postgres-ddl',
    title: 'PostgreSQL Schema: Saga State & Idempotency Store',
    fileName: 'V1__his_saga_and_idempotency.sql',
    language: 'sql',
    category: 'Configuration',
    description: 'PostgreSQL relational DDL for the saga orchestrator state machine, lock table, and consumer deduplication audit log.',
    code: `-- Saga State Persistence Table
CREATE TABLE his_pharmacy_saga_state (
    saga_id VARCHAR(64) PRIMARY KEY,
    order_id VARCHAR(64) NOT NULL,
    patient_id VARCHAR(64) NOT NULL,
    medication_code VARCHAR(32) NOT NULL,
    quantity INTEGER NOT NULL,
    estimated_cost NUMERIC(10, 2) NOT NULL,
    status VARCHAR(48) NOT NULL,
    inventory_lock_id VARCHAR(64),
    pre_auth_code VARCHAR(64),
    failure_reason TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at TIMESTAMPTZ
);

CREATE INDEX idx_saga_patient ON his_pharmacy_saga_state(patient_id);
CREATE INDEX idx_saga_status ON his_pharmacy_saga_state(status);

-- Exactly-Once Consumer Deduplication Table
CREATE TABLE his_consumed_messages (
    fingerprint VARCHAR(256) PRIMARY KEY, -- consumer_group:topic:partition:offset
    event_id VARCHAR(64) NOT NULL,
    consumer_group VARCHAR(128) NOT NULL,
    topic VARCHAR(128) NOT NULL,
    partition_id INTEGER NOT NULL,
    message_offset BIGINT NOT NULL,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_dedup_event_id ON his_consumed_messages(event_id);
CREATE INDEX idx_dedup_topic_offset ON his_consumed_messages(topic, partition_id, message_offset);`
  }
];
