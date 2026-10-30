package org.his.saga;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.his.saga.dto.SagaCommandsAndEvents.*;
import org.his.saga.model.PharmacySagaState;
import org.his.saga.model.SagaStatus;
import org.his.saga.repository.SagaStateRepository;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.UUID;

/**
 * PharmacyOrderSagaOrchestrator
 *
 * Implements the 4-step Orchestrated Saga Pattern:
 * Step 1: Initiation & Stock Reservation (Pharmacy Service)
 * Step 2: Patient Billing (Billing Service / IdempotentBillingConsumer)
 * Step 3: Timeline Recording (Patient Service / PatientTimelinePartitioner)
 * Step 4: Completion (Pharmacy Service)
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class PharmacyOrderSagaOrchestrator {

    private final SagaStateRepository sagaRepository;
    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final JdbcTemplate jdbcTemplate;

    // Kafka Topics
    public static final String TOPIC_PHARMACY_COMMANDS = "his.pharmacy.commands.v1";
    public static final String TOPIC_PHARMACY_EVENTS   = "his.pharmacy.events.v1";
    public static final String TOPIC_BILLING_COMMANDS  = "his.billing.commands.v1";
    public static final String TOPIC_BILLING_EVENTS    = "his.billing.events.v1";
    public static final String TOPIC_PATIENT_COMMANDS  = "his.patient.timeline-commands.v1";
    public static final String TOPIC_PATIENT_EVENTS    = "his.patient.events.v1";
    public static final String TOPIC_COMPLETION_EVENTS = "his.pharmacy.prescriptions.v1";

    /**
     * =========================================================================
     * STEP 1: Initiation & Stock Reservation
     * =========================================================================
     * Trigger: API receives request to process prescription.
     * Orchestrator generates saga_id (e.g. SAGA-1001), persists STARTED status,
     * and sends ReserveStockCommand to Pharmacy Service.
     */
    @Transactional
    public String startPharmacyOrderSaga(ProcessPrescriptionRequest request) {
        String sagaId = "SAGA-" + UUID.randomUUID().toString().substring(0, 8).toUpperCase();
        String orderId = "ORD-" + UUID.randomUUID().toString().substring(0, 8).toUpperCase();

        log.info("[Saga {}] STEP 1 INITIATION: Triggered prescription processing for Patient: {}, Drug: {}",
                sagaId, request.getPatientId(), request.getMedicationCode());

        // 1. Create and save initial Saga State in PostgreSQL
        PharmacySagaState sagaState = PharmacySagaState.builder()
                .sagaId(sagaId)
                .orderId(orderId)
                .patientId(request.getPatientId())
                .medicationCode(request.getMedicationCode())
                .quantity(request.getQuantity())
                .estimatedCost(request.getPrice())
                .status(SagaStatus.STARTED)
                .build();
        sagaRepository.save(sagaState);

        // 2. Send command to Pharmacy Service to reserve inventory
        ReserveStockCommand command = ReserveStockCommand.builder()
                .sagaId(sagaId)
                .orderId(orderId)
                .prescriptionId(request.getPrescriptionId())
                .patientId(request.getPatientId())
                .medicationCode(request.getMedicationCode())
                .quantity(request.getQuantity())
                .price(request.getPrice())
                .build();

        sagaState.setStatus(SagaStatus.STOCK_RESERVATION_PENDING);
        sagaRepository.save(sagaState);

        kafkaTemplate.send(new ProducerRecord<>(TOPIC_PHARMACY_COMMANDS, sagaId, command));
        log.info("[Saga {}] Dispatched ReserveStockCommand to {}", sagaId, TOPIC_PHARMACY_COMMANDS);

        return sagaId;
    }

    /**
     * =========================================================================
     * STEP 2: Stock Reserved Event Handler -> Patient Billing Trigger
     * =========================================================================
     * Receives StockReservedEvent from Pharmacy Service.
     * Dispatches ChargePatientBillingCommand ($50) to Billing Service topic
     * handled by IdempotentBillingConsumer.java.
     */
    @KafkaListener(topics = TOPIC_PHARMACY_EVENTS, groupId = "pharmacy-saga-orchestrator-group")
    @Transactional
    public void onStockReservedEvent(StockReservedEvent event, Acknowledgment ack) {
        log.info("[Saga {}] Received StockReservedEvent. Success: {}", event.getSagaId(), event.isSuccess());

        PharmacySagaState state = sagaRepository.findById(event.getSagaId())
                .orElseThrow(() -> new IllegalStateException("Saga not found: " + event.getSagaId()));

        if (event.isSuccess()) {
            state.setStatus(SagaStatus.STOCK_RESERVED);
            state.setStatus(SagaStatus.BILLING_PENDING);
            sagaRepository.save(state);

            // Command Step 2: Patient Billing ($50 charge)
            ChargePatientBillingCommand billingCmd = ChargePatientBillingCommand.builder()
                    .sagaId(state.getSagaId())
                    .orderId(state.getOrderId())
                    .patientId(state.getPatientId())
                    .amount(state.getEstimatedCost()) // e.g., $50.00
                    .description("Prescription fulfillment for " + state.getMedicationCode())
                    .build();

            kafkaTemplate.send(new ProducerRecord<>(TOPIC_BILLING_COMMANDS, state.getSagaId(), billingCmd));
            log.info("[Saga {}] Dispatched ChargePatientBillingCommand (${}) to {}",
                    state.getSagaId(), state.getEstimatedCost(), TOPIC_BILLING_COMMANDS);
        } else {
            // Inventory shortage compensation
            state.setStatus(SagaStatus.FAILED_STOCK_UNAVAILABLE);
            state.setFailureReason("Medication inventory out of stock: " + event.getFailureReason());
            state.setCompletedAt(Instant.now());
            sagaRepository.save(state);
            log.warn("[Saga {}] Stock reservation failed. Saga terminated.", state.getSagaId());
        }

        ack.acknowledge();
    }

    /**
     * =========================================================================
     * STEP 3: Payment Charged Event Handler -> Timeline Recording Trigger
     * =========================================================================
     * Receives PaymentChargedEvent from Billing Service.
     * Dispatches RecordPatientTimelineCommand to Patient Service.
     * Uses patient_id as partition key via PatientTimelinePartitioner for chronological ordering.
     */
    @KafkaListener(topics = TOPIC_BILLING_EVENTS, groupId = "pharmacy-saga-orchestrator-group")
    @Transactional
    public void onPaymentChargedEvent(PaymentChargedEvent event, Acknowledgment ack) {
        log.info("[Saga {}] Received PaymentChargedEvent. Success: {}", event.getSagaId(), event.isSuccess());

        PharmacySagaState state = sagaRepository.findById(event.getSagaId())
                .orElseThrow(() -> new IllegalStateException("Saga not found: " + event.getSagaId()));

        if (event.isSuccess()) {
            state.setStatus(SagaStatus.PAYMENT_CHARGED);
            state.setPreAuthCode(event.getTransactionId());
            state.setStatus(SagaStatus.TIMELINE_PENDING);
            sagaRepository.save(state);

            // Command Step 3: Timeline Recording (Keyed by patient_id)
            RecordPatientTimelineCommand timelineCmd = RecordPatientTimelineCommand.builder()
                    .sagaId(state.getSagaId())
                    .patientId(state.getPatientId())
                    .orderId(state.getOrderId())
                    .eventType("PHARMACY_ORDER_DISPENSED")
                    .description(String.format("Prescription %s fulfilled for %s (Qty: %d, Cost: $%.2f)",
                            state.getOrderId(), state.getMedicationCode(), state.getQuantity(), state.getEstimatedCost()))
                    .occurredAt(Instant.now())
                    .build();

            // Routed via PatientTimelinePartitioner using patientId as key
            kafkaTemplate.send(new ProducerRecord<>(TOPIC_PATIENT_COMMANDS, state.getPatientId(), timelineCmd));
            log.info("[Saga {}] Dispatched RecordPatientTimelineCommand for patient {} to {}",
                    state.getSagaId(), state.getPatientId(), TOPIC_PATIENT_COMMANDS);
        } else {
            // Billing compensation: Rollback reserved stock
            log.warn("[Saga {}] Billing failed ({}). Triggering COMPENSATION: ReleaseStock.",
                    state.getSagaId(), event.getDenialReason());
            state.setStatus(SagaStatus.COMPENSATING_BILLING_FAILED);
            state.setFailureReason("Billing declined: " + event.getDenialReason());
            sagaRepository.save(state);

            ReleaseStockCommand releaseCmd = ReleaseStockCommand.builder()
                    .sagaId(state.getSagaId())
                    .orderId(state.getOrderId())
                    .medicationCode(state.getMedicationCode())
                    .quantity(state.getQuantity())
                    .build();

            kafkaTemplate.send(new ProducerRecord<>(TOPIC_PHARMACY_COMMANDS, state.getSagaId(), releaseCmd));
        }

        ack.acknowledge();
    }

    /**
     * =========================================================================
     * STEP 4: Timeline Updated Event Handler -> Saga Completion
     * =========================================================================
     * Receives TimelineUpdatedEvent from Patient Service.
     * Marks pharmacy_orders status as COMPLETED and successfully ends the Saga.
     */
    @KafkaListener(topics = TOPIC_PATIENT_EVENTS, groupId = "pharmacy-saga-orchestrator-group")
    @Transactional
    public void onTimelineUpdatedEvent(TimelineUpdatedEvent event, Acknowledgment ack) {
        log.info("[Saga {}] Received TimelineUpdatedEvent. Success: {}", event.getSagaId(), event.isSuccess());

        PharmacySagaState state = sagaRepository.findById(event.getSagaId())
                .orElseThrow(() -> new IllegalStateException("Saga not found: " + event.getSagaId()));

        if (event.isSuccess()) {
            // 1. Mark Saga state as COMPLETED
            state.setStatus(SagaStatus.COMPLETED);
            state.setCompletedAt(Instant.now());
            sagaRepository.save(state);

            // 2. Update pharmacy_orders table to COMPLETED
            jdbcTemplate.update(
                    "UPDATE pharmacy_orders SET status = 'COMPLETED', updated_at = NOW() WHERE order_id = ?",
                    state.getOrderId()
            );

            // 3. Publish final terminal domain event
            PharmacyOrderCompletedEvent completedEvent = PharmacyOrderCompletedEvent.builder()
                    .sagaId(state.getSagaId())
                    .orderId(state.getOrderId())
                    .patientId(state.getPatientId())
                    .completedAt(Instant.now())
                    .build();

            kafkaTemplate.send(new ProducerRecord<>(TOPIC_COMPLETION_EVENTS, state.getPatientId(), completedEvent));

            log.info("[Saga {}] STEP 4 COMPLETION: Pharmacy order {} marked as COMPLETED. Saga finished successfully.",
                    state.getSagaId(), state.getOrderId());
        } else {
            state.setStatus(SagaStatus.FAILED_ROLLED_BACK);
            state.setFailureReason("Failed to append patient clinical timeline.");
            sagaRepository.save(state);
            log.error("[Saga {}] Timeline recording failed. Marked as FAILED_ROLLED_BACK.", state.getSagaId());
        }

        ack.acknowledge();
    }
}
