package org.his.saga;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.his.saga.dto.SagaCommandsAndEvents.*;
import org.his.saga.model.PharmacySagaState;
import org.his.saga.model.SagaStatus;
import org.his.saga.repository.SagaStateRepository;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.UUID;

/**
 * Stateful Pharmacy Order Saga Orchestrator.
 *
 * Implements the Orchestrated Saga Pattern in Spring Boot 3.4 / Apache Kafka.
 * Workflow: Doctor Places Order -> Inventory Check -> Billing Pre-Auth -> Dispense Dispatch
 * Failure handling: Automated compensation rollbacks across Inventory & Billing.
 */
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

        // 1. Persist initial Saga state in PostgreSQL
        PharmacySagaState state = PharmacySagaState.builder()
                .sagaId(sagaId)
                .orderId(request.getOrderId())
                .patientId(request.getPatientId())
                .medicationCode(request.getMedicationCode())
                .quantity(request.getQuantity())
                .estimatedCost(request.getEstimatedCost())
                .status(SagaStatus.INVENTORY_RESERVATION_PENDING)
                .build();
        sagaRepository.save(state);

        // 2. Command Step 1 -> Reserve Medication Stock in Pharmacy Inventory
        ReserveInventoryCommand cmd = new ReserveInventoryCommand(
                sagaId, request.getOrderId(), request.getMedicationCode(), request.getQuantity()
        );
        kafkaTemplate.send(new ProducerRecord<>(TOPIC_COMMANDS, sagaId, cmd));

        log.info("[Saga {}] Started prescription order. Emitted ReserveInventoryCommand for {}",
                sagaId, request.getMedicationCode());
        return sagaId;
    }

    /**
     * STEP 2 Listener: Inventory Service replies with Reserved or OutOfStock.
     */
    @KafkaListener(topics = "his.pharmacy.inventory-responses.v1", groupId = "pharmacy-saga-orchestrator")
    @Transactional
    public void onInventoryEvent(InventoryResponseEvent event, Acknowledgment ack) {
        PharmacySagaState state = sagaRepository.findById(event.getSagaId())
                .orElseThrow(() -> new IllegalStateException("Unknown Saga ID: " + event.getSagaId()));

        if (event.isSuccess()) {
            state.setStatus(SagaStatus.BILLING_PREAUTH_PENDING);
            state.setInventoryLockId(event.getLockId());
            sagaRepository.save(state);

            // Step 3 Command -> Request Billing Pre-authorization
            AuthorizeBillingCommand billingCmd = new AuthorizeBillingCommand(
                    state.getSagaId(), state.getPatientId(), state.getOrderId(), state.getEstimatedCost()
            );
            kafkaTemplate.send(new ProducerRecord<>(TOPIC_COMMANDS, state.getSagaId(), billingCmd));
            log.info("[Saga {}] Stock locked (#{}). Dispatched AuthorizeBillingCommand.",
                    state.getSagaId(), event.getLockId());
        } else {
            // Failure: Medication out of stock -> Terminate Saga, notify physician
            state.setStatus(SagaStatus.FAILED_OUT_OF_STOCK);
            state.setFailureReason("Medication unavailable in pharmacy inventory: " + event.getFailureReason());
            state.setCompletedAt(Instant.now());
            sagaRepository.save(state);

            publishSagaFailureEvent(state, "OUT_OF_STOCK");
            log.warn("[Saga {}] Inventory reservation failed. Terminated.", state.getSagaId());
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
                .orElseThrow(() -> new IllegalStateException("Unknown Saga ID: " + event.getSagaId()));

        if (event.isApproved()) {
            state.setStatus(SagaStatus.DISPATCH_PENDING);
            state.setPreAuthCode(event.getPreAuthAuthorizationCode());
            sagaRepository.save(state);

            // Step 4 Command -> Dispatch to Automated Dispensing Cabinet (Pyxis/Omnicell)
            DispatchPrescriptionCommand dispatchCmd = new DispatchPrescriptionCommand(
                    state.getSagaId(), state.getOrderId(), state.getPatientId(), state.getMedicationCode()
            );
            kafkaTemplate.send(new ProducerRecord<>(TOPIC_COMMANDS, state.getSagaId(), dispatchCmd));
            log.info("[Saga {}] Pre-auth approved. Dispatched DispatchPrescriptionCommand.", state.getSagaId());
        } else {
            // COMPENSATION TRIGGER: Billing Rejected -> Rollback Inventory Lock!
            log.warn("[Saga {}] Billing denied ({}). Triggering COMPENSATION: ReleaseInventory.",
                    state.getSagaId(), event.getDenialReason());

            state.setStatus(SagaStatus.COMPENSATING_BILLING_FAILED);
            state.setFailureReason("Billing Rejected: " + event.getDenialReason());
            sagaRepository.save(state);

            // Send Compensation Event to Inventory Service
            ReleaseInventoryCommand rollbackCmd = new ReleaseInventoryCommand(
                    state.getSagaId(), state.getInventoryLockId(), state.getMedicationCode(), state.getQuantity()
            );
            kafkaTemplate.send(new ProducerRecord<>(TOPIC_COMMANDS, state.getSagaId(), rollbackCmd));

            publishSagaFailureEvent(state, "BILLING_DENIED");
        }
        ack.acknowledge();
    }

    /**
     * STEP 4 Listener: Dispense Cabinet confirms physical dispatch or reports hardware jam.
     */
    @KafkaListener(topics = "his.pharmacy.dispense-events.v1", groupId = "pharmacy-saga-orchestrator")
    @Transactional
    public void onDispenseConfirmed(PrescriptionDispatchedEvent event, Acknowledgment ack) {
        PharmacySagaState state = sagaRepository.findById(event.getSagaId())
                .orElseThrow(() -> new IllegalStateException("Unknown Saga ID: " + event.getSagaId()));

        if (event.isSuccess()) {
            state.setStatus(SagaStatus.COMPLETED);
            state.setCompletedAt(Instant.now());
            sagaRepository.save(state);

            PrescriptionFulfilledEvent successEvent = new PrescriptionFulfilledEvent(
                    state.getSagaId(), state.getOrderId(), state.getPatientId(), Instant.now()
            );
            kafkaTemplate.send(new ProducerRecord<>(TOPIC_PRESCRIPTION_EVENTS, state.getPatientId(), successEvent));
            log.info("[Saga {}] Pharmacy workflow completed successfully. Medication dispensed.", state.getSagaId());
        } else {
            // DUAL COMPENSATION TRIGGER: Pyxis Drawer Jam / Hardware fault
            log.error("[Saga {}] Pyxis hardware fault ({}). Triggering DUAL COMPENSATION.",
                    state.getSagaId(), event.getFailureReason());

            state.setStatus(SagaStatus.COMPENSATING_DISPATCH_JAM);
            state.setFailureReason("Cabinet hardware fault: " + event.getFailureReason());
            sagaRepository.save(state);

            // 1. Rollback Billing Hold
            VoidBillingHoldCommand voidBilling = new VoidBillingHoldCommand(
                    state.getSagaId(), state.getPreAuthCode(), state.getPatientId()
            );
            kafkaTemplate.send(new ProducerRecord<>(TOPIC_COMMANDS, state.getSagaId(), voidBilling));

            // 2. Return Inventory to Stock
            ReleaseInventoryCommand restock = new ReleaseInventoryCommand(
                    state.getSagaId(), state.getInventoryLockId(), state.getMedicationCode(), state.getQuantity()
            );
            kafkaTemplate.send(new ProducerRecord<>(TOPIC_COMMANDS, state.getSagaId(), restock));

            publishSagaFailureEvent(state, "HARDWARE_JAM");
        }
        ack.acknowledge();
    }

    private void publishSagaFailureEvent(PharmacySagaState state, String reasonCode) {
        PrescriptionFailedEvent failedEvent = new PrescriptionFailedEvent(
                state.getSagaId(), state.getOrderId(), state.getPatientId(), reasonCode, state.getFailureReason()
        );
        kafkaTemplate.send(new ProducerRecord<>(TOPIC_PRESCRIPTION_EVENTS, state.getPatientId(), failedEvent));
    }
}
