package org.his.consumer;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.his.saga.PharmacyOrderSagaOrchestrator;
import org.his.saga.dto.SagaCommandsAndEvents.ReleaseStockCommand;
import org.his.saga.dto.SagaCommandsAndEvents.ReserveStockCommand;
import org.his.saga.dto.SagaCommandsAndEvents.StockReservedEvent;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

/**
 * Pharmacy Service Local Transaction Consumer.
 *
 * Implements Step 1 of the Saga:
 * - Listens for ReserveStockCommand
 * - Inserts order record into pharmacy_orders with status PENDING
 * - Decrements medication count in medication_inventory
 * - Emits StockReservedEvent back to the orchestrator.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class PharmacyServiceLocalTransactionConsumer {

    private final JdbcTemplate jdbcTemplate;
    private final KafkaTemplate<String, Object> kafkaTemplate;

    @Transactional
    @KafkaListener(
            topics = PharmacyOrderSagaOrchestrator.TOPIC_PHARMACY_COMMANDS,
            groupId = "pharmacy-inventory-service-group",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void processPharmacyCommand(ConsumerRecord<String, Object> record, Acknowledgment ack) {
        Object payload = record.value();

        if (payload instanceof ReserveStockCommand cmd) {
            handleReserveStock(cmd);
        } else if (payload instanceof ReleaseStockCommand cmd) {
            handleReleaseStock(cmd);
        }

        ack.acknowledge();
    }

    private void handleReserveStock(ReserveStockCommand cmd) {
        log.info("[Pharmacy Service] Processing ReserveStockCommand for Drug: {}, Qty: {}",
                cmd.getMedicationCode(), cmd.getQuantity());

        try {
            // 1. Check stock availability and decrement medication_inventory
            int updatedRows = jdbcTemplate.update(
                    "UPDATE medication_inventory " +
                    "SET available_stock = available_stock - ?, reserved_stock = reserved_stock + ?, updated_at = NOW() " +
                    "WHERE medication_code = ? AND available_stock >= ?",
                    cmd.getQuantity(), cmd.getQuantity(), cmd.getMedicationCode(), cmd.getQuantity()
            );

            if (updatedRows == 0) {
                // Stock unavailable
                log.warn("[Pharmacy Service] Out of stock for Drug: {}", cmd.getMedicationCode());
                StockReservedEvent failureEvent = StockReservedEvent.builder()
                        .sagaId(cmd.getSagaId())
                        .orderId(cmd.getOrderId())
                        .patientId(cmd.getPatientId())
                        .medicationCode(cmd.getMedicationCode())
                        .success(false)
                        .failureReason("Insufficient inventory available in pharmacy catalog.")
                        .build();

                kafkaTemplate.send(new ProducerRecord<>(
                        PharmacyOrderSagaOrchestrator.TOPIC_PHARMACY_EVENTS, cmd.getSagaId(), failureEvent));
                return;
            }

            // 2. Insert order record into pharmacy_orders with status PENDING
            jdbcTemplate.update(
                    "INSERT INTO pharmacy_orders (order_id, saga_id, prescription_id, patient_id, medication_code, quantity, total_amount, status, created_at, updated_at) " +
                    "VALUES (?, ?, ?, ?, ?, ?, ?, 'PENDING', NOW(), NOW())",
                    cmd.getOrderId(), cmd.getSagaId(), cmd.getPrescriptionId(), cmd.getPatientId(),
                    cmd.getMedicationCode(), cmd.getQuantity(), cmd.getPrice()
            );

            log.info("[Pharmacy Service] Successfully reserved {} units of {}. Emitting StockReservedEvent.",
                    cmd.getQuantity(), cmd.getMedicationCode());

            // 3. Emit StockReservedEvent back to orchestrator
            StockReservedEvent event = StockReservedEvent.builder()
                    .sagaId(cmd.getSagaId())
                    .orderId(cmd.getOrderId())
                    .patientId(cmd.getPatientId())
                    .medicationCode(cmd.getMedicationCode())
                    .quantityReserved(cmd.getQuantity())
                    .totalCost(cmd.getPrice())
                    .success(true)
                    .build();

            kafkaTemplate.send(new ProducerRecord<>(
                    PharmacyOrderSagaOrchestrator.TOPIC_PHARMACY_EVENTS, cmd.getSagaId(), event));

        } catch (Exception ex) {
            log.error("[Pharmacy Service] Error reserving stock for saga {}", cmd.getSagaId(), ex);
            StockReservedEvent errorEvent = StockReservedEvent.builder()
                    .sagaId(cmd.getSagaId())
                    .orderId(cmd.getOrderId())
                    .patientId(cmd.getPatientId())
                    .success(false)
                    .failureReason(ex.getMessage())
                    .build();

            kafkaTemplate.send(new ProducerRecord<>(
                    PharmacyOrderSagaOrchestrator.TOPIC_PHARMACY_EVENTS, cmd.getSagaId(), errorEvent));
        }
    }

    private void handleReleaseStock(ReleaseStockCommand cmd) {
        log.warn("[Pharmacy Service] COMPENSATION: Releasing reserved stock for Drug: {}, Qty: {}",
                cmd.getMedicationCode(), cmd.getQuantity());

        jdbcTemplate.update(
                "UPDATE medication_inventory " +
                "SET available_stock = available_stock + ?, reserved_stock = reserved_stock - ?, updated_at = NOW() " +
                "WHERE medication_code = ? AND reserved_stock >= ?",
                cmd.getQuantity(), cmd.getQuantity(), cmd.getMedicationCode(), cmd.getQuantity()
        );

        jdbcTemplate.update(
                "UPDATE pharmacy_orders SET status = 'CANCELLED', updated_at = NOW() WHERE order_id = ?",
                cmd.getOrderId()
        );
    }
}
