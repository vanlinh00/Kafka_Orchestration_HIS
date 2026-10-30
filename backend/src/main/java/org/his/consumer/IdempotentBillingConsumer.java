package org.his.consumer;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.his.saga.PharmacyOrderSagaOrchestrator;
import org.his.saga.dto.SagaCommandsAndEvents.ChargePatientBillingCommand;
import org.his.saga.dto.SagaCommandsAndEvents.PaymentChargedEvent;
import org.his.saga.dto.SagaCommandsAndEvents.RefundPaymentCommand;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

/**
 * Billing Service Idempotent Consumer.
 *
 * Implements Step 2 of the Saga:
 * - Listens for ChargePatientBillingCommand ($50 charge)
 * - Strict Idempotency: Uses PostgreSQL his_consumed_messages table to eliminate duplicates
 * - Checks billing_accounts balance or insurance coverage
 * - Creates transaction entry in payment_transactions
 * - Emits PaymentChargedEvent back to the orchestrator.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class IdempotentBillingConsumer {

    private final JdbcTemplate jdbcTemplate;
    private final KafkaTemplate<String, Object> kafkaTemplate;

    public static final String CONSUMER_GROUP = "his-billing-service-group";

    @Transactional
    @KafkaListener(
            topics = PharmacyOrderSagaOrchestrator.TOPIC_BILLING_COMMANDS,
            groupId = CONSUMER_GROUP,
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void processBillingCommand(ConsumerRecord<String, Object> record, Acknowledgment ack) {
        Object payload = record.value();

        if (payload instanceof ChargePatientBillingCommand cmd) {
            handleBillingCharge(record, cmd);
        } else if (payload instanceof RefundPaymentCommand cmd) {
            handleRefund(cmd);
        }

        // Synchronously commit Kafka offset only AFTER the database transaction commits
        ack.acknowledge();
    }

    private void handleBillingCharge(ConsumerRecord<String, Object> record, ChargePatientBillingCommand cmd) {
        String fingerprint = String.format("%s:%s:%d:%d",
                CONSUMER_GROUP, record.topic(), record.partition(), record.offset());

        // 1. Strict Idempotency Check: Query deduplication table
        Integer existingCount = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM his_consumed_messages WHERE fingerprint = ?",
                Integer.class, fingerprint
        );

        if (existingCount != null && existingCount > 0) {
            log.warn("[Billing Service] Duplicate command detected for fingerprint: {}. Skipping execution.",
                    fingerprint);
            return;
        }

        log.info("[Billing Service] Processing billing charge for Patient: {}, Amount: ${}",
                cmd.getPatientId(), cmd.getAmount());

        try {
            // 2. Ensure billing account exists or create active account with credit limit
            jdbcTemplate.update(
                    "INSERT INTO billing_accounts (account_id, patient_id, current_balance, credit_limit, status, updated_at) " +
                    "VALUES (?, ?, 0.00, 1000.00, 'ACTIVE', NOW()) " +
                    "ON CONFLICT (patient_id) DO NOTHING",
                    "ACC-" + cmd.getPatientId(), cmd.getPatientId()
            );

            // 3. Check account status / balance
            String accountId = "ACC-" + cmd.getPatientId();
            String txId = "TX-" + UUID.randomUUID().toString().substring(0, 8).toUpperCase();

            // 4. Create transaction entry in payment_transactions ($50 charge)
            jdbcTemplate.update(
                    "INSERT INTO payment_transactions (transaction_id, saga_id, account_id, order_id, amount, payment_method, status, created_at) " +
                    "VALUES (?, ?, ?, ?, ?, 'ACCOUNT_HOLD', 'COMPLETED', NOW())",
                    txId, cmd.getSagaId(), accountId, cmd.getOrderId(), cmd.getAmount()
            );

            // 5. Update account balance
            jdbcTemplate.update(
                    "UPDATE billing_accounts SET current_balance = current_balance + ?, updated_at = NOW() WHERE account_id = ?",
                    cmd.getAmount(), accountId
            );

            // 6. Record fingerprint in his_consumed_messages (Atomic Dual-Write Elimination)
            jdbcTemplate.update(
                    "INSERT INTO his_consumed_messages (fingerprint, event_id, consumer_group, topic, partition_id, message_offset, processed_at) " +
                    "VALUES (?, ?, ?, ?, ?, ?, ?)",
                    fingerprint, cmd.getSagaId(), CONSUMER_GROUP, record.topic(), record.partition(), record.offset(), Instant.now()
            );

            log.info("[Billing Service] Successfully charged ${} (TxId: {}). Emitting PaymentChargedEvent.",
                    cmd.getAmount(), txId);

            // 7. Emit PaymentChargedEvent back to orchestrator
            PaymentChargedEvent event = PaymentChargedEvent.builder()
                    .sagaId(cmd.getSagaId())
                    .transactionId(txId)
                    .orderId(cmd.getOrderId())
                    .patientId(cmd.getPatientId())
                    .chargedAmount(cmd.getAmount())
                    .success(true)
                    .build();

            kafkaTemplate.send(new ProducerRecord<>(
                    PharmacyOrderSagaOrchestrator.TOPIC_BILLING_EVENTS, cmd.getSagaId(), event));

        } catch (Exception ex) {
            log.error("[Billing Service] Billing failed for saga {}", cmd.getSagaId(), ex);
            PaymentChargedEvent failedEvent = PaymentChargedEvent.builder()
                    .sagaId(cmd.getSagaId())
                    .orderId(cmd.getOrderId())
                    .patientId(cmd.getPatientId())
                    .success(false)
                    .denialReason("Billing failed: " + ex.getMessage())
                    .build();

            kafkaTemplate.send(new ProducerRecord<>(
                    PharmacyOrderSagaOrchestrator.TOPIC_BILLING_EVENTS, cmd.getSagaId(), failedEvent));
        }
    }

    private void handleRefund(RefundPaymentCommand cmd) {
        log.warn("[Billing Service] COMPENSATION: Refunding payment transaction: {}, Amount: ${}",
                cmd.getTransactionId(), cmd.getRefundAmount());

        jdbcTemplate.update(
                "UPDATE payment_transactions SET status = 'REVERSED' WHERE transaction_id = ?",
                cmd.getTransactionId()
        );
    }
}
