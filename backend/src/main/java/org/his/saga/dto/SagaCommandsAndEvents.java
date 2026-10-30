package org.his.saga.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.math.BigDecimal;
import java.time.Instant;

/**
 * Event-Driven Commands and Domain Events for the 4-step Pharmacy Saga Workflow.
 */
public class SagaCommandsAndEvents {

    // INITIATION TRIGGER DTO
    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class ProcessPrescriptionRequest implements Serializable {
        private String prescriptionId;
        private String patientId;
        private String medicationCode;
        private Integer quantity;
        private BigDecimal price; // e.g. $50.00
        private String physicianId;
    }

    // STEP 1: PHARMACY INVENTORY RESERVATION
    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class ReserveStockCommand implements Serializable {
        private String sagaId;
        private String orderId;
        private String prescriptionId;
        private String patientId;
        private String medicationCode;
        private Integer quantity;
        private BigDecimal price;
    }

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class StockReservedEvent implements Serializable {
        private String sagaId;
        private String orderId;
        private String patientId;
        private String medicationCode;
        private Integer quantityReserved;
        private BigDecimal totalCost;
        private boolean success;
        private String failureReason;
    }

    // STEP 2: BILLING CHARGE (Handled by IdempotentBillingConsumer)
    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class ChargePatientBillingCommand implements Serializable {
        private String sagaId;
        private String orderId;
        private String patientId;
        private BigDecimal amount; // e.g. $50.00
        private String description;
    }

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class PaymentChargedEvent implements Serializable {
        private String sagaId;
        private String transactionId;
        private String orderId;
        private String patientId;
        private BigDecimal chargedAmount;
        private boolean success;
        private String denialReason;
    }

    // STEP 3: PATIENT CLINICAL TIMELINE RECORDING
    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class RecordPatientTimelineCommand implements Serializable {
        private String sagaId;
        private String patientId;
        private String orderId;
        private String eventType;
        private String description;
        private Instant occurredAt;
    }

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class TimelineUpdatedEvent implements Serializable {
        private String sagaId;
        private String timelineId;
        private String patientId;
        private String orderId;
        private boolean success;
    }

    // STEP 4: PHARMACY ORDER COMPLETION
    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class CompletePharmacyOrderCommand implements Serializable {
        private String sagaId;
        private String orderId;
        private String patientId;
    }

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class PharmacyOrderCompletedEvent implements Serializable {
        private String sagaId;
        private String orderId;
        private String patientId;
        private Instant completedAt;
    }

    // COMPENSATING TRANSACTIONS
    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class ReleaseStockCommand implements Serializable {
        private String sagaId;
        private String orderId;
        private String medicationCode;
        private Integer quantity;
    }

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class RefundPaymentCommand implements Serializable {
        private String sagaId;
        private String transactionId;
        private String orderId;
        private BigDecimal refundAmount;
    }
}
