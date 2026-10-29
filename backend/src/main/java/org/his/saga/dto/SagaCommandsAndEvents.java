package org.his.saga.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.math.BigDecimal;
import java.time.Instant;

public class SagaCommandsAndEvents {

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class DoctorPharmacyOrderRequest implements Serializable {
        private String orderId;
        private String patientId;
        private String medicationCode;
        private Integer quantity;
        private BigDecimal estimatedCost;
        private String physicianId;
        private String clinicalNotes;
    }

    // STEP 2 COMMAND & REPLIES
    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class ReserveInventoryCommand implements Serializable {
        private String sagaId;
        private String orderId;
        private String medicationCode;
        private Integer quantity;
    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class InventoryResponseEvent implements Serializable {
        private String sagaId;
        private boolean success;
        private String lockId;
        private String failureReason;
    }

    // STEP 3 COMMAND & REPLIES
    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class AuthorizeBillingCommand implements Serializable {
        private String sagaId;
        private String patientId;
        private String orderId;
        private BigDecimal amount;
    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class BillingPreAuthResponseEvent implements Serializable {
        private String sagaId;
        private boolean approved;
        private String preAuthAuthorizationCode;
        private String denialReason;
    }

    // STEP 4 COMMAND & REPLIES
    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class DispatchPrescriptionCommand implements Serializable {
        private String sagaId;
        private String orderId;
        private String patientId;
        private String medicationCode;
    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class PrescriptionDispatchedEvent implements Serializable {
        private String sagaId;
        private boolean success;
        private String cabinetId;
        private String nurseBadge;
        private String failureReason;
    }

    // COMPENSATING COMMANDS
    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class ReleaseInventoryCommand implements Serializable {
        private String sagaId;
        private String lockId;
        private String medicationCode;
        private Integer quantity;
    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class VoidBillingHoldCommand implements Serializable {
        private String sagaId;
        private String preAuthCode;
        private String patientId;
    }

    // TERMINAL DOMAIN EVENTS
    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class PrescriptionFulfilledEvent implements Serializable {
        private String sagaId;
        private String orderId;
        private String patientId;
        private Instant completedAt;
    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class PrescriptionFailedEvent implements Serializable {
        private String sagaId;
        private String orderId;
        private String patientId;
        private String failureCode;
        private String reason;
    }
}
