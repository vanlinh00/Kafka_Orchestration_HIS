package org.his.saga.model;

import jakarta.persistence.*;
import lombok.*;

import java.math.BigDecimal;
import java.time.Instant;

/**
 * PostgreSQL JPA entity tracking the exact state of the distributed Pharmacy Order Saga.
 */
@Entity
@Table(name = "his_pharmacy_saga_state")
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class PharmacySagaState {

    @Id
    @Column(name = "saga_id", length = 64)
    private String sagaId;

    @Column(name = "order_id", nullable = false, length = 64)
    private String orderId;

    @Column(name = "patient_id", nullable = false, length = 64)
    private String patientId;

    @Column(name = "medication_code", nullable = false, length = 32)
    private String medicationCode;

    @Column(name = "quantity", nullable = false)
    private Integer quantity;

    @Column(name = "estimated_cost", nullable = false, precision = 12, scale = 2)
    private BigDecimal estimatedCost;

    @Enumerated(EnumType.STRING)
    @Column(name = "status", nullable = false, length = 48)
    private SagaStatus status;

    @Column(name = "inventory_lock_id", length = 64)
    private String inventoryLockId;

    @Column(name = "pre_auth_code", length = 64)
    private String preAuthCode;

    @Column(name = "failure_reason", columnDefinition = "TEXT")
    private String failureReason;

    @Column(name = "retry_count", nullable = false)
    @Builder.Default
    private Integer retryCount = 0;

    @Column(name = "created_at", nullable = false, updatable = false)
    private Instant createdAt;

    @Column(name = "updated_at", nullable = false)
    private Instant updatedAt;

    @Column(name = "completed_at")
    private Instant completedAt;

    @PrePersist
    protected void onCreate() {
        this.createdAt = Instant.now();
        this.updatedAt = Instant.now();
    }

    @PreUpdate
    protected void onUpdate() {
        this.updatedAt = Instant.now();
    }
}
