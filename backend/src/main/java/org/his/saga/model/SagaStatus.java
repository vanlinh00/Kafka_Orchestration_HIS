package org.his.saga.model;

/**
 * Saga lifecycle state machine status codes.
 */
public enum SagaStatus {
    STARTED,
    STOCK_RESERVATION_PENDING,
    STOCK_RESERVED,
    BILLING_PENDING,
    PAYMENT_CHARGED,
    TIMELINE_PENDING,
    TIMELINE_UPDATED,
    COMPLETED,

    // Compensations and Failures
    FAILED_STOCK_UNAVAILABLE,
    COMPENSATING_BILLING_FAILED,
    FAILED_ROLLED_BACK
}
