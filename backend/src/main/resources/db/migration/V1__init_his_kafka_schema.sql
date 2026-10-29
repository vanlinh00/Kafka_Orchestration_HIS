-- ==============================================================================
-- HOSPITAL INFORMATION SYSTEM (HIS) KAFKA BACKBONE DATABASE SCHEMA
-- Target Database: PostgreSQL 16+
-- ==============================================================================

-- 1. SAGA ORCHESTRATION STATE TABLE (Pharmacy Order Fulfillment)
CREATE TABLE his_pharmacy_saga_state (
    saga_id VARCHAR(64) PRIMARY KEY,
    order_id VARCHAR(64) NOT NULL,
    patient_id VARCHAR(64) NOT NULL,
    medication_code VARCHAR(32) NOT NULL,
    quantity INTEGER NOT NULL CHECK (quantity > 0),
    estimated_cost NUMERIC(12, 2) NOT NULL,
    status VARCHAR(48) NOT NULL,
    inventory_lock_id VARCHAR(64),
    pre_auth_code VARCHAR(64),
    failure_reason TEXT,
    retry_count INTEGER NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at TIMESTAMPTZ
);

CREATE INDEX idx_saga_patient_id ON his_pharmacy_saga_state(patient_id);
CREATE INDEX idx_saga_order_id ON his_pharmacy_saga_state(order_id);
CREATE INDEX idx_saga_status ON his_pharmacy_saga_state(status);
CREATE INDEX idx_saga_created_at ON his_pharmacy_saga_state(created_at);

-- 2. EXACTLY-ONCE CONSUMER DEDUPLICATION TABLE (Dual-Write Elimination)
CREATE TABLE his_consumed_messages (
    fingerprint VARCHAR(256) PRIMARY KEY, -- format: consumer_group:topic:partition:offset
    event_id VARCHAR(64) NOT NULL,
    consumer_group VARCHAR(128) NOT NULL,
    topic VARCHAR(128) NOT NULL,
    partition_id INTEGER NOT NULL,
    message_offset BIGINT NOT NULL,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_consumed_event_id ON his_consumed_messages(event_id);
CREATE INDEX idx_consumed_topic_partition ON his_consumed_messages(topic, partition_id);
CREATE INDEX idx_consumed_processed_at ON his_consumed_messages(processed_at);

-- 3. DEAD LETTER QUEUE (DLQ) TRIAGE & COMPLIANCE AUDIT LOG
CREATE TABLE his_dlq_audit_log (
    id BIGSERIAL PRIMARY KEY,
    dlq_id VARCHAR(64) NOT NULL UNIQUE,
    original_topic VARCHAR(128) NOT NULL,
    original_partition INTEGER NOT NULL,
    original_offset BIGINT NOT NULL,
    message_key VARCHAR(128),
    exception_class VARCHAR(256) NOT NULL,
    exception_message TEXT NOT NULL,
    stack_trace TEXT,
    raw_payload TEXT NOT NULL,
    headers_json JSONB,
    status VARCHAR(32) NOT NULL DEFAULT 'QUARANTINED', -- QUARANTINED, REPLAYED, DISCARDED
    replayed_at TIMESTAMPTZ,
    replayed_by VARCHAR(64),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_dlq_status ON his_dlq_audit_log(status);
CREATE INDEX idx_dlq_topic ON his_dlq_audit_log(original_topic);
CREATE INDEX idx_dlq_key ON his_dlq_audit_log(message_key);

-- 4. PATIENT KMS DATA ENCRYPTION KEY (DEK) VAULT (Crypto-Shredding Store)
CREATE TABLE his_kms_patient_keys (
    patient_id VARCHAR(64) PRIMARY KEY,
    encrypted_dek_blob BYTEA NOT NULL, -- AES DEK wrapped with Master KEK from HSM/KMS
    key_version INTEGER NOT NULL DEFAULT 1,
    algorithm VARCHAR(32) NOT NULL DEFAULT 'AES-256-GCM',
    status VARCHAR(32) NOT NULL DEFAULT 'ACTIVE', -- ACTIVE, SHREDDED, ROTATED
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    shredded_at TIMESTAMPTZ,
    shredded_by VARCHAR(64),
    legal_basis VARCHAR(128) -- e.g., 'GDPR_ART_17_RIGHT_TO_BE_FORGOTTEN'
);

CREATE INDEX idx_patient_keys_status ON his_kms_patient_keys(status);
