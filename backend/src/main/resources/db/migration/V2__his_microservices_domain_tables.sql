-- ==============================================================================
-- DOMAIN SERVICE TABLES: PHARMACY, BILLING, AND PATIENT
-- ==============================================================================

-- 1. PHARMACY SERVICE DOMAIN
CREATE TABLE prescriptions (
    prescription_id VARCHAR(64) PRIMARY KEY,
    patient_id VARCHAR(64) NOT NULL,
    physician_id VARCHAR(64) NOT NULL,
    medication_code VARCHAR(32) NOT NULL,
    dosage VARCHAR(64) NOT NULL,
    quantity INTEGER NOT NULL CHECK (quantity > 0),
    status VARCHAR(32) NOT NULL DEFAULT 'ACTIVE',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE medication_inventory (
    medication_code VARCHAR(32) PRIMARY KEY,
    medication_name VARCHAR(128) NOT NULL,
    available_stock INTEGER NOT NULL CHECK (available_stock >= 0),
    reserved_stock INTEGER NOT NULL DEFAULT 0 CHECK (reserved_stock >= 0),
    unit_price NUMERIC(10, 2) NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE pharmacy_orders (
    order_id VARCHAR(64) PRIMARY KEY,
    saga_id VARCHAR(64),
    prescription_id VARCHAR(64) REFERENCES prescriptions(prescription_id),
    patient_id VARCHAR(64) NOT NULL,
    medication_code VARCHAR(32) NOT NULL,
    quantity INTEGER NOT NULL,
    total_amount NUMERIC(10, 2) NOT NULL,
    status VARCHAR(32) NOT NULL DEFAULT 'PENDING', -- PENDING, STOCK_RESERVED, COMPLETED, CANCELLED
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_pharmacy_orders_patient ON pharmacy_orders(patient_id);
CREATE INDEX idx_pharmacy_orders_saga ON pharmacy_orders(saga_id);

-- 2. BILLING SERVICE DOMAIN
CREATE TABLE billing_accounts (
    account_id VARCHAR(64) PRIMARY KEY,
    patient_id VARCHAR(64) NOT NULL UNIQUE,
    current_balance NUMERIC(12, 2) NOT NULL DEFAULT 0.00,
    credit_limit NUMERIC(12, 2) NOT NULL DEFAULT 1000.00,
    insurance_policy_number VARCHAR(64),
    status VARCHAR(32) NOT NULL DEFAULT 'ACTIVE',
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE invoices (
    invoice_id VARCHAR(64) PRIMARY KEY,
    account_id VARCHAR(64) REFERENCES billing_accounts(account_id),
    order_id VARCHAR(64) NOT NULL,
    amount NUMERIC(10, 2) NOT NULL,
    status VARCHAR(32) NOT NULL DEFAULT 'ISSUED',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE payment_transactions (
    transaction_id VARCHAR(64) PRIMARY KEY,
    saga_id VARCHAR(64),
    account_id VARCHAR(64) REFERENCES billing_accounts(account_id),
    order_id VARCHAR(64) NOT NULL,
    amount NUMERIC(10, 2) NOT NULL,
    payment_method VARCHAR(32) NOT NULL DEFAULT 'ACCOUNT_HOLD',
    status VARCHAR(32) NOT NULL DEFAULT 'COMPLETED', -- PENDING, COMPLETED, REVERSED
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_payment_transactions_saga ON payment_transactions(saga_id);
CREATE INDEX idx_payment_transactions_order ON payment_transactions(order_id);

-- 3. PATIENT SERVICE DOMAIN
CREATE TABLE patients (
    patient_id VARCHAR(64) PRIMARY KEY, -- Medical Record Number (MRN)
    full_name VARCHAR(128) NOT NULL,
    date_of_birth DATE NOT NULL,
    gender VARCHAR(16) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE adt_history (
    event_id VARCHAR(64) PRIMARY KEY,
    patient_id VARCHAR(64) REFERENCES patients(patient_id),
    adt_type VARCHAR(16) NOT NULL, -- A01_ADMISSION, A02_TRANSFER, A03_DISCHARGE, A08_UPDATE
    ward VARCHAR(64),
    bed VARCHAR(32),
    timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE patient_timeline_events (
    timeline_id VARCHAR(64) PRIMARY KEY,
    patient_id VARCHAR(64) REFERENCES patients(patient_id),
    saga_id VARCHAR(64),
    event_type VARCHAR(64) NOT NULL, -- PHARMACY_DISPENSED, LAB_RESULT, ADT_ADMISSION, BILLING_CHARGE
    description TEXT NOT NULL,
    metadata JSONB,
    occurred_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_patient_timeline_patient_occurred ON patient_timeline_events(patient_id, occurred_at);
