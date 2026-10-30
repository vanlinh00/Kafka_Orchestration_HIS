# HIS Architecture & Microservice Domains

This repository contains the backend components and event-driven architecture for the Hospital Information System (HIS). Below is an overview of the core domain services, their responsibilities, database ownership, and an example of the Orchestrated Saga pattern workflow.

---

## 1. Pharmacy Service

* **Domain:** Order & Inventory Management
* **Core Job:** Manages medication catalogs, prescription fulfillment, and drug inventory levels.
* **Why It Must Be Its Own Service:**
  * **Inventory Locking:** Requires lock management to reserve stock so multiple medical staff cannot dispense the same remaining inventory concurrently.
  * **Workflow Control:** Manages the initial state of a dispense workflow before financial charges or clinical record updates occur.
* **Database Tables Owned:**
  * `pharmacy_orders`
  * `medication_inventory`
  * `prescriptions`

---

## 2. Billing Service

* **Domain:** Finance & Revenue Cycle
* **Core Job:** Handles patient billing, insurance claims, payment processing, and financial ledgering.
* **Why It Must Be Its Own Service:**
  * **Strict Idempotency:** Implements idempotent consumers (`IdempotentBillingConsumer.java`) to prevent duplicate charges caused by network retries or dropped messages.
  * **Data Privacy & Compliance:** Enforces strict regulatory access control; accounting personnel can access payment data without exposure to clinical medical histories.
* **Database Tables Owned:**
  * `billing_accounts`
  * `invoices`
  * `payment_transactions`

---

## 3. Patient Service

* **Domain:** Clinical EHR & ADT (Admission, Discharge, Transfer)
* **Core Job:** Maintains the patient's master profile, ADT history, and complete medical record timeline.
* **Why It Must Be Its Own Service:**
  * **Single Source of Truth:** Serves as the authoritative source for patient identity and clinical history.
  * **Chronological Event Ordering:** Uses specialized partitioners (`PatientTimelinePartitioner.java`) to ensure event messages are sequenced strictly by time for clear diagnostic tracking.
* **Database Tables Owned:**
  * `patients`
  * `adt_history`
  * `patient_timeline_events`

---

## Orchestrated Saga Pattern: Step-by-Step Workflow

### Scenario: Processing a Pharmacy Order

```
┌─────────────────────────┐
│ Pharmacy Saga           │
│ Orchestrator            │
└───┬─────────────────┬───┘
    │ (1) Reserve     │ (2) Charge
    ▼                 ▼
┌──────────────┐  ┌──────────────┐
│ Pharmacy     │  │ Billing      │
│ Service      │  │ Service      │
└──────────────┘  └──────────────┘
                          │ (3) Log Timeline
                          ▼
                  ┌──────────────┐
                  │ Patient      │
                  │ Service      │
                  └──────────────┘
```

### Successful Path Execution Steps

#### Step 1: Initiation & Stock Reservation (Pharmacy Service)
1. **Trigger:** The API receives a request to process a prescription.
2. **Orchestrator Action:** `PharmacyOrderSagaOrchestrator` generates a unique `saga_id` (e.g., `SAGA-1001`), sets the status to `STARTED`, and sends a command to the Pharmacy Service.
3. **Local Transaction (Pharmacy DB):**
   * Inserts order record into `pharmacy_orders` with status `PENDING`.
   * Decrements medication count in `medication_inventory`.
4. **Result:** Pharmacy Service emits `StockReservedEvent` back to the orchestrator.

#### Step 2: Patient Billing (Billing Service)
1. **Orchestrator Action:** Receives `StockReservedEvent` and sends a command event to the Kafka topic handled by `IdempotentBillingConsumer.java`.
2. **Local Transaction (Billing DB):**
   * Checks `billing_accounts` balance or insurance coverage.
   * Creates a transaction entry in `payment_transactions` for $50.
   * `IdempotentBillingConsumer` ensures this message is processed exactly once even if network retries occur.
3. **Result:** Billing Service emits `PaymentChargedEvent` back to the orchestrator.

#### Step 3: Timeline Recording (Patient Service)
1. **Orchestrator Action:** Receives `PaymentChargedEvent` and emits a command to update the clinical medical record.
2. **Local Transaction (Patient DB):**
   * `PatientTimelinePartitioner` routes the event using `patient_id` as the key to maintain strict chronological order in Kafka.
   * The Patient Service writes the entry into `patient_timeline_events`.
3. **Result:** Patient Service emits `TimelineUpdatedEvent` back to the orchestrator.

#### Step 4: Completion (Pharmacy Service)
1. **Orchestrator Action:** Marks `pharmacy_orders` status as `COMPLETED` and ends the Saga.