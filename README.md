# HIS Architecture & Microservice Domains

This repository contains the backend components and event-driven architecture for the Hospital Information System (HIS)[cite: 1]. Below is an overview of the core domain services, their responsibilities, and database ownership.

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
  * **Strict Idempotency:** Implements idempotent consumers (`IdempotentBillingConsumer.java`) to prevent duplicate charges caused by network retries or dropped messages[cite: 1].
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
  * **Chronological Event Ordering:** Uses specialized partitioners (`PatientTimelinePartitioner.java`) to ensure event messages are sequenced strictly by time for clear diagnostic tracking[cite: 1].
* **Database Tables Owned:**
  * `patients`
  * `adt_history`
  * `patient_timeline_events`