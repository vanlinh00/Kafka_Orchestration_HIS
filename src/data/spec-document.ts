export const HIS_FULL_SPEC_MARKDOWN = `# ENTERPRISE ARCHITECTURE DESIGN SPECIFICATION & TECHNICAL EXECUTION GUIDE
## Apache Kafka as the Central Event Stream & Orchestration Backbone for Hospital Information Systems (HIS)

**Author:** Principal Enterprise Systems Architect (Distributed Systems & Healthcare)  
**Target Platform:** Java 17 / Spring Boot 3.4 / Apache Kafka 3.7+ / PostgreSQL 16  
**Standards:** HL7 v2.x / HL7 FHIR R4 / CloudEvents v1.0 / HIPAA Security Rule / GDPR Art. 17  
**Status:** Approved for Implementation (v1.0.0-PROD)

---

### 1. Executive Summary & Architectural Scope

Modern hospital campuses operate hundreds of disparate subsystems: Electronic Health Record (EHR) systems, Patient Administration Systems (PAS), Laboratory Information Systems (LIS), Pharmacy Management Systems (PMS), Picture Archiving and Communication Systems (PACS), Medical Device Telemetry (ICU/Ward IoT), and Revenue Cycle Management (RCM/Billing). Historically, point-to-point interface engines (e.g., Mirth Connect, Cloverleaf) produced fragile spaghetti integrations with opaque message loss, uncoordinated state rollbacks, and zero horizontal scalability.

This specification details the end-to-end design and execution guide for establishing **Apache Kafka** as the authoritative enterprise event backbone and orchestration layer for the Hospital Information System (HIS).

\`\`\`
+----------------------------------------------------------------------------------------------------+
|                                    HOSPITAL INFORMATION SYSTEM (HIS)                               |
|                                                                                                    |
|   +------------------+     +-------------------+     +------------------+    +-----------------+   |
|   |   PAS / ADT      |     |  LIS / Pathology  |     | Pharmacy (CPOE)  |    | Billing / Payer |   |
|   | (Admission/Bed)  |     | (Lab Instruments) |     |  & Pyxis Cabinet |    |   (Claims RCM)  |   |
|   +--------+---------+     +---------+---------+     +--------+---------+    +--------+--------+   |
|            |                         |                        |                       |            |
|            v                         v                        v                       v            |
|     [Patient Producer]        [LIS Outbox Prod]        [Saga Orchestrator]     [Billing Consumer]  |
|            |                         |                        |                       |            |
|            +-------------------------+------------------------+-----------------------+            |
|                                                  |                                                 |
|                                                  v                                                 |
|   ==============================================================================================   |
|                                     APACHE KAFKA CLUSTER (KRAFT)                                   |
|                                                                                                    |
|    +-----------------------------+     +-------------------------------+                           |
|    | his.patient.adt.v1          |     | his.clinical.lab-results.v1   | (Replication: 3)          |
|    | (Key: patient_id / MRN)     |     | (Key: order_id)               | (min.insync: 2)           |
|    +-----------------------------+     +-------------------------------+ (zstd compressed)         |
|    +-----------------------------+     +-------------------------------+                           |
|    | his.pharmacy.prescript.v1   |     | his.billing.claims.v1         | (Exactly-Once Semantics)  |
|    | (Key: prescription_id)      |     | (Key: encounter_id)           | (PostgreSQL Dedup)        |
|    +-----------------------------+     +-------------------------------+                           |
|    +-----------------------------+     +-------------------------------+                           |
|    | his.clinical.vitals-telem.v1|     | his.patient.master-index.v1   | (High Throughput / IoT)   |
|    | (Key: device#bucket_min)    |     | (Cleanup: compact)            | (Compacted Demographics)  |
|    +-----------------------------+     +-------------------------------+                           |
|   ==============================================================================================   |
|                                                  |                                                 |
|                     +----------------------------+----------------------------+                    |
|                     |                                                         |                    |
|                     v                                                         v                    |
|         +-----------------------+                                 +-----------------------+        |
|         | Poison / Malformed    |                                 | Confluent Schema Reg. |        |
|         | Dead Letter Topic     |                                 | (Avro / FHIR R4)      |        |
|         | (*.DLT & Retry Queue) |                                 | BACKWARD_TRANSITIVE   |        |
|         +-----------------------+                                 +-----------------------+        |
+----------------------------------------------------------------------------------------------------+
\`\`\`

---

### 2. Topic Taxonomy & Design Standard

#### 2.1 Standardized Naming Convention
Every topic created on the HIS Kafka cluster must strictly adhere to the dot-delimited hierarchical naming convention:

$$\\mathbf{his}.\\langle \\text{domain} \\rangle.\\langle \\text{entity} \\rangle.\\langle \\text{event-type} \\rangle.\\mathbf{v}\\langle \\text{major-version} \\rangle$$

- **Domain:** Bounded business context (\`patient\`, \`clinical\`, \`pharmacy\`, \`billing\`, \`telemetry\`).
- **Entity:** Business domain entity (\`adt\`, \`lab-results\`, \`prescriptions\`, \`claims\`, \`master-index\`).
- **Event-Type:** Operational semantic:
  - \`events\`: Fact-based, immutable occurrences that have already happened.
  - \`commands\`: Intent-based directives targeted at a specific subsystem or saga actor.
  - \`snapshot\`: Entity state snapshots designed for log compaction.
  - \`streaming\`: High-throughput time-series sensor or telemetry data.
- **Version:** Contract version (\`v1\`, \`v2\`). Schema alterations that break backward compatibility require a major version bump.

#### 2.2 Standard CloudEvents Header Specification
Every Kafka record must transmit mandatory headers to support tracing, deduplication, and regulatory security:
- \`ce_id\`: RFC 4122 UUID v4 event identifier.
- \`ce_type\`: Fully qualified event contract (e.g., \`org.his.clinical.observation.v1\`).
- \`ce_source\`: URI of origin microservice (e.g., \`his/lis-analyzer-core\`).
- \`ce_time\`: ISO-8601 UTC timestamp.
- \`traceparent\`: W3C distributed trace context header for OpenTelemetry.
- \`x_data_classification\`: Mandatory data tier (\`RESTRICTED_PHI\` | \`CONFIDENTIAL\` | \`INTERNAL\`).
- \`x_encryption_dek_id\`: KMS Data Encryption Key identifier used for crypto-shredding.

---

### 3. Partitioning & Message Keying Strategy

#### 3.1 Total In-Order Event Processing Guarantee
In healthcare, out-of-order events create severe clinical risk. For example:
$$\\text{Patient Admission (A01)} \\longrightarrow \\text{Ward Bed Move (A02)} \\longrightarrow \\text{Medication Order} \\longrightarrow \\text{Discharge (A03)}$$
If a network delay causes the discharge event to be processed prior to the medication order, the hospital system may dispatch lethal dosages to an empty bed or fail billing audits.

**Partition Key Strategy:**
- All Patient ADT and EHR lifecycle events must be keyed by the **normalized Enterprise Master Patient Identifier (EMPI / MRN)**.
- Kafka’s deterministic Murmur2 hashing algorithm guarantees:
$$\\text{partition} = \\text{toPositive}(\\text{murmur2}(\\text{patient\\_id})) \\pmod{\\text{num\\_partitions}}$$
- Because Kafka guarantees strict FIFO ordering within a single partition, and only one consumer in a consumer group can read a given partition at any instant, **all events for a single patient are processed in strict chronological order**.

#### 3.2 Hot Partition Avoidance for High-Throughput Telemetry (ICU IoT)
While administrative ADT events produce moderate throughput (100–500 events/sec), bedside patient monitors (SpO2, 12-lead ECG, arterial pressure waveforms) generate 1,000 to 50,000 events/sec.
- **The Skew Problem:** If an ICU patient is keyed purely by \`patient_id\`, one critical patient partition will experience massive data spikes while ambulatory patient partitions sit idle.
- **The Solution - Composite Salted Partition Key:**
$$\\text{Key} = \\text{patient\\_id} \\parallel \\text{"\\#"} \\parallel \\lfloor \\text{timestamp\\_millis} / 60000 \\rfloor$$
This distributes streaming frames across 64 dedicated partitions in 60-second time windows, while downstream Kafka Streams or Flink session windows reconstruct the unified patient waveform without memory starvation.

#### 3.3 Partition Sizing Formula
To avoid broker rebalancing churn, partition sizing must satisfy:
$$P = \\max\\left(\\left\\lceil \\frac{T_{\\text{peak}}}{T_{\\text{prod\\_throughput}}} \\right\\rceil, \\left\\lceil \\frac{T_{\\text{peak}}}{T_{\\text{cons\\_throughput}}} \\right\\rceil\\right) \\times S_{\\text{headroom}}$$
Where:
- $T_{\\text{peak}}$ = Peak target throughput (e.g., $15{,}000$ msg/sec)
- $T_{\\text{cons\\_throughput}}$ = Realistic consumer write speed to PostgreSQL ($1{,}200$ msg/sec/thread)
- $S_{\\text{headroom}}$ = $1.5$ safety factor
$$P = \\left\\lceil \\frac{15000}{1200} \\right\\rceil \\times 1.5 = 13 \\times 1.5 \\approx 20 \\implies \\mathbf{24\\text{ partitions}}$$

---

### 4. Offset Management, Delivery Guarantees & Fault Tolerance

#### 4.1 Offset Commit Discipline
- **Rule 1:** \`enable.auto.commit = false\` across all HIS consumers. Auto-commit can acknowledge records before business database commits succeed, creating permanent silent data loss during container crashes.
- **Rule 2:** Use Spring Kafka \`ContainerProperties.AckMode.MANUAL_IMMEDIATE\`.
- **Rule 3:** Always commit Kafka offsets **synchronously after** the local PostgreSQL database transaction commits.

#### 4.2 Exactly-Once Semantics (EOS) & The Transactional Outbox Pattern
Financial charges and pharmacy dispenses cannot tolerate either data loss or double execution.
- **Producer Config:** \`acks=all\`, \`enable.idempotence=true\`, \`max.in.flight.requests.per.connection=5\`.
- **Consumer Deduplication Table (PostgreSQL):**
\`\`\`sql
CREATE TABLE his_consumed_messages (
    fingerprint VARCHAR(256) PRIMARY KEY, -- group:topic:partition:offset
    event_id VARCHAR(64) NOT NULL,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
\`\`\`
Before applying any state change, the consumer checks if \`fingerprint\` exists. If present, it acknowledges the message and exits cleanly without re-executing business operations.

#### 4.3 Dead Letter Queue (DLQ) & Multi-Tier Retry Architecture
Payloads received with corrupt HL7 segment delimiters, invalid FHIR JSON syntax, or unresolvable schema IDs must never block partition consumption.

\`\`\`
Incoming Message
       |
       v
[Deserializer / Schema Validator]
       |
       +---(Fatal: DeserializationException)--------> [his.<domain>.<entity>.DLT] (Quarantine)
       |                                                    |
       +---(Success)                                        v
       |                                       [Audit Alert & Ops Triage UI]
       v
[Business Processing Handler]
       |
       +---(Transient: SQL Timeout / Lock)-----> [Retry Queue with Exponential Backoff]
       |                                                    |
       |                                                    +-> (Attempt 1: +10s)
       |                                                    +-> (Attempt 2: +60s)
       |                                                    +-> (Attempt 3: +300s)
       |                                                    +-> (Exhausted) -> [.DLT]
       v
[PostgreSQL Atomic Commit] ---> [Manual Offset Ack]
\`\`\`

---

### 5. Multi-Step Workflow Orchestration (Saga Pattern)

#### 5.1 Hospital Pharmacy Fulfillment Workflow
The clinical workflow **"Doctor Places Pharmacy Order -> Inventory Check -> Billing Pre-authorization -> Prescription Dispatch"** spans multiple microservices and physical dispensing hardware.

\`\`\`
+----------------------------------------------------------------------------------------------------------+
|                                     SAGA WORKFLOW STATE MACHINE                                          |
|                                                                                                          |
|   [Physician CPOE Order]                                                                                 |
|              |                                                                                           |
|              v                                                                                           |
|   (Step 1: Order Registered)                                                                             |
|              |                                                                                           |
|              v                                                                                           |
|   [Command: Reserve Inventory] ---------(Out of Stock)----------> [COMPENSATION: Cancel Order]          |
|              |                                                              |                            |
|          (Reserved)                                                         v                            |
|              |                                                    [Physician Notified]                   |
|              v                                                                                           |
|   [Command: Authorize Billing] ---------(Payer Denied)----------> [COMPENSATION: Release Inventory Lock] |
|              |                                                              |                            |
|          (Approved)                                                         v                            |
|              |                                                    [Order Cancelled & Billed Rollback]    |
|              v                                                                                           |
|   [Command: Dispatch Dispense Cabinet (Pyxis)]                                                           |
|              |                                                                                           |
|          (Confirmed)                                                                                     |
|              |                                                                                           |
|              v                                                                                           |
|   (Prescription Fulfilled: Five-Rights Verified)                                                         |
+----------------------------------------------------------------------------------------------------------+
\`\`\`

#### 5.2 Orchestration vs. Choreography in Healthcare Systems
| Evaluation Dimension | Choreographed Saga (Event-Driven) | Orchestrated Saga (Stateful Coordinator) | HIS Verdict |
| :--- | :--- | :--- | :--- |
| **Auditability & Visibility** | Opaque; state spread across 5 service databases | Explicit; centralized state table in PostgreSQL | **Orchestration Required (JCAHO Compliance)** |
| **Cyclic Dependency Risk** | High; services listen to each other's domain events | Low; clear command/reply star topology | **Orchestration Prevents Deadlocks** |
| **Rollback Reliability** | Difficult to trace which step failed | Orchestrator issues deterministic compensation | **Orchestration Ensures Safety** |
| **Latency** | Minimal overhead | Minimal state store I/O (~3ms per transition) | **Negligible difference in clinical time** |

---

### 6. Compliance, Data Privacy & Log Compaction (HIPAA / GDPR)

#### 6.1 Envelope Encryption & Crypto-Shredding Architecture
Under HIPAA § 164.312(a)(2)(iv) and GDPR Article 17 ("Right to be Forgotten"), Protected Health Information (PHI) stored indefinitely in Kafka logs poses major regulatory liability. Because Kafka's commit log is architecturally immutable, deleting single records from historical log segments is impossible without corrupting consumer offsets.

**The Solution: Cryptographic Erasure (Crypto-Shredding):**
1. Every patient is assigned a distinct **Data Encryption Key (DEK)** generated via AWS KMS / HashiCorp Vault.
2. The payload is encrypted with \`AES-256-GCM\` using the patient's DEK before emitting to Kafka.
3. The DEK ID is attached in the Kafka header \`x-encryption-dek-id\`.
4. When a patient exercises their Right to be Forgotten, the hospital KMS permanently **destroys the patient's DEK**.
5. All historical records in Kafka topic segments immediately become cryptographically unrecoverable ciphertext, satisfying legal erasure requirements while preserving Kafka log integrity!

#### 6.2 Log Compaction for Patient Master Index (EMPI)
Topic: \`his.patient.master-index.v1\`
- \`cleanup.policy = compact\`
- \`min.cleanable.dirty.ratio = 0.5\`
- \`delete.retention.ms = 86400000\` (24-hour tombstone window)
- Key: \`enterprise_patient_id\`
- Emitting a record with \`key = patient_id\` and \`value = null\` (Tombstone) purges previous versions upon compaction, allowing clean patient merge reconciliations.

---

### 7. Zero-Downtime Schema Evolution & Confluent Registry Rules

1. **Compatibility Mode:** Enforce \`BACKWARD_TRANSITIVE\` compatibility in Confluent Schema Registry. This guarantees newly deployed consumers can parse data produced by old services, and older consumers can read payloads with newly added optional fields.
2. **Schema Evolution Invariants:**
   - Always assign a default value to new fields (\`default: null\`).
   - Never remove required fields.
   - Never rename existing field identifiers (use Avro \`aliases\` if renaming is mandatory).
   - Use Enums with care; always include an \`UNKNOWN\` fallback symbol for forward-compatibility.
`;
