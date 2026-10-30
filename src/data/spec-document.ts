export const HIS_FULL_SPEC_MARKDOWN = `# ENTERPRISE ARCHITECTURE DESIGN SPECIFICATION & TECHNICAL EXECUTION GUIDE
## Apache Kafka as the Central Event Stream & Orchestration Backbone for Hospital Information Systems (HIS)

**Author:** Principal Enterprise Systems Architect (Distributed Systems & Healthcare)  
**Target Platform:** Java 17 / Spring Boot 3.4 / Apache Kafka 3.7+ / PostgreSQL 16  
**Standards:** HL7 FHIR R4 / CloudEvents v1.0 / HIPAA Security Rule / GDPR Art. 17  

### 1. Executive Summary & Core Domains
Modern hospital campuses operate hundreds of disparate subsystems. This platform defines:
- **Pharmacy Service:** Prescription orders, automated cabinet locks, and saga state orchestration.
- **Billing Service:** Revenue cycle, insurance claim pre-authorization, and PostgreSQL Exactly-Once deduplication.
- **Patient Service:** Master patient demographics, admission/discharge/transfer (ADT) timelines, and HIPAA/GDPR envelope encryption.

### 2. Topic Taxonomy Standard
Topic Format: \`his.<domain>.<entity>.<event-type>.v<major>\`
Topics include: \`his.patient.adt.v1\`, \`his.patient.master-index.v1\`, \`his.clinical.lab-results.v1\`, \`his.clinical.vitals-telemetry.v1\`, \`his.pharmacy.prescriptions.v1\`, \`his.billing.claims.v1\`.

### 3. Saga Orchestration (Pharmacy Workflow)
Workflow: Physician CPOE Order -> Inventory Lock -> Billing Pre-Auth -> Cabinet Dispense.
Compensations: Automated rollbacks (ReleaseInventoryCommand, VoidBillingHoldCommand) ensure financial and clinical safety.
`;
