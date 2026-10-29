import { KafkaTopicDef } from '../types/his-kafka';

export const HIS_TOPIC_TAXONOMY: KafkaTopicDef[] = [
  // Patient Domain
  {
    name: 'his.patient.adt.v1',
    domain: 'patient',
    entity: 'adt',
    eventType: 'events',
    version: 'v1',
    description: 'Patient Admission, Discharge, Transfer, Bed Movement, and Demographic Change lifecycle events (HL7 A01/A02/A03/A08 mapped to FHIR Encounter).',
    partitionKey: 'patient_id (MRN)',
    partitionKeyRationale: 'Guarantees strict chronological timeline per patient. A discharge cannot be processed before an admission or transfer.',
    partitions: 12,
    replicationFactor: 3,
    minInsyncReplicas: 2,
    cleanupPolicy: 'delete',
    retentionHours: 2160, // 90 days
    maxMessageBytes: '1048576 (1MB)',
    schemaFormat: 'Avro',
    throughputEstimate: '450 events/sec peak',
    guarantee: 'EOS'
  },
  {
    name: 'his.patient.master-index.v1',
    domain: 'patient',
    entity: 'master-index',
    eventType: 'snapshot',
    version: 'v1',
    description: 'Patient Master Identity (Enterprise Master Patient Index - EMPI) state table with verified demographics, insurance cards, and merge tombstones.',
    partitionKey: 'enterprise_patient_uuid',
    partitionKeyRationale: 'Enables log compaction to maintain latest canonical patient identity state. Null value represents patient merge tombstone.',
    partitions: 12,
    replicationFactor: 3,
    minInsyncReplicas: 2,
    cleanupPolicy: 'compact',
    retentionHours: 'Compacted (Infinite)',
    maxMessageBytes: '1048576 (1MB)',
    schemaFormat: 'Avro',
    throughputEstimate: '60 events/sec',
    guarantee: 'EOS'
  },

  // Clinical Domain
  {
    name: 'his.clinical.cpoe-orders.v1',
    domain: 'clinical',
    entity: 'cpoe-orders',
    eventType: 'orders',
    version: 'v1',
    description: 'Computerized Physician Order Entry (CPOE) for lab tests, diagnostic imaging, nursing protocols, and clinical consults.',
    partitionKey: 'patient_id',
    partitionKeyRationale: 'Preserves chronological clinical order sequence per patient, allowing validation against patient current condition and active diagnoses.',
    partitions: 16,
    replicationFactor: 3,
    minInsyncReplicas: 2,
    cleanupPolicy: 'delete',
    retentionHours: 4320, // 180 days
    maxMessageBytes: '2097152 (2MB)',
    schemaFormat: 'Avro',
    throughputEstimate: '380 events/sec',
    guarantee: 'EOS'
  },
  {
    name: 'his.clinical.lab-results.v1',
    domain: 'clinical',
    entity: 'lab-results',
    eventType: 'results',
    version: 'v1',
    description: 'Laboratory Information System (LIS) finalized test values, critical alerts, panicking flags, and microbiology reports (FHIR Observation).',
    partitionKey: 'order_id',
    partitionKeyRationale: 'Serializes sequential test result components (preliminary, final, corrected, append) for a specific diagnostic requisition.',
    partitions: 16,
    replicationFactor: 3,
    minInsyncReplicas: 2,
    cleanupPolicy: 'delete',
    retentionHours: 8760, // 365 days
    maxMessageBytes: '4194304 (4MB)',
    schemaFormat: 'Avro',
    throughputEstimate: '850 events/sec',
    guarantee: 'EOS'
  },
  {
    name: 'his.clinical.vitals-telemetry.v1',
    domain: 'telemetry',
    entity: 'vitals-telemetry',
    eventType: 'streaming',
    version: 'v1',
    description: 'High-frequency ICU bedside monitors, SpO2, arterial pulse, ECG waveform aggregates, ventilator parameters, and infusion pump telemetry.',
    partitionKey: 'device_id#minute_bucket',
    partitionKeyRationale: 'Salted temporal key prevents hot partition skew from unstable critical ICU beds while spreading millions of stream frames evenly.',
    partitions: 64,
    replicationFactor: 3,
    minInsyncReplicas: 2,
    cleanupPolicy: 'delete',
    retentionHours: 72, // 3 days high-frequency, downstream downsampled
    maxMessageBytes: '524288 (512KB)',
    schemaFormat: 'Protobuf',
    throughputEstimate: '42,000 events/sec',
    guarantee: 'At-Least-Once'
  },

  // Pharmacy Domain
  {
    name: 'his.pharmacy.prescriptions.v1',
    domain: 'pharmacy',
    entity: 'prescriptions',
    eventType: 'events',
    version: 'v1',
    description: 'Doctor pharmacy orders, e-prescriptions, dosage administration schedules, and titrations (FHIR MedicationRequest).',
    partitionKey: 'prescription_id',
    partitionKeyRationale: 'Coordinates pharmacy fulfillment lifecycle, inventory lock verification, and automatic dispensing machine dispensing sequences.',
    partitions: 12,
    replicationFactor: 3,
    minInsyncReplicas: 2,
    cleanupPolicy: 'delete',
    retentionHours: 4320, // 180 days
    maxMessageBytes: '1048576 (1MB)',
    schemaFormat: 'Avro',
    throughputEstimate: '220 events/sec',
    guarantee: 'EOS'
  },
  {
    name: 'his.pharmacy.dispense-events.v1',
    domain: 'pharmacy',
    entity: 'dispense-events',
    eventType: 'dispense',
    version: 'v1',
    description: 'Automated Dispensing Cabinet (Pyxis/Omnicell) drawer openings, nurse barcode scan confirmations, and pill administrations.',
    partitionKey: 'prescription_id',
    partitionKeyRationale: 'Aligns dispense verification directly with the originating prescription to verify five-rights medication administration.',
    partitions: 12,
    replicationFactor: 3,
    minInsyncReplicas: 2,
    cleanupPolicy: 'delete',
    retentionHours: 8760,
    maxMessageBytes: '1048576 (1MB)',
    schemaFormat: 'Avro',
    throughputEstimate: '180 events/sec',
    guarantee: 'EOS'
  },
  {
    name: 'his.pharmacy.saga-commands.v1',
    domain: 'pharmacy',
    entity: 'saga-commands',
    eventType: 'commands',
    version: 'v1',
    description: 'Saga orchestrator command channel routing execution instructions: ReserveInventory, AuthorizeBilling, DispatchMedication, RollbackHold.',
    partitionKey: 'saga_id',
    partitionKeyRationale: 'Ensures all orchestrator commands and state transitions for a given saga instance are consumed in strict sequence.',
    partitions: 12,
    replicationFactor: 3,
    minInsyncReplicas: 2,
    cleanupPolicy: 'delete',
    retentionHours: 168, // 7 days
    maxMessageBytes: '524288 (512KB)',
    schemaFormat: 'Avro',
    throughputEstimate: '310 events/sec',
    guarantee: 'EOS'
  },

  // Billing Domain
  {
    name: 'his.billing.claims.v1',
    domain: 'billing',
    entity: 'claims',
    eventType: 'claims',
    version: 'v1',
    description: 'Adjudication claims, diagnosis code (ICD-10/CPT) billable items, hospital encounter charges, and insurance clearinghouse submissions.',
    partitionKey: 'encounter_id',
    partitionKeyRationale: 'Aggregates all billable line items (pharmacy, lab, bed stay, surgery) under a unified hospitalization encounter.',
    partitions: 12,
    replicationFactor: 3,
    minInsyncReplicas: 2,
    cleanupPolicy: 'delete',
    retentionHours: 8760, // 365 days
    maxMessageBytes: '2097152 (2MB)',
    schemaFormat: 'Avro',
    throughputEstimate: '140 events/sec',
    guarantee: 'EOS'
  },
  {
    name: 'his.billing.preauth-requests.v1',
    domain: 'billing',
    entity: 'preauth-requests',
    eventType: 'authorizations',
    version: 'v1',
    description: 'Payer prior authorization transactions, real-time eligibility (EDI 270/271), and copay/deductible verification.',
    partitionKey: 'patient_id',
    partitionKeyRationale: 'Serializes payer inquiries per patient to prevent race conditions in insurance benefits accumulator checking.',
    partitions: 8,
    replicationFactor: 3,
    minInsyncReplicas: 2,
    cleanupPolicy: 'delete',
    retentionHours: 2160,
    maxMessageBytes: '1048576 (1MB)',
    schemaFormat: 'Avro',
    throughputEstimate: '95 events/sec',
    guarantee: 'EOS'
  }
];

export const TAXONOMY_RULES = {
  namingStandard: 'his.<domain>.<entity>.<event-type>.<version>',
  conventionFormat: {
    prefix: 'his (Hospital Information System root)',
    domain: 'patient | clinical | pharmacy | billing | laboratory | telemetry',
    entity: 'Business noun (adt, lab-results, prescriptions, claims, encounters)',
    eventType: 'events | commands | snapshots | streaming',
    version: 'v1 | v2 (Strict semver major version indicator)'
  },
  standardHeaders: [
    { key: 'ce_id', description: 'Unique CloudEvents UUID for distributed deduplication' },
    { key: 'ce_specversion', description: 'CloudEvents specification version (1.0)' },
    { key: 'ce_type', description: 'FQDN event type e.g., org.his.clinical.observation.v1' },
    { key: 'ce_source', description: 'Emitting microservice identifier e.g., his/pharmacy-service' },
    { key: 'ce_time', description: 'ISO 8601 UTC timestamp of original event occurrence' },
    { key: 'traceparent', description: 'W3C distributed trace header for OpenTelemetry tracing' },
    { key: 'x_data_classification', description: 'RESTRICTED_PHI | CONFIDENTIAL | INTERNAL' },
    { key: 'x_encryption_dek_id', description: 'KMS Data Encryption Key ID for payload crypto-shredding' },
    { key: 'x_correlation_id', description: 'Correlation ID tying all saga operations together' }
  ]
};
