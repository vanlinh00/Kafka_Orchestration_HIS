import React, { useState } from 'react';
import { Header } from './components/Header';
import { Database, ShieldCheck, Pill, CreditCard, UserCheck, Layers, FileCode, CheckCircle2, Copy, Check } from 'lucide-react';
import { HIS_TOPIC_TAXONOMY } from './data/topics-catalog';

export default function App() {
  const [activeTab, setActiveTab] = useState<string>('domains');
  const [copiedFile, setCopiedFile] = useState<string | null>(null);

  const handleExportReadme = () => {
    const text = `# HIS Architecture & Microservice Domains\n\nThis repository contains the backend components and event-driven architecture for the Hospital Information System (HIS).\n\n## 1. Pharmacy Service\n* **Domain:** Order & Inventory Management\n* **Core Job:** Manages medication catalogs, prescription fulfillment, and drug inventory levels.\n* **Database Tables Owned:** pharmacy_orders, medication_inventory, prescriptions\n\n## 2. Billing Service\n* **Domain:** Finance & Revenue Cycle\n* **Core Job:** Handles patient billing, insurance claims, payment processing, and financial ledgering.\n* **Database Tables Owned:** billing_accounts, invoices, payment_transactions\n\n## 3. Patient Service\n* **Domain:** Clinical EHR & ADT\n* **Core Job:** Maintains patient master profile, ADT history, and medical record timeline.\n* **Database Tables Owned:** patients, adt_history, patient_timeline_events`;
    const blob = new Blob([text], { type: 'text/markdown;charset=utf-8' });
    const url = URL.createObjectURL(blob);
    const link = document.createElement('a');
    link.href = url;
    link.download = 'README.md';
    document.body.appendChild(link);
    link.click();
    document.body.removeChild(link);
    URL.revokeObjectURL(url);
  };

  const backendFiles = [
    { name: 'PharmacyOrderSagaOrchestrator.java', path: 'src/main/java/org/his/saga/PharmacyOrderSagaOrchestrator.java', domain: 'Pharmacy', desc: '4-step stateful saga orchestrator with inventory locks and compensation rollbacks.' },
    { name: 'IdempotentBillingConsumer.java', path: 'src/main/java/org/his/consumer/IdempotentBillingConsumer.java', domain: 'Billing', desc: 'Strict Exactly-Once consumer using PostgreSQL deduplication to prevent double-billing.' },
    { name: 'PatientTimelinePartitioner.java', path: 'src/main/java/org/his/partitioner/PatientTimelinePartitioner.java', domain: 'Patient', desc: 'In-order patient timeline Murmur2 partitioner with ICU telemetry salting.' },
    { name: 'KmsEnvelopeEncryptionSerializer.java', path: 'src/main/java/org/his/security/KmsEnvelopeEncryptionSerializer.java', domain: 'Security', desc: 'AES-256-GCM envelope encryption for HIPAA and GDPR crypto-shredding.' },
    { name: 'KafkaTopicConfiguration.java', path: 'src/main/java/org/his/config/KafkaTopicConfiguration.java', domain: 'Kafka', desc: '10 enterprise topics definitions with replication factor 3 and min.isr 2.' },
    { name: 'KafkaProducerConsumerConfig.java', path: 'src/main/java/org/his/config/KafkaProducerConsumerConfig.java', domain: 'Kafka', desc: 'Transactional producer, manual ack listener container, and DLQ recoverer.' },
    { name: 'V1__init_his_kafka_schema.sql', path: 'src/main/resources/db/migration/V1__init_his_kafka_schema.sql', domain: 'PostgreSQL', desc: 'Flyway DDL migration for saga state, deduplication fingerprints, and KMS keys.' },
    { name: 'application.yml', path: 'src/main/resources/application.yml', domain: 'Config', desc: 'Production Spring Boot 3.4 Kafka cluster configuration.' }
  ];

  return (
    <div className="min-h-screen bg-slate-50 text-slate-900 flex flex-col font-sans">
      <Header
        activeTab={activeTab}
        setActiveTab={setActiveTab}
        onExportSpec={handleExportReadme}
      />

      <main className="flex-1 max-w-7xl w-full mx-auto px-6 py-6 space-y-6">
        {/* TAB 1: DOMAINS */}
        {activeTab === 'domains' && (
          <div className="space-y-6">
            <div className="bg-white rounded-lg border border-slate-200 p-6">
              <h1 className="text-xl font-bold text-slate-900 tracking-tight">
                HIS Architecture &amp; Microservice Domains
              </h1>
              <p className="text-xs text-slate-600 mt-1 max-w-3xl">
                This repository contains the backend components and event-driven architecture for the Hospital Information System (HIS). Below is an overview of the core domain services, their responsibilities, and database ownership.
              </p>
            </div>

            <div className="grid grid-cols-1 md:grid-cols-3 gap-6">
              {/* Domain 1: Pharmacy */}
              <div className="bg-white rounded-lg border border-slate-200 p-6 space-y-4">
                <div className="flex items-center gap-2 border-b border-slate-100 pb-3">
                  <div className="w-8 h-8 rounded bg-emerald-100 text-emerald-800 flex items-center justify-center">
                    <Pill className="w-4 h-4" />
                  </div>
                  <div>
                    <h2 className="text-sm font-bold text-slate-900">1. Pharmacy Service</h2>
                    <span className="text-[11px] text-slate-500 font-medium">Domain: Order &amp; Inventory Management</span>
                  </div>
                </div>

                <div>
                  <div className="text-xs font-semibold text-slate-700">Core Job:</div>
                  <p className="text-xs text-slate-600 mt-0.5">
                    Manages medication catalogs, prescription fulfillment, and drug inventory levels.
                  </p>
                </div>

                <div className="space-y-1.5 text-xs text-slate-600">
                  <div className="font-semibold text-slate-700">Why It Must Be Its Own Service:</div>
                  <ul className="list-disc list-inside space-y-1 text-slate-600">
                    <li><strong>Inventory Locking:</strong> Requires lock management to reserve stock so multiple medical staff cannot dispense the same remaining inventory concurrently.</li>
                    <li><strong>Workflow Control:</strong> Manages the initial state of a dispense workflow before financial charges or clinical record updates occur.</li>
                  </ul>
                </div>

                <div className="pt-3 border-t border-slate-100">
                  <div className="text-[11px] font-semibold text-slate-700 mb-1">Database Tables Owned:</div>
                  <div className="space-y-1 font-mono text-[11px]">
                    <div className="bg-slate-50 px-2 py-0.5 rounded border border-slate-200 text-slate-800">pharmacy_orders</div>
                    <div className="bg-slate-50 px-2 py-0.5 rounded border border-slate-200 text-slate-800">medication_inventory</div>
                    <div className="bg-slate-50 px-2 py-0.5 rounded border border-slate-200 text-slate-800">prescriptions</div>
                    <div className="bg-slate-50 px-2 py-0.5 rounded border border-slate-200 text-slate-800">his_pharmacy_saga_state</div>
                  </div>
                </div>
              </div>

              {/* Domain 2: Billing */}
              <div className="bg-white rounded-lg border border-slate-200 p-6 space-y-4">
                <div className="flex items-center gap-2 border-b border-slate-100 pb-3">
                  <div className="w-8 h-8 rounded bg-blue-100 text-blue-800 flex items-center justify-center">
                    <CreditCard className="w-4 h-4" />
                  </div>
                  <div>
                    <h2 className="text-sm font-bold text-slate-900">2. Billing Service</h2>
                    <span className="text-[11px] text-slate-500 font-medium">Domain: Finance &amp; Revenue Cycle</span>
                  </div>
                </div>

                <div>
                  <div className="text-xs font-semibold text-slate-700">Core Job:</div>
                  <p className="text-xs text-slate-600 mt-0.5">
                    Handles patient billing, insurance claims, payment processing, and financial ledgering.
                  </p>
                </div>

                <div className="space-y-1.5 text-xs text-slate-600">
                  <div className="font-semibold text-slate-700">Why It Must Be Its Own Service:</div>
                  <ul className="list-disc list-inside space-y-1 text-slate-600">
                    <li><strong>Strict Idempotency:</strong> Implements idempotent consumers (<code className="font-mono text-slate-800">IdempotentBillingConsumer.java</code>) to prevent duplicate charges caused by network retries or dropped messages.</li>
                    <li><strong>Data Privacy &amp; Compliance:</strong> Enforces strict regulatory access control; accounting personnel can access payment data without exposure to clinical medical histories.</li>
                  </ul>
                </div>

                <div className="pt-3 border-t border-slate-100">
                  <div className="text-[11px] font-semibold text-slate-700 mb-1">Database Tables Owned:</div>
                  <div className="space-y-1 font-mono text-[11px]">
                    <div className="bg-slate-50 px-2 py-0.5 rounded border border-slate-200 text-slate-800">billing_accounts</div>
                    <div className="bg-slate-50 px-2 py-0.5 rounded border border-slate-200 text-slate-800">invoices</div>
                    <div className="bg-slate-50 px-2 py-0.5 rounded border border-slate-200 text-slate-800">payment_transactions</div>
                    <div className="bg-slate-50 px-2 py-0.5 rounded border border-slate-200 text-slate-800">his_consumed_messages</div>
                  </div>
                </div>
              </div>

              {/* Domain 3: Patient */}
              <div className="bg-white rounded-lg border border-slate-200 p-6 space-y-4">
                <div className="flex items-center gap-2 border-b border-slate-100 pb-3">
                  <div className="w-8 h-8 rounded bg-purple-100 text-purple-800 flex items-center justify-center">
                    <UserCheck className="w-4 h-4" />
                  </div>
                  <div>
                    <h2 className="text-sm font-bold text-slate-900">3. Patient Service</h2>
                    <span className="text-[11px] text-slate-500 font-medium">Domain: Clinical EHR &amp; ADT</span>
                  </div>
                </div>

                <div>
                  <div className="text-xs font-semibold text-slate-700">Core Job:</div>
                  <p className="text-xs text-slate-600 mt-0.5">
                    Maintains the patient's master profile, ADT history, and complete medical record timeline.
                  </p>
                </div>

                <div className="space-y-1.5 text-xs text-slate-600">
                  <div className="font-semibold text-slate-700">Why It Must Be Its Own Service:</div>
                  <ul className="list-disc list-inside space-y-1 text-slate-600">
                    <li><strong>Single Source of Truth:</strong> Serves as the authoritative source for patient identity and clinical history.</li>
                    <li><strong>Chronological Event Ordering:</strong> Uses specialized partitioners (<code className="font-mono text-slate-800">PatientTimelinePartitioner.java</code>) to ensure event messages are sequenced strictly by time for clear diagnostic tracking.</li>
                  </ul>
                </div>

                <div className="pt-3 border-t border-slate-100">
                  <div className="text-[11px] font-semibold text-slate-700 mb-1">Database Tables Owned:</div>
                  <div className="space-y-1 font-mono text-[11px]">
                    <div className="bg-slate-50 px-2 py-0.5 rounded border border-slate-200 text-slate-800">patients</div>
                    <div className="bg-slate-50 px-2 py-0.5 rounded border border-slate-200 text-slate-800">adt_history</div>
                    <div className="bg-slate-50 px-2 py-0.5 rounded border border-slate-200 text-slate-800">patient_timeline_events</div>
                    <div className="bg-slate-50 px-2 py-0.5 rounded border border-slate-200 text-slate-800">his_kms_patient_keys</div>
                  </div>
                </div>
              </div>
            </div>
          </div>
        )}

        {/* TAB 2: TOPOLOGY */}
        {activeTab === 'topology' && (
          <div className="bg-white rounded-lg border border-slate-200 p-6 space-y-4">
            <h2 className="text-lg font-bold text-slate-900">Kafka Event Stream Backbone Topology</h2>
            <p className="text-xs text-slate-600">
              Interconnecting Pharmacy, Billing, Patient, and Clinical Subsystems with KRaft Consensus, Replication Factor 3, and min.insync.replicas 2.
            </p>
            <div className="p-4 bg-slate-900 text-slate-100 rounded font-mono text-xs overflow-x-auto whitespace-pre leading-relaxed">
{`+------------------+     +-------------------+     +------------------+    +-----------------+
|   Patient ADT    |     |  LIS / Pathology  |     | Pharmacy (CPOE)  |    | Billing / Payer |
| (Admission/Bed)  |     | (Lab Instruments) |     |  & Pyxis Cabinet |    |   (Claims RCM)  |
+--------+---------+     +---------+---------+     +--------+---------+    +--------+--------+
         |                         |                        |                       |
         v                         v                        v                       v
  [Patient Producer]        [LIS Outbox Prod]        [Saga Orchestrator]     [Billing Consumer]
         |                         |                        |                       |
         +-------------------------+------------------------+-----------------------+
                                               |
                                               v
==============================================================================================
                                  APACHE KAFKA CLUSTER (KRAFT)
 +-----------------------------+     +-------------------------------+
 | his.patient.adt.v1          |     | his.clinical.lab-results.v1   | (Replication: 3)
 | (Key: patient_id / MRN)     |     | (Key: order_id)               | (min.insync: 2)
 +-----------------------------+     +-------------------------------+ (zstd compressed)
 +-----------------------------+     +-------------------------------+
 | his.pharmacy.prescript.v1   |     | his.billing.claims.v1         | (Exactly-Once Semantics)
 | (Key: prescription_id)      |     | (Key: encounter_id)           | (PostgreSQL Dedup)
 +-----------------------------+     +-------------------------------+
==============================================================================================`}
            </div>
          </div>
        )}

        {/* TAB 3: TOPICS */}
        {activeTab === 'topics' && (
          <div className="bg-white rounded-lg border border-slate-200 p-6 space-y-4">
            <h2 className="text-lg font-bold text-slate-900">Standardized Kafka Topic Taxonomy</h2>
            <div className="overflow-x-auto">
              <table className="w-full text-left text-xs border-collapse">
                <thead>
                  <tr className="bg-slate-50 border-b border-slate-200 text-slate-500 font-semibold uppercase text-[10px]">
                    <th className="py-2 px-3">Topic Name</th>
                    <th className="py-2 px-3">Domain</th>
                    <th className="py-2 px-3">Partition Key</th>
                    <th className="py-2 px-2 text-right">Partitions</th>
                    <th className="py-2 px-2 text-center">RF / ISR</th>
                    <th className="py-2 px-3">Cleanup</th>
                    <th className="py-2 px-3">Schema</th>
                  </tr>
                </thead>
                <tbody className="divide-y divide-slate-100 font-mono text-[11px]">
                  {HIS_TOPIC_TAXONOMY.map(t => (
                    <tr key={t.name} className="hover:bg-slate-50">
                      <td className="py-2 px-3 font-semibold text-slate-900">{t.name}</td>
                      <td className="py-2 px-3 text-slate-600 capitalize">{t.domain}</td>
                      <td className="py-2 px-3 text-slate-700">{t.partitionKey}</td>
                      <td className="py-2 px-2 text-right text-slate-800">{t.partitions}</td>
                      <td className="py-2 px-2 text-center text-slate-600">{t.replicationFactor} / {t.minInsyncReplicas}</td>
                      <td className="py-2 px-3 text-slate-600">{t.cleanupPolicy}</td>
                      <td className="py-2 px-3 text-slate-600">{t.schemaFormat}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </div>
        )}

        {/* TAB 4: SAGA */}
        {activeTab === 'saga' && (
          <div className="bg-white rounded-lg border border-slate-200 p-6 space-y-4">
            <h2 className="text-lg font-bold text-slate-900">Pharmacy Order Saga Workflow (State Machine)</h2>
            <p className="text-xs text-slate-600">
              Coordinating multi-service transactions with automatic compensation rollbacks across Inventory &amp; Billing.
            </p>
            <div className="grid grid-cols-1 sm:grid-cols-4 gap-3 text-xs">
              <div className="p-3 bg-slate-50 border border-slate-200 rounded">
                <div className="font-bold text-slate-900">Step 1: Order Placement</div>
                <div className="text-[11px] text-slate-500 mt-1">CPOE MedicationRequest submitted by physician.</div>
              </div>
              <div className="p-3 bg-slate-50 border border-slate-200 rounded">
                <div className="font-bold text-slate-900">Step 2: Inventory Check</div>
                <div className="text-[11px] text-slate-500 mt-1">Acquires stock lock on drug supply in pharmacy inventory.</div>
              </div>
              <div className="p-3 bg-slate-50 border border-slate-200 rounded">
                <div className="font-bold text-slate-900">Step 3: Billing Pre-Auth</div>
                <div className="text-[11px] text-slate-500 mt-1">Insurance clearinghouse eligibility check (EDI 270/271).</div>
              </div>
              <div className="p-3 bg-slate-50 border border-slate-200 rounded">
                <div className="font-bold text-slate-900">Step 4: Dispense Dispatch</div>
                <div className="text-[11px] text-slate-500 mt-1">Pyxis automated dispensing cabinet unlocks medication drawer.</div>
              </div>
            </div>
          </div>
        )}

        {/* TAB 5: BACKEND FILES */}
        {activeTab === 'backend-files' && (
          <div className="bg-white rounded-lg border border-slate-200 p-6 space-y-4">
            <h2 className="text-lg font-bold text-slate-900">Java 17 &amp; Spring Boot 3.4 Backend File Manifest</h2>
            <p className="text-xs text-slate-600">
              The complete production backend source code is located in the <code className="font-mono text-slate-900">/backend</code> folder.
            </p>
            <div className="grid grid-cols-1 md:grid-cols-2 gap-3">
              {backendFiles.map(f => (
                <div key={f.name} className="p-3 bg-slate-50 border border-slate-200 rounded text-xs space-y-1">
                  <div className="flex items-center justify-between">
                    <span className="font-bold font-mono text-slate-900">{f.name}</span>
                    <span className="text-[10px] bg-slate-200 px-1.5 py-0.5 rounded font-semibold text-slate-700">{f.domain}</span>
                  </div>
                  <div className="text-slate-600 text-[11px]">{f.desc}</div>
                  <div className="font-mono text-[10px] text-slate-400">backend/{f.path}</div>
                </div>
              ))}
            </div>
          </div>
        )}
      </main>

      <footer className="border-t border-slate-200 bg-white py-3 px-6 text-xs text-slate-500">
        <div className="max-w-7xl mx-auto flex justify-between items-center">
          <span>Hospital Information System (HIS) · Apache Kafka Backbone</span>
          <span>Java 17 / Spring Boot 3.4 / PostgreSQL 16</span>
        </div>
      </footer>
    </div>
  );
}
