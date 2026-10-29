import React, { useState } from 'react';
import { Network, ArrowRight, ShieldCheck, Database, RefreshCw, AlertTriangle, Layers, Activity } from 'lucide-react';

interface SelectedNodeInfo {
  title: string;
  role: string;
  topics: string[];
  protocol: string;
  keyStrategy: string;
  reliability: string;
  details: string;
}

export const TopologyDiagram: React.FC<{ onNavigateToTopics: () => void; onNavigateToSaga: () => void }> = ({
  onNavigateToTopics,
  onNavigateToSaga
}) => {
  const [selectedNode, setSelectedNode] = useState<string>('kafka-core');

  const nodesInfo: Record<string, SelectedNodeInfo> = {
    'pas-adt': {
      title: 'Patient Administration System (PAS / ADT)',
      role: 'Upstream Event Producer',
      topics: ['his.patient.adt.v1', 'his.patient.master-index.v1'],
      protocol: 'HL7 v2.x (A01/A02/A03/A08) -> FHIR R4 Encounter Avro',
      keyStrategy: 'patient_id (MRN) — Strict FIFO patient event timeline ordering',
      reliability: 'Transactional Outbox + Idempotent Producer (acks=all)',
      details: 'Captures admissions, room/bed transfers, and discharges. Crucial requirement: A discharge event must never bypass or precede an admission or room move on Kafka partitions.'
    },
    'lis-lab': {
      title: 'Laboratory Information System (LIS)',
      role: 'Clinical Diagnostic Producer & Consumer',
      topics: ['his.clinical.cpoe-orders.v1', 'his.clinical.lab-results.v1'],
      protocol: 'FHIR Observation / DiagnosticReport Avro',
      keyStrategy: 'order_id — Keeps partial, preliminary, and final result components serialized',
      reliability: 'Manual Ack + Dead Letter Topic for unparseable analyzer payloads',
      details: 'Automated analyzers publish blood gas, chemistry, and pathology results. Emits critical value flags directly into low-latency consumer channels for physician paging.'
    },
    'pharmacy-saga': {
      title: 'Pharmacy CPOE & Saga Orchestrator',
      role: 'Distributed Workflow Coordinator',
      topics: ['his.pharmacy.prescriptions.v1', 'his.pharmacy.saga-commands.v1', 'his.pharmacy.dispense-events.v1'],
      protocol: 'FHIR MedicationRequest & Custom Avro Saga Commands',
      keyStrategy: 'saga_id & prescription_id — Deterministic command-reply pairing',
      reliability: 'Exactly-Once Semantics (EOS) + PostgreSQL Saga State Store',
      details: 'Coordinates the 4-step workflow: Doctor Order -> Inventory Lock -> Billing Pre-auth -> Dispense Cabinet. Handles automatic compensation rollback if drugs are out of stock or billing is denied.'
    },
    'billing-rcm': {
      title: 'Revenue Cycle & Billing (Payer Gateway)',
      role: 'Financial Claims Consumer & Producer',
      topics: ['his.billing.claims.v1', 'his.billing.preauth-requests.v1'],
      protocol: 'FHIR Claim / ANSI X12 EDI 837 / 835',
      keyStrategy: 'encounter_id — Collates all billable items per hospitalization stay',
      reliability: 'PostgreSQL Idempotency Table (Dual-Write elimination)',
      details: 'Processes diagnosis codes, charges, and insurance pre-authorization. Zero-tolerance for duplicate billing or dropped claim events.'
    },
    'icu-telemetry': {
      title: 'Bedside ICU IoT Streaming',
      role: 'High-Frequency Stream Producer',
      topics: ['his.clinical.vitals-telemetry.v1'],
      protocol: 'High-density Protobuf streaming',
      keyStrategy: 'device_id#minute_bucket — Salted key to eliminate hot partition skew',
      reliability: 'At-Least-Once with high partition count (64 partitions)',
      details: 'Handles continuous 50Hz ECG and SpO2 sensor feeds from 2,500 ICU beds. Distributes multi-million frame streams evenly across cluster without blocking clinical topics.'
    },
    'kafka-core': {
      title: 'Apache Kafka 3.7+ KRaft Cluster',
      role: 'Central Event Stream & Storage Log',
      topics: ['10 Production Topics across 5 Domains'],
      protocol: 'Kafka Binary Protocol over TLS 1.3 (mTLS) with Schema Registry',
      keyStrategy: 'Murmur2 deterministic hashing with specialized composite partition keys',
      reliability: 'Replication Factor 3, min.insync.replicas=2, zstd compression',
      details: 'The central hospital nervous system. Integrates Confluent Schema Registry (BACKWARD_TRANSITIVE compatibility) and AWS KMS envelope encryption for HIPAA crypto-shredding.'
    },
    'dlq-triage': {
      title: 'Dead Letter Topic & Error Triage (.DLT)',
      role: 'Poison Pill Quarantine & Incident Remediation',
      topics: ['*.DLT, *.RETRY-10S, *.RETRY-60S, *.RETRY-5M'],
      protocol: 'Enriched CloudEvents with exception stacktrace headers',
      keyStrategy: 'Preserves original record key and partition header metadata',
      reliability: 'Spring DefaultErrorHandler + DeadLetterPublishingRecoverer',
      details: 'Quarantines corrupted HL7 payloads or schema validation errors instantly. Prevents partition stalling while providing operators with an audit log to inspect and replay fixes.'
    }
  };

  const active = nodesInfo[selectedNode] || nodesInfo['kafka-core'];

  return (
    <div className="space-y-6">
      {/* Overview Banner */}
      <div className="bg-white rounded-lg border border-slate-200 p-6">
        <div className="flex flex-col md:flex-row md:items-center justify-between gap-4 border-b border-slate-100 pb-5">
          <div>
            <div className="flex items-center gap-2 text-xs font-medium text-slate-500 mb-1">
              <span>Enterprise Blueprint</span>
              <span aria-hidden="true">·</span>
              <span>Healthcare Information System</span>
              <span aria-hidden="true">·</span>
              <span>Apache Kafka Backbone</span>
            </div>
            <h1 className="text-xl font-bold text-slate-900 tracking-tight">
              Hospital Event Stream & Orchestration Topology
            </h1>
            <p className="text-sm text-slate-600 mt-1 max-w-3xl">
              Centralized, low-latency event spine interconnecting hospital clinical, administrative, pharmacy, and billing subsystems with strict ordering guarantees, HIPAA envelope encryption, and saga orchestration.
            </p>
          </div>
          <div className="flex items-center gap-2 shrink-0">
            <button
              onClick={onNavigateToTopics}
              className="px-3 py-1.5 text-xs font-semibold text-slate-700 bg-slate-100 hover:bg-slate-200 rounded transition-colors cursor-pointer"
            >
              View Topic Catalog
            </button>
            <button
              onClick={onNavigateToSaga}
              className="px-3 py-1.5 text-xs font-semibold text-white bg-slate-900 hover:bg-slate-800 rounded transition-colors cursor-pointer"
            >
              Run Saga Simulator
            </button>
          </div>
        </div>

        {/* Interactive Architecture Canvas */}
        <div className="mt-6 grid grid-cols-1 lg:grid-cols-12 gap-6 items-start">
          {/* Visual Topology Map */}
          <div className="lg:col-span-8 bg-slate-50 border border-slate-200 rounded-lg p-5">
            <div className="text-xs font-semibold text-slate-500 uppercase tracking-wider mb-4 flex items-center justify-between">
              <span>System Interaction Map (Click a node to inspect)</span>
              <span className="text-slate-400 font-normal">Active Selection: {active.title}</span>
            </div>

            {/* Producers Row */}
            <div className="space-y-4">
              <div className="text-xs font-semibold text-slate-400">Upstream Clinical & Hospital Producers</div>
              <div className="grid grid-cols-2 sm:grid-cols-4 gap-2.5">
                <button
                  onClick={() => setSelectedNode('pas-adt')}
                  className={`p-3 rounded border text-left transition-all cursor-pointer ${
                    selectedNode === 'pas-adt'
                      ? 'bg-white border-slate-900 shadow-sm ring-1 ring-slate-900'
                      : 'bg-white/80 border-slate-200 hover:border-slate-300'
                  }`}
                >
                  <div className="text-xs font-semibold text-slate-900">PAS / ADT</div>
                  <div className="text-[11px] text-slate-500 mt-0.5">Admissions & Beds</div>
                  <div className="text-[10px] font-mono text-slate-400 mt-2">Key: patient_id</div>
                </button>

                <button
                  onClick={() => setSelectedNode('lis-lab')}
                  className={`p-3 rounded border text-left transition-all cursor-pointer ${
                    selectedNode === 'lis-lab'
                      ? 'bg-white border-slate-900 shadow-sm ring-1 ring-slate-900'
                      : 'bg-white/80 border-slate-200 hover:border-slate-300'
                  }`}
                >
                  <div className="text-xs font-semibold text-slate-900">LIS / Labs</div>
                  <div className="text-[11px] text-slate-500 mt-0.5">Analyzers & Tests</div>
                  <div className="text-[10px] font-mono text-slate-400 mt-2">Key: order_id</div>
                </button>

                <button
                  onClick={() => setSelectedNode('pharmacy-saga')}
                  className={`p-3 rounded border text-left transition-all cursor-pointer ${
                    selectedNode === 'pharmacy-saga'
                      ? 'bg-white border-slate-900 shadow-sm ring-1 ring-slate-900'
                      : 'bg-white/80 border-slate-200 hover:border-slate-300'
                  }`}
                >
                  <div className="text-xs font-semibold text-slate-900">Pharmacy CPOE</div>
                  <div className="text-[11px] text-slate-500 mt-0.5">Saga Coordinator</div>
                  <div className="text-[10px] font-mono text-slate-400 mt-2">Key: saga_id</div>
                </button>

                <button
                  onClick={() => setSelectedNode('icu-telemetry')}
                  className={`p-3 rounded border text-left transition-all cursor-pointer ${
                    selectedNode === 'icu-telemetry'
                      ? 'bg-white border-slate-900 shadow-sm ring-1 ring-slate-900'
                      : 'bg-white/80 border-slate-200 hover:border-slate-300'
                  }`}
                >
                  <div className="text-xs font-semibold text-slate-900">ICU Telemetry</div>
                  <div className="text-[11px] text-slate-500 mt-0.5">50Hz Sensors</div>
                  <div className="text-[10px] font-mono text-slate-400 mt-2">Key: device#bucket</div>
                </button>
              </div>

              {/* Connecting Flow Indicator */}
              <div className="flex items-center justify-center py-1">
                <div className="flex items-center gap-2 text-xs text-slate-400 font-mono">
                  <span>↓↓ TLS 1.3 mTLS Stream Ingestion (KMS Envelope Encrypted PHI) ↓↓</span>
                </div>
              </div>

              {/* Center Core: Apache Kafka Cluster */}
              <div
                onClick={() => setSelectedNode('kafka-core')}
                className={`p-5 rounded-lg border text-center transition-all cursor-pointer ${
                  selectedNode === 'kafka-core'
                    ? 'bg-white border-slate-900 shadow-md ring-2 ring-slate-900'
                    : 'bg-white border-slate-300 hover:border-slate-400'
                }`}
              >
                <div className="flex items-center justify-center gap-2 mb-1.5">
                  <Database className="w-4 h-4 text-slate-800" />
                  <span className="text-sm font-bold text-slate-900">
                    APACHE KAFKA ENTERPRISE EVENT BUS (KRaft Consensus)
                  </span>
                </div>
                <div className="text-xs text-slate-600 max-w-xl mx-auto">
                  10 Standard Topics · Avro & Protobuf Contracts · Confluent Schema Registry · RF=3, min.isr=2
                </div>
                <div className="grid grid-cols-2 sm:grid-cols-4 gap-2 mt-3 pt-3 border-t border-slate-100 text-xs">
                  <div className="text-slate-600"><strong className="text-slate-900">Patient:</strong> his.patient.*</div>
                  <div className="text-slate-600"><strong className="text-slate-900">Clinical:</strong> his.clinical.*</div>
                  <div className="text-slate-600"><strong className="text-slate-900">Pharmacy:</strong> his.pharmacy.*</div>
                  <div className="text-slate-600"><strong className="text-slate-900">Billing:</strong> his.billing.*</div>
                </div>
              </div>

              {/* Connecting Flow Indicator */}
              <div className="flex items-center justify-center py-1">
                <div className="flex items-center gap-2 text-xs text-slate-400 font-mono">
                  <span>↓↓ Exactly-Once Consumers · Compensations · DLQ Quarantine ↓↓</span>
                </div>
              </div>

              {/* Downstream Consumers Row */}
              <div className="space-y-2">
                <div className="text-xs font-semibold text-slate-400">Consumers, Sinks & Recovery Infrastructure</div>
                <div className="grid grid-cols-1 sm:grid-cols-2 gap-2.5">
                  <button
                    onClick={() => setSelectedNode('billing-rcm')}
                    className={`p-3 rounded border text-left transition-all cursor-pointer ${
                      selectedNode === 'billing-rcm'
                        ? 'bg-white border-slate-900 shadow-sm ring-1 ring-slate-900'
                        : 'bg-white/80 border-slate-200 hover:border-slate-300'
                    }`}
                  >
                    <div className="flex items-center justify-between">
                      <div className="text-xs font-semibold text-slate-900">Billing & Payer Gateway</div>
                      <span className="text-[10px] text-slate-500 font-mono">Exactly-Once</span>
                    </div>
                    <div className="text-[11px] text-slate-500 mt-0.5">PostgreSQL Idempotency Store (Claims & EDI 837)</div>
                  </button>

                  <button
                    onClick={() => setSelectedNode('dlq-triage')}
                    className={`p-3 rounded border text-left transition-all cursor-pointer ${
                      selectedNode === 'dlq-triage'
                        ? 'bg-white border-slate-900 shadow-sm ring-1 ring-slate-900'
                        : 'bg-white/80 border-slate-200 hover:border-slate-300'
                    }`}
                  >
                    <div className="flex items-center justify-between">
                      <div className="text-xs font-semibold text-slate-900">Dead Letter Topic (.DLT)</div>
                      <span className="text-[10px] text-amber-700 font-mono">Quarantine</span>
                    </div>
                    <div className="text-[11px] text-slate-500 mt-0.5">Multi-tier exponential retry & corrupted payload audit</div>
                  </button>
                </div>
              </div>
            </div>
          </div>

          {/* Node Specification Panel */}
          <div className="lg:col-span-4 bg-white border border-slate-200 rounded-lg p-5 space-y-4">
            <div className="border-b border-slate-100 pb-3">
              <div className="text-[11px] font-medium text-slate-500 uppercase tracking-wider">{active.role}</div>
              <h2 className="text-base font-bold text-slate-900 mt-0.5">{active.title}</h2>
            </div>

            <div className="space-y-3 text-xs">
              <div>
                <div className="font-semibold text-slate-700 mb-1">Associated Kafka Topics</div>
                <div className="space-y-1">
                  {active.topics.map((t, idx) => (
                    <div key={idx} className="font-mono text-[11px] bg-slate-50 border border-slate-200 px-2 py-1 rounded text-slate-800">
                      {t}
                    </div>
                  ))}
                </div>
              </div>

              <div>
                <div className="font-semibold text-slate-700">Payload Schema / Protocol</div>
                <p className="text-slate-600 mt-0.5">{active.protocol}</p>
              </div>

              <div>
                <div className="font-semibold text-slate-700">Partition Keying Strategy</div>
                <p className="text-slate-600 mt-0.5">{active.keyStrategy}</p>
              </div>

              <div>
                <div className="font-semibold text-slate-700">Delivery & Fault Tolerance</div>
                <p className="text-slate-600 mt-0.5">{active.reliability}</p>
              </div>

              <div className="pt-2 border-t border-slate-100">
                <div className="font-semibold text-slate-700">Architectural Rationale</div>
                <p className="text-slate-600 mt-1 leading-relaxed">{active.details}</p>
              </div>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
};
