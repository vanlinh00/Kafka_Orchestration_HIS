import React, { useState } from 'react';
import { ShieldCheck, AlertTriangle, RefreshCw, CheckCircle, Trash2, ArrowUpRight, Terminal } from 'lucide-react';
import { DlqMessage } from '../types/his-kafka';

export const ReliabilityAndDlqSection: React.FC = () => {
  const [dlqList, setDlqList] = useState<DlqMessage[]>([
    {
      id: 'DLQ-00192',
      originalTopic: 'his.patient.adt.v1',
      failedTimestamp: '2026-10-28 19:42:10 UTC',
      partition: 4,
      offset: 184920,
      key: 'MRN-77391',
      exceptionClass: 'org.apache.kafka.common.errors.SerializationException',
      exceptionMessage: 'Malformed HL7 MSH segment: Field delimiter missing at position 12. Invalid FHIR Encounter structure.',
      rawPayload: 'MSH|^~\\&|EMR_EPIC||PATHOLOGY||202610281942||ADT^A08|MSG00912|P|2.5\nEVN||202610281942\nPID|1||MRN-77391^^^HOSP||DOE^JOHN^^^^^L||19800512|M|||CORRUPTED_STREET_DELIMITER_NULL_BYTE',
      status: 'QUARANTINED',
      retryCount: 3
    },
    {
      id: 'DLQ-00193',
      originalTopic: 'his.clinical.lab-results.v1',
      failedTimestamp: '2026-10-28 19:55:04 UTC',
      partition: 7,
      offset: 92831,
      key: 'ORD-99120',
      exceptionClass: 'io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException',
      exceptionMessage: 'Schema not found in registry: subject=his.clinical.lab-results.v1-value, versionId=420. Schema validation rejected.',
      rawPayload: '{"resourceType":"Observation","status":"final","code":{"coding":[{"system":"http://loinc.org","code":"1751-7"}]},"subject":{"reference":"Patient/MRN-4421"},"unknownFieldDisallowedBySchema":"ERR_SCHEMA_VIOLATION"}',
      status: 'QUARANTINED',
      retryCount: 3
    }
  ]);

  const [selectedMessage, setSelectedMessage] = useState<DlqMessage>(dlqList[0]);
  const [actionSuccessMessage, setActionSuccessMessage] = useState<string | null>(null);

  const handleInjectPoisonPill = (type: 'hl7' | 'fhir') => {
    const newMsg: DlqMessage = {
      id: `DLQ-${Math.floor(10000 + Math.random() * 90000)}`,
      originalTopic: type === 'hl7' ? 'his.patient.adt.v1' : 'his.pharmacy.prescriptions.v1',
      failedTimestamp: new Date().toISOString().replace('T', ' ').substring(0, 19) + ' UTC',
      partition: Math.floor(Math.random() * 12),
      offset: Math.floor(100000 + Math.random() * 500000),
      key: type === 'hl7' ? 'MRN-NEW-CORRUPT' : 'RX-POISON-PILL',
      exceptionClass: type === 'hl7' ? 'ca.uhn.hl7v2.HL7Exception' : 'jakarta.validation.ValidationException',
      exceptionMessage: type === 'hl7'
        ? 'HL7 v2.5 Parse Error: Unrecognized segment Z-LEGACY-HOSPITAL in ADT^A01 feed.'
        : 'Validation failed on FHIR MedicationRequest: Dosage instructions violate maximum safe threshold (10,000mg vs max 1,000mg).',
      rawPayload: type === 'hl7'
        ? 'MSH|^~\\&|ADT|HOSP|||20261028||ADT^A01|001|P|2.5\nZ-LEGACY-HOSPITAL|NON_CONFORMING_UNREGISTERED_DATA'
        : '{"resourceType":"MedicationRequest","dosageInstruction":[{"doseAndRate":[{"doseQuantity":{"value":10000,"unit":"mg"}}]}]}',
      status: 'QUARANTINED',
      retryCount: 3
    };

    setDlqList(prev => [newMsg, ...prev]);
    setSelectedMessage(newMsg);
    setActionSuccessMessage(`Poison pill quarantined to ${newMsg.originalTopic}.DLT with error headers!`);
    setTimeout(() => setActionSuccessMessage(null), 3000);
  };

  const handleReplay = (id: string) => {
    setDlqList(prev => prev.map(m => m.id === id ? { ...m, status: 'REPLAYED' } : m));
    setActionSuccessMessage(`Message ${id} re-published to source topic after payload sanitization.`);
    setTimeout(() => setActionSuccessMessage(null), 3000);
  };

  const handleDiscard = (id: string) => {
    setDlqList(prev => prev.filter(m => m.id !== id));
    setActionSuccessMessage(`Message ${id} discarded and archived to long-term audit store.`);
    setTimeout(() => setActionSuccessMessage(null), 3000);
  };

  return (
    <div className="space-y-6">
      {/* Guarantees & Offset Commit Strategy */}
      <div className="bg-white rounded-lg border border-slate-200 p-6 space-y-4">
        <div className="flex flex-col md:flex-row md:items-center justify-between gap-4 border-b border-slate-100 pb-4">
          <div>
            <h2 className="text-lg font-bold text-slate-900 tracking-tight">
              3. Offset Management, Delivery Guarantees & Exactly-Once Semantics
            </h2>
            <p className="text-xs text-slate-500 mt-0.5">
              Enforcing zero-data-loss offset discipline and dual-write elimination for medical and financial transactions.
            </p>
          </div>
          <div className="font-mono text-xs bg-slate-100 px-3 py-1.5 rounded text-slate-800 font-semibold border border-slate-200">
            enable.auto.commit = false · MANUAL_IMMEDIATE
          </div>
        </div>

        <div className="grid grid-cols-1 md:grid-cols-3 gap-4 text-xs">
          <div className="p-4 bg-slate-50 rounded border border-slate-200 space-y-2">
            <div className="font-bold text-slate-900 flex items-center justify-between">
              <span>Offset Commit Strategy</span>
              <span className="font-mono text-[10px] text-slate-500">Consumer Invariant</span>
            </div>
            <p className="text-slate-600 leading-relaxed">
              Auto-commit (<code className="text-slate-900 font-mono">enable.auto.commit=true</code>) is strictly forbidden in hospital systems. If a pod crashes mid-processing, unpersisted messages are committed and lost forever.
            </p>
            <div className="pt-2 text-[11px] text-slate-500 border-t border-slate-200 font-mono">
              Mode: ContainerProperties.AckMode.MANUAL_IMMEDIATE synchronously invoked only AFTER the local PostgreSQL transaction commits.
            </div>
          </div>

          <div className="p-4 bg-slate-50 rounded border border-slate-200 space-y-2">
            <div className="font-bold text-slate-900 flex items-center justify-between">
              <span>Exactly-Once Semantics (EOS)</span>
              <span className="font-mono text-[10px] text-slate-500">Financial & Pharmacy</span>
            </div>
            <p className="text-slate-600 leading-relaxed">
              For billing charges and medication dispatches, we employ dual-write elimination using PostgreSQL atomic deduplication table:
            </p>
            <div className="pt-2 text-[11px] text-slate-800 bg-white p-2 rounded border border-slate-200 font-mono">
              fingerprint: (consumer_group, topic, partition, offset)
            </div>
            <div className="text-[11px] text-slate-500">
              Duplicates caused by network reconnects or rebalances are silently skipped without re-billing.
            </div>
          </div>

          <div className="p-4 bg-slate-50 rounded border border-slate-200 space-y-2">
            <div className="font-bold text-slate-900 flex items-center justify-between">
              <span>Out-of-Order Handling Strategy</span>
              <span className="font-mono text-[10px] text-slate-500">Timeline Preservation</span>
            </div>
            <p className="text-slate-600 leading-relaxed">
              1. Deterministic hashing ensures single-patient events always hit the same partition.
              <br />
              2. Single consumer thread per partition guarantees sequential timeline processing.
              <br />
              3. Logical clock checks: If a discharge event arrives before an admission, it is held in a 60-second Redis delay window before raising an ops alert.
            </p>
          </div>
        </div>
      </div>

      {/* Dead Letter Queue (DLQ) Architecture & Triage Console */}
      <div className="bg-white rounded-lg border border-slate-200 p-6 space-y-5">
        <div className="flex flex-col sm:flex-row sm:items-center justify-between gap-3 border-b border-slate-100 pb-4">
          <div>
            <div className="text-[11px] font-semibold text-slate-500 uppercase tracking-wider">Fault Tolerance & Incident Remediation</div>
            <h3 className="text-base font-bold text-slate-900 mt-0.5">
              Dead Letter Topic (.DLT) & Poison Pill Triage Console
            </h3>
            <p className="text-xs text-slate-600 mt-0.5">
              When corrupt HL7 v2 messages or malformed FHIR payloads fail deserialization, they are quarantined to <code className="font-mono text-slate-800">&lt;topic&gt;.DLT</code> with error headers rather than stalling the partition.
            </p>
          </div>
          <div className="flex items-center gap-2">
            <button
              onClick={() => handleInjectPoisonPill('hl7')}
              className="px-3 py-1.5 text-xs font-semibold text-rose-700 bg-rose-50 border border-rose-200 hover:bg-rose-100 rounded transition-colors cursor-pointer"
            >
              + Inject Corrupt HL7
            </button>
            <button
              onClick={() => handleInjectPoisonPill('fhir')}
              className="px-3 py-1.5 text-xs font-semibold text-amber-700 bg-amber-50 border border-amber-200 hover:bg-amber-100 rounded transition-colors cursor-pointer"
            >
              + Inject Schema Violation
            </button>
          </div>
        </div>

        {actionSuccessMessage && (
          <div className="p-3 bg-emerald-50 border border-emerald-200 text-emerald-800 rounded text-xs flex items-center gap-2 font-medium">
            <CheckCircle className="w-4 h-4 text-emerald-600 shrink-0" />
            <span>{actionSuccessMessage}</span>
          </div>
        )}

        <div className="grid grid-cols-1 lg:grid-cols-12 gap-6">
          {/* Quarantined Messages List */}
          <div className="lg:col-span-5 space-y-2">
            <div className="text-xs font-semibold text-slate-500 flex justify-between">
              <span>Quarantined Records ({dlqList.length})</span>
              <span>Status: ACTIVE_DLT</span>
            </div>

            <div className="space-y-2">
              {dlqList.map((msg) => (
                <div
                  key={msg.id}
                  onClick={() => setSelectedMessage(msg)}
                  className={`p-3 rounded border text-xs cursor-pointer transition-all ${
                    selectedMessage?.id === msg.id
                      ? 'bg-slate-900 text-white border-slate-900 shadow-sm'
                      : 'bg-white hover:bg-slate-50 border-slate-200 text-slate-800'
                  }`}
                >
                  <div className="flex items-center justify-between text-[11px] mb-1">
                    <span className="font-mono font-bold">{msg.id}</span>
                    <span className={`font-mono text-[10px] ${selectedMessage?.id === msg.id ? 'text-slate-300' : 'text-slate-500'}`}>
                      {msg.failedTimestamp}
                    </span>
                  </div>
                  <div className={`font-mono text-[11px] truncate ${selectedMessage?.id === msg.id ? 'text-slate-200' : 'text-slate-600'}`}>
                    Topic: {msg.originalTopic} · Key: {msg.key}
                  </div>
                  <div className={`text-[11px] mt-1 line-clamp-1 ${selectedMessage?.id === msg.id ? 'text-rose-200' : 'text-rose-600'}`}>
                    {msg.exceptionMessage}
                  </div>
                </div>
              ))}
            </div>
          </div>

          {/* Detailed Message Triage Inspector */}
          <div className="lg:col-span-7 bg-slate-50 border border-slate-200 rounded-lg p-5 space-y-4 text-xs">
            {selectedMessage ? (
              <>
                <div className="flex items-center justify-between border-b border-slate-200 pb-3">
                  <div>
                    <span className="font-mono font-bold text-sm text-slate-900">{selectedMessage.id}</span>
                    <div className="text-slate-500 text-[11px] mt-0.5">
                      Failed on <span className="font-mono text-slate-800">{selectedMessage.originalTopic}</span> (Partition {selectedMessage.partition}, Offset {selectedMessage.offset})
                    </div>
                  </div>
                  <div className="flex items-center gap-2">
                    <button
                      onClick={() => handleReplay(selectedMessage.id)}
                      className="flex items-center gap-1 px-3 py-1 bg-slate-900 hover:bg-slate-800 text-white font-semibold rounded text-[11px] cursor-pointer"
                    >
                      <RefreshCw className="w-3 h-3" />
                      <span>Sanitize & Replay</span>
                    </button>
                    <button
                      onClick={() => handleDiscard(selectedMessage.id)}
                      className="flex items-center gap-1 px-2.5 py-1 border border-slate-300 hover:bg-white text-slate-700 rounded text-[11px] cursor-pointer"
                    >
                      <Trash2 className="w-3 h-3 text-rose-600" />
                      <span>Archive</span>
                    </button>
                  </div>
                </div>

                <div>
                  <div className="font-semibold text-slate-700 mb-1">Diagnostic Exception:</div>
                  <div className="p-2.5 bg-rose-50 border border-rose-200 rounded text-rose-800 font-mono text-[11px] leading-relaxed">
                    <div className="font-bold">{selectedMessage.exceptionClass}</div>
                    <div className="mt-1">{selectedMessage.exceptionMessage}</div>
                  </div>
                </div>

                <div>
                  <div className="font-semibold text-slate-700 mb-1">Raw Payload (Quarantined):</div>
                  <pre className="p-3 bg-slate-900 text-slate-100 rounded text-[11px] font-mono overflow-x-auto whitespace-pre-wrap max-h-48">
                    {selectedMessage.rawPayload}
                  </pre>
                </div>

                <div className="pt-2 border-t border-slate-200 text-[11px] text-slate-500 flex items-center justify-between">
                  <span>Kafka Consumer Error Handler: Spring <code className="font-mono text-slate-700">DefaultErrorHandler</code></span>
                  <span>Exponential Retries Attempted: <strong className="text-slate-800">{selectedMessage.retryCount}/3</strong></span>
                </div>
              </>
            ) : (
              <div className="text-slate-400 py-12 text-center">Select a quarantined message to inspect.</div>
            )}
          </div>
        </div>
      </div>
    </div>
  );
};
