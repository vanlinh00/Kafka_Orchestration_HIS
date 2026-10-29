import React, { useState } from 'react';
import { Lock, Key, ShieldCheck, Flame, Database, CheckCircle2, AlertOctagon } from 'lucide-react';

export const ComplianceAndSecuritySection: React.FC = () => {
  const [patientDekStatus, setPatientDekStatus] = useState<'ACTIVE' | 'SHREDDED'>('ACTIVE');
  const [selectedPatient, setSelectedPatient] = useState('MRN-994820');

  const rawPhiRecord = {
    patientId: 'MRN-994820',
    socialSecurityNumber: '***-**-4912',
    fullName: 'Eleanor Vance',
    dateOfBirth: '1974-06-18',
    diagnosis: 'Cerebral Ischemia (ICD-10 I67.82)',
    medicationPrescribed: 'Atorvastatin 80mg Daily',
    dekId: 'kms/arn:aws:kms:us-east-1:123456789:key/dek-patient-mrn-994820'
  };

  const handleShredDek = () => {
    setPatientDekStatus('SHREDDED');
  };

  const handleRestoreDek = () => {
    setPatientDekStatus('ACTIVE');
  };

  return (
    <div className="space-y-6">
      {/* Overview Card */}
      <div className="bg-white rounded-lg border border-slate-200 p-6 space-y-4">
        <div className="flex flex-col md:flex-row md:items-center justify-between gap-4 border-b border-slate-100 pb-4">
          <div>
            <h2 className="text-lg font-bold text-slate-900 tracking-tight">
              5. HIPAA / GDPR Compliance & Cryptographic Privacy Architecture
            </h2>
            <p className="text-xs text-slate-500 mt-0.5">
              Handling sensitive Protected Health Information (PHI) over immutable append-only event logs.
            </p>
          </div>
          <div className="font-mono text-xs bg-slate-100 px-3 py-1.5 rounded text-slate-800 font-semibold border border-slate-200">
            AES-256-GCM Envelope Encryption · Crypto-Shredding
          </div>
        </div>

        <div className="grid grid-cols-1 md:grid-cols-3 gap-4 text-xs">
          <div className="p-4 bg-slate-50 rounded border border-slate-200 space-y-2">
            <div className="font-bold text-slate-900 flex items-center gap-1.5">
              <Lock className="w-3.5 h-3.5 text-slate-700" />
              <span>Envelope Encryption (KMS)</span>
            </div>
            <p className="text-slate-600 leading-relaxed">
              Plaintext PHI never hits Kafka broker disks or OS page caches unencrypted. Each patient record is encrypted using a unique per-patient <strong>Data Encryption Key (DEK)</strong>, wrapped under a Key Encryption Key (KEK) in HashiCorp Vault or AWS KMS.
            </p>
          </div>

          <div className="p-4 bg-slate-50 rounded border border-slate-200 space-y-2">
            <div className="font-bold text-slate-900 flex items-center gap-1.5">
              <Flame className="w-3.5 h-3.5 text-rose-600" />
              <span>Crypto-Shredding (GDPR Art. 17)</span>
            </div>
            <p className="text-slate-600 leading-relaxed">
              Kafka topic segments are physically append-only and immutable. When a patient exercises their "Right to be Forgotten", the KMS permanently destroys the patient's individual DEK. All historical topic segments for that patient become unrecoverable random noise, satisfying legal erasure without corrupting Kafka log offsets!
            </p>
          </div>

          <div className="p-4 bg-slate-50 rounded border border-slate-200 space-y-2">
            <div className="font-bold text-slate-900 flex items-center gap-1.5">
              <Database className="w-3.5 h-3.5 text-blue-600" />
              <span>Log Compaction (Patient Master Index)</span>
            </div>
            <p className="text-slate-600 leading-relaxed">
              Topic <code className="font-mono text-slate-900">his.patient.master-index.v1</code> uses <code className="font-mono">cleanup.policy=compact</code>. Emitting a tombstone (<code className="font-mono">value = null</code>) permanently purges previous demographic versions during broker background compaction cycles.
            </p>
          </div>
        </div>
      </div>

      {/* Interactive Crypto-Shredding Simulator */}
      <div className="bg-white rounded-lg border border-slate-200 p-6 space-y-4">
        <div className="flex flex-col sm:flex-row sm:items-center justify-between gap-3 border-b border-slate-100 pb-3">
          <div>
            <h3 className="text-base font-bold text-slate-900">
              Interactive Crypto-Shredding & Vault DEK Simulator
            </h3>
            <p className="text-xs text-slate-500 mt-0.5">
              Observe how destroying a single patient DEK cryptographically shreds historical records across all Kafka partitions instantly.
            </p>
          </div>

          <div className="flex items-center gap-2">
            {patientDekStatus === 'ACTIVE' ? (
              <button
                onClick={handleShredDek}
                className="flex items-center gap-1.5 px-3.5 py-1.5 bg-rose-700 hover:bg-rose-800 text-white font-semibold rounded text-xs cursor-pointer shadow-xs"
              >
                <Flame className="w-3.5 h-3.5" />
                <span>Simulate GDPR Right-to-be-Forgotten (Shred DEK)</span>
              </button>
            ) : (
              <button
                onClick={handleRestoreDek}
                className="flex items-center gap-1.5 px-3.5 py-1.5 bg-slate-900 hover:bg-slate-800 text-white font-semibold rounded text-xs cursor-pointer shadow-xs"
              >
                <Key className="w-3.5 h-3.5" />
                <span>Regenerate Test DEK</span>
              </button>
            )}
          </div>
        </div>

        <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
          {/* KMS Key Vault Status */}
          <div className="p-4 bg-slate-50 border border-slate-200 rounded space-y-3 text-xs">
            <div className="flex items-center justify-between border-b border-slate-200 pb-2">
              <span className="font-semibold text-slate-800 flex items-center gap-1.5">
                <Key className="w-3.5 h-3.5 text-slate-700" />
                <span>KMS / HashiCorp Vault Key Store</span>
              </span>
              <span className={`font-mono font-bold text-[11px] px-2 py-0.5 rounded ${
                patientDekStatus === 'ACTIVE' ? 'bg-emerald-100 text-emerald-800' : 'bg-rose-100 text-rose-800'
              }`}>
                DEK STATUS: {patientDekStatus}
              </span>
            </div>

            <div className="space-y-1.5 font-mono text-[11px]">
              <div className="text-slate-500">Target Patient MRN: <strong className="text-slate-800">{rawPhiRecord.patientId}</strong></div>
              <div className="text-slate-500">DEK KMS Resource ARN:</div>
              <div className="p-2 bg-white border border-slate-200 rounded text-slate-700 break-all">
                {rawPhiRecord.dekId}
              </div>
              <div className="text-slate-500 pt-1">
                Symmetric Key Algorithm: <strong>AES-256-GCM (Authenticated Encryption with AAD)</strong>
              </div>
            </div>

            <div className="p-3 bg-white border border-slate-200 rounded text-[11px] text-slate-600 leading-relaxed">
              {patientDekStatus === 'ACTIVE' ? (
                <span>
                  Authorized consumers (Clinical Portal, EHR Sync) can decrypt the record using the active DEK key from KMS.
                </span>
              ) : (
                <span className="text-rose-700 font-medium">
                  KEY SHREDDED: The KMS key has been permanently purged with cryptographic zeroing. The ciphertext stored in Kafka logs is mathematically unrecoverable.
                </span>
              )}
            </div>
          </div>

          {/* Consumer View: Decrypted vs Shredded Payload */}
          <div className="p-4 bg-slate-900 text-slate-100 rounded space-y-3 text-xs font-mono">
            <div className="flex items-center justify-between border-b border-slate-800 pb-2">
              <span className="text-slate-400 font-semibold text-[11px]">Consumer Deserialized Payload</span>
              <span className="text-slate-500 text-[10px]">Topic: his.patient.adt.v1 (Partition 3)</span>
            </div>

            {patientDekStatus === 'ACTIVE' ? (
              <pre className="text-emerald-400 text-[11px] leading-relaxed whitespace-pre-wrap overflow-x-auto">
{`{
  "patientId": "${rawPhiRecord.patientId}",
  "fullName": "${rawPhiRecord.fullName}",
  "ssn": "${rawPhiRecord.socialSecurityNumber}",
  "dob": "${rawPhiRecord.dateOfBirth}",
  "diagnosis": "${rawPhiRecord.diagnosis}",
  "activeMedication": "${rawPhiRecord.medicationPrescribed}",
  "phiDecryptionStatus": "CLEARED_HIPAA_AUTHORIZED"
}`}
              </pre>
            ) : (
              <div className="space-y-2">
                <div className="p-2.5 bg-rose-950/60 border border-rose-800 text-rose-300 rounded text-[11px] font-sans">
                  <strong>CryptoShreddedException:</strong> DEK key revoked in KMS. Unrecoverable AES-256-GCM ciphertext:
                </div>
                <pre className="text-slate-500 text-[10px] break-all whitespace-pre-wrap leading-tight bg-black/40 p-2.5 rounded">
{`\x1B\x4F\xA9\xFE\x8B\x22\x90\xAA\x7C\x01\xDF\x6A\x99\xC2\x04\x88\x12\xFE\xDD\x09\x76\x5A\x31\xBC\x88\x09\x1F\x44\x90\xAA\x23\x7B\x5E\x01\x9B\x77\x55\x8A\xEE\x99\x22\x33\x44\x55\x66\x77\x88\x99\xAA\xBB\xCC\xDD\xEE\xFF\x00\x11\x22\x33\x44\x55\x66\x77\x88\x99\xAA\xBB\xCC\xDD\xEE\xFF\x00\x11\x22\x33\x44\x55\x66\x77\x88\x99\xAA\xBB\xCC\xDD\xEE\xFF\x00\x11\x22\x33\x44\x55\x66\x77\x88\x99\xAA\xBB\xCC\xDD\xEE\xFF [UNRECOVERABLE CIPHERTEXT]`}
                </pre>
              </div>
            )}

            <div className="pt-2 text-[10px] text-slate-400 border-t border-slate-800 flex justify-between">
              <span>Kafka Offset Preserved: <strong>#482910</strong></span>
              <span>Consumer Group Health: <strong>NOMINAL</strong></span>
            </div>
          </div>
        </div>
      </div>

      {/* Log Compaction Rules Reference Table */}
      <div className="bg-white rounded-lg border border-slate-200 p-6 space-y-3">
        <h3 className="text-sm font-bold text-slate-900">
          Patient Master Index (EMPI) Log Compaction Rules
        </h3>
        <p className="text-xs text-slate-600">
          Configured topic settings for <code className="font-mono text-slate-800 font-semibold">his.patient.master-index.v1</code> to maintain canonical patient identities without unbounded log growth.
        </p>

        <div className="overflow-x-auto">
          <table className="w-full text-left text-xs border-collapse">
            <thead>
              <tr className="bg-slate-50 border-b border-slate-200 text-slate-500 font-semibold uppercase text-[10px]">
                <th className="py-2.5 px-3">Configuration Parameter</th>
                <th className="py-2.5 px-3">Recommended Value</th>
                <th className="py-2.5 px-4">Hospital Architectural Purpose</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-slate-100 text-slate-700">
              <tr>
                <td className="py-2.5 px-3 font-mono font-semibold text-slate-900">cleanup.policy</td>
                <td className="py-2.5 px-3 font-mono text-blue-700 font-bold">compact</td>
                <td className="py-2.5 px-4 text-slate-600">Retains the latest known record for each patient ID indefinitely.</td>
              </tr>
              <tr>
                <td className="py-2.5 px-3 font-mono font-semibold text-slate-900">delete.retention.ms</td>
                <td className="py-2.5 px-3 font-mono">86400000 (24 hours)</td>
                <td className="py-2.5 px-4 text-slate-600">Ensures downstream consumers have 24 hours to observe patient merge / purge tombstones before garbage collection.</td>
              </tr>
              <tr>
                <td className="py-2.5 px-3 font-mono font-semibold text-slate-900">min.compaction.lag.ms</td>
                <td className="py-2.5 px-3 font-mono">3600000 (1 hour)</td>
                <td className="py-2.5 px-4 text-slate-600">Guarantees in-flight consumers can see rapid patient demographic changes before segments compact.</td>
              </tr>
              <tr>
                <td className="py-2.5 px-3 font-mono font-semibold text-slate-900">min.cleanable.dirty.ratio</td>
                <td className="py-2.5 px-3 font-mono">0.5 (50%)</td>
                <td className="py-2.5 px-4 text-slate-600">Triggers log cleaner threads once 50% of the segment consists of superseded duplicates.</td>
              </tr>
            </tbody>
          </table>
        </div>
      </div>
    </div>
  );
};
