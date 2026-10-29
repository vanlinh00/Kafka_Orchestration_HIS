import React, { useState, useMemo } from 'react';
import { Search, Filter, Hash, Sliders, CheckCircle2, Copy, Check } from 'lucide-react';
import { HIS_TOPIC_TAXONOMY, TAXONOMY_RULES } from '../data/topics-catalog';
import { KafkaTopicDef, DomainType } from '../types/his-kafka';

export const TopicTaxonomySection: React.FC = () => {
  const [searchQuery, setSearchQuery] = useState('');
  const [selectedDomain, setSelectedDomain] = useState<string>('all');
  const [selectedTopic, setSelectedTopic] = useState<KafkaTopicDef>(HIS_TOPIC_TAXONOMY[0]);
  const [copiedTopic, setCopiedTopic] = useState<string | null>(null);

  // Partition Calculator State
  const [targetThroughput, setTargetThroughput] = useState<number>(4500);
  const [consumerCapacity, setConsumerCapacity] = useState<number>(500);
  const [headroomRatio, setHeadroomRatio] = useState<number>(1.5);

  // Key Hasher state
  const [sampleKey, setSampleKey] = useState<string>('MRN-948201');
  const [samplePartitions, setSamplePartitions] = useState<number>(12);
  const [useSalt, setUseSalt] = useState<boolean>(false);

  const filteredTopics = useMemo(() => {
    return HIS_TOPIC_TAXONOMY.filter(topic => {
      const matchesSearch = topic.name.toLowerCase().includes(searchQuery.toLowerCase()) ||
                            topic.description.toLowerCase().includes(searchQuery.toLowerCase()) ||
                            topic.partitionKey.toLowerCase().includes(searchQuery.toLowerCase());
      const matchesDomain = selectedDomain === 'all' || topic.domain === selectedDomain;
      return matchesSearch && matchesDomain;
    });
  }, [searchQuery, selectedDomain]);

  const calculatedPartitions = useMemo(() => {
    if (!consumerCapacity || consumerCapacity <= 0) return 1;
    const raw = Math.ceil(targetThroughput / consumerCapacity) * headroomRatio;
    return Math.max(1, Math.ceil(raw));
  }, [targetThroughput, consumerCapacity, headroomRatio]);

  // Pseudo-Murmur2 hash calculation for simulation
  const computedPartition = useMemo(() => {
    let keyToHash = sampleKey.trim().toUpperCase();
    if (useSalt) {
      keyToHash += '#1740700000'; // Current minute epoch window
    }
    let hash = 0;
    for (let i = 0; i < keyToHash.length; i++) {
      hash = ((hash << 5) - hash) + keyToHash.charCodeAt(i);
      hash |= 0;
    }
    return Math.abs(hash) % Math.max(1, samplePartitions);
  }, [sampleKey, samplePartitions, useSalt]);

  const handleCopyConfig = (topic: KafkaTopicDef) => {
    const configStr = `kafka-topics.sh --create --bootstrap-server localhost:9092 \\
  --topic ${topic.name} \\
  --partitions ${topic.partitions} \\
  --replication-factor ${topic.replicationFactor} \\
  --config min.insync.replicas=${topic.minInsyncReplicas} \\
  --config cleanup.policy=${topic.cleanupPolicy} \\
  --config retention.ms=${typeof topic.retentionHours === 'number' ? topic.retentionHours * 3600000 : -1} \\
  --config max.message.bytes=${topic.maxMessageBytes.split(' ')[0]}`;

    navigator.clipboard.writeText(configStr);
    setCopiedTopic(topic.name);
    setTimeout(() => setCopiedTopic(null), 2500);
  };

  return (
    <div className="space-y-6">
      {/* Naming Convention & CloudEvents Header Specification */}
      <div className="bg-white rounded-lg border border-slate-200 p-6">
        <div className="flex flex-col md:flex-row md:items-center justify-between gap-4 border-b border-slate-100 pb-4 mb-4">
          <div>
            <h2 className="text-lg font-bold text-slate-900 tracking-tight">1. Standardized Topic Taxonomy</h2>
            <p className="text-xs text-slate-500 mt-0.5">Strict hierarchical naming standard and CloudEvents enterprise envelope format.</p>
          </div>
          <div className="font-mono text-xs bg-slate-100 px-3 py-1.5 rounded text-slate-800 font-semibold border border-slate-200">
            his.&lt;domain&gt;.&lt;entity&gt;.&lt;event-type&gt;.v&lt;major&gt;
          </div>
        </div>

        <div className="grid grid-cols-1 md:grid-cols-4 gap-4 text-xs">
          <div className="bg-slate-50 p-3.5 rounded border border-slate-200">
            <div className="font-semibold text-slate-900 mb-1">Prefix & Domain</div>
            <div className="font-mono text-slate-700">his.patient | his.clinical | his.pharmacy | his.billing</div>
            <div className="text-slate-500 mt-1">Bounded contexts matching hospital organizational domains.</div>
          </div>
          <div className="bg-slate-50 p-3.5 rounded border border-slate-200">
            <div className="font-semibold text-slate-900 mb-1">Entity Name</div>
            <div className="font-mono text-slate-700">adt, lab-results, prescriptions, claims, telemetry</div>
            <div className="text-slate-500 mt-1">Singular or plural business domain aggregate noun.</div>
          </div>
          <div className="bg-slate-50 p-3.5 rounded border border-slate-200">
            <div className="font-semibold text-slate-900 mb-1">Event Type Semantic</div>
            <div className="font-mono text-slate-700">events | commands | snapshot | streaming</div>
            <div className="text-slate-500 mt-1">Declares whether record is state fact, directive, or sensor stream.</div>
          </div>
          <div className="bg-slate-50 p-3.5 rounded border border-slate-200">
            <div className="font-semibold text-slate-900 mb-1">Version Indicator</div>
            <div className="font-mono text-slate-700">v1 | v2 (Semantic Major)</div>
            <div className="text-slate-500 mt-1">Breaking changes bump topic version. Minor changes evolved in Schema Registry.</div>
          </div>
        </div>

        {/* CloudEvents Header Matrix */}
        <div className="mt-4 pt-4 border-t border-slate-100">
          <div className="text-xs font-semibold text-slate-700 mb-2">Mandatory Kafka Record Headers Contract</div>
          <div className="grid grid-cols-1 sm:grid-cols-3 gap-2 text-xs">
            {TAXONOMY_RULES.standardHeaders.slice(0, 6).map((h, i) => (
              <div key={i} className="flex items-start gap-2 bg-slate-50/70 p-2 rounded border border-slate-100">
                <span className="font-mono font-semibold text-slate-800 text-[11px] shrink-0">{h.key}:</span>
                <span className="text-slate-500 text-[11px]">{h.description}</span>
              </div>
            ))}
          </div>
        </div>
      </div>

      {/* Searchable Topic Catalog Data Grid */}
      <div className="bg-white rounded-lg border border-slate-200 overflow-hidden">
        <div className="p-4 border-b border-slate-200 flex flex-col sm:flex-row items-stretch sm:items-center justify-between gap-3">
          <div className="flex items-center gap-2">
            <div className="relative">
              <Search className="w-4 h-4 text-slate-400 absolute left-2.5 top-2.5" />
              <input
                type="text"
                placeholder="Search topics, keys, schemas..."
                value={searchQuery}
                onChange={(e) => setSearchQuery(e.target.value)}
                className="pl-8 pr-3 py-1.5 text-xs rounded border border-slate-200 focus:outline-none focus:ring-1 focus:ring-slate-900 w-64"
              />
            </div>
            {/* Filter buttons */}
            <div className="flex items-center gap-1 bg-slate-100 p-1 rounded">
              {['all', 'patient', 'clinical', 'pharmacy', 'billing', 'telemetry'].map((domain) => (
                <button
                  key={domain}
                  onClick={() => setSelectedDomain(domain)}
                  className={`px-2.5 py-1 text-xs font-medium rounded transition-colors capitalize cursor-pointer ${
                    selectedDomain === domain ? 'bg-white text-slate-900 shadow-xs' : 'text-slate-600 hover:text-slate-900'
                  }`}
                >
                  {domain}
                </button>
              ))}
            </div>
          </div>

          <div className="text-xs text-slate-500">
            Showing <strong className="text-slate-900">{filteredTopics.length}</strong> configured topics
          </div>
        </div>

        {/* High-Density Data Grid */}
        <div className="overflow-x-auto">
          <table className="w-full text-left text-xs border-collapse">
            <thead>
              <tr className="bg-slate-50 border-b border-slate-200 text-slate-500 font-semibold uppercase text-[10px] tracking-wider">
                <th className="py-2.5 px-4">Topic Name</th>
                <th className="py-2.5 px-3">Partition Key</th>
                <th className="py-2.5 px-2 text-right">Partitions</th>
                <th className="py-2.5 px-2 text-center">RF / ISR</th>
                <th className="py-2.5 px-3">Cleanup</th>
                <th className="py-2.5 px-3">Retention</th>
                <th className="py-2.5 px-3">Throughput</th>
                <th className="py-2.5 px-3 text-center">Actions</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-slate-100">
              {filteredTopics.map((topic) => (
                <tr
                  key={topic.name}
                  onClick={() => setSelectedTopic(topic)}
                  className={`hover:bg-slate-50/80 transition-colors cursor-pointer ${
                    selectedTopic.name === topic.name ? 'bg-slate-100/60 font-medium' : ''
                  }`}
                >
                  <td className="py-2.5 px-4">
                    <div className="font-mono text-slate-900 font-semibold">{topic.name}</div>
                    <div className="text-[11px] text-slate-500 truncate max-w-sm">{topic.description}</div>
                  </td>
                  <td className="py-2.5 px-3 font-mono text-slate-700">{topic.partitionKey}</td>
                  <td className="py-2.5 px-2 text-right font-mono tabular-nums text-slate-800">{topic.partitions}</td>
                  <td className="py-2.5 px-2 text-center font-mono text-[11px] text-slate-600">
                    {topic.replicationFactor} / {topic.minInsyncReplicas}
                  </td>
                  <td className="py-2.5 px-3">
                    <span className={`font-mono text-[11px] ${topic.cleanupPolicy === 'compact' ? 'text-blue-700 font-semibold' : 'text-slate-600'}`}>
                      {topic.cleanupPolicy}
                    </span>
                  </td>
                  <td className="py-2.5 px-3 text-slate-600 font-mono text-[11px]">
                    {typeof topic.retentionHours === 'number' ? `${topic.retentionHours}h (${Math.round(topic.retentionHours / 24)}d)` : topic.retentionHours}
                  </td>
                  <td className="py-2.5 px-3 text-slate-700 font-mono tabular-nums text-[11px]">{topic.throughputEstimate}</td>
                  <td className="py-2.5 px-3 text-center" onClick={(e) => e.stopPropagation()}>
                    <button
                      onClick={() => handleCopyConfig(topic)}
                      title="Copy CLI creation command"
                      className="inline-flex items-center gap-1 text-[11px] text-slate-600 hover:text-slate-900 border border-slate-200 hover:border-slate-300 rounded px-2 py-0.5 bg-white cursor-pointer"
                    >
                      {copiedTopic === topic.name ? <Check className="w-3 h-3 text-emerald-600" /> : <Copy className="w-3 h-3" />}
                      <span>CLI</span>
                    </button>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>

        {/* Selected Topic Detailed Rationale Inspector */}
        <div className="p-4 bg-slate-50 border-t border-slate-200 grid grid-cols-1 md:grid-cols-3 gap-4 text-xs">
          <div>
            <div className="font-semibold text-slate-700 mb-1">Key Strategy & Ordering Invariant</div>
            <p className="text-slate-600 leading-relaxed">{selectedTopic.partitionKeyRationale}</p>
          </div>
          <div>
            <div className="font-semibold text-slate-700 mb-1">Delivery Semantics & Schema</div>
            <div className="space-y-1 text-slate-600 font-mono text-[11px]">
              <div>Guarantee: <strong>{selectedTopic.guarantee}</strong></div>
              <div>Schema Registry: <strong>{selectedTopic.schemaFormat} (FHIR R4)</strong></div>
              <div>Max Payload Size: <strong>{selectedTopic.maxMessageBytes}</strong></div>
            </div>
          </div>
          <div>
            <div className="font-semibold text-slate-700 mb-1">Kafka CLI Command</div>
            <pre className="p-2 bg-slate-900 text-slate-100 rounded text-[10px] font-mono overflow-x-auto leading-tight">
{`kafka-topics.sh --create --bootstrap-server localhost:9092 \\
  --topic ${selectedTopic.name} \\
  --partitions ${selectedTopic.partitions} \\
  --replication-factor ${selectedTopic.replicationFactor} \\
  --config min.insync.replicas=${selectedTopic.minInsyncReplicas}`}
            </pre>
          </div>
        </div>
      </div>

      {/* Interactive Partitioning & Key Sizing Calculator */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        {/* Partition Sizing Calculator */}
        <div className="bg-white rounded-lg border border-slate-200 p-5 space-y-4">
          <div className="flex items-center gap-2 border-b border-slate-100 pb-3">
            <Sliders className="w-4 h-4 text-slate-800" />
            <h3 className="text-sm font-bold text-slate-900">Partition Sizing Mathematical Model</h3>
          </div>
          <p className="text-xs text-slate-600">
            Calculate partition requirements based on hospital peak volume, consumer DB write throughput, and failover buffer.
          </p>

          <div className="space-y-3 text-xs">
            <div>
              <div className="flex justify-between text-slate-700 mb-1">
                <span>Peak Target Throughput:</span>
                <span className="font-mono font-semibold tabular-nums">{targetThroughput.toLocaleString()} msg/sec</span>
              </div>
              <input
                type="range"
                min="500"
                max="50000"
                step="500"
                value={targetThroughput}
                onChange={(e) => setTargetThroughput(Number(e.target.value))}
                className="w-full accent-slate-900 cursor-pointer"
              />
            </div>

            <div>
              <div className="flex justify-between text-slate-700 mb-1">
                <span>Consumer Postgres Write Capacity per Thread:</span>
                <span className="font-mono font-semibold tabular-nums">{consumerCapacity.toLocaleString()} msg/sec</span>
              </div>
              <input
                type="range"
                min="100"
                max="2500"
                step="50"
                value={consumerCapacity}
                onChange={(e) => setConsumerCapacity(Number(e.target.value))}
                className="w-full accent-slate-900 cursor-pointer"
              />
            </div>

            <div>
              <div className="flex justify-between text-slate-700 mb-1">
                <span>Headroom / Rebalance Safety Multiplier:</span>
                <span className="font-mono font-semibold tabular-nums">{headroomRatio}x</span>
              </div>
              <input
                type="range"
                min="1.0"
                max="3.0"
                step="0.25"
                value={headroomRatio}
                onChange={(e) => setHeadroomRatio(Number(e.target.value))}
                className="w-full accent-slate-900 cursor-pointer"
              />
            </div>
          </div>

          <div className="p-3 bg-slate-50 border border-slate-200 rounded text-xs flex items-center justify-between">
            <div>
              <div className="text-[11px] text-slate-500">Recommended Partition Count:</div>
              <div className="text-xl font-bold font-mono text-slate-900 tabular-nums">
                {calculatedPartitions} <span className="text-xs font-normal text-slate-500">partitions</span>
              </div>
            </div>
            <div className="text-right text-[11px] text-slate-500">
              <div>Allows <strong>{calculatedPartitions}</strong> parallel Spring consumers</div>
              <div>Min broker nodes recommended: <strong>{Math.min(calculatedPartitions, 6)}</strong></div>
            </div>
          </div>
        </div>

        {/* Partition Key Router & Hot-Spot Simulator */}
        <div className="bg-white rounded-lg border border-slate-200 p-5 space-y-4">
          <div className="flex items-center gap-2 border-b border-slate-100 pb-3">
            <Hash className="w-4 h-4 text-slate-800" />
            <h3 className="text-sm font-bold text-slate-900">Deterministic Key Hashing & Salt Simulator</h3>
          </div>
          <p className="text-xs text-slate-600">
            Test how Murmur2 hashing maps clinical identifiers to partitions, and how temporal salting prevents ICU hot partition spikes.
          </p>

          <div className="space-y-3 text-xs">
            <div>
              <label className="block text-slate-700 font-semibold mb-1">Enter Test Identifier (Key):</label>
              <div className="flex gap-2">
                <input
                  type="text"
                  value={sampleKey}
                  onChange={(e) => setSampleKey(e.target.value)}
                  className="w-full px-3 py-1.5 border border-slate-200 rounded font-mono text-xs focus:ring-1 focus:ring-slate-900"
                />
                <button
                  onClick={() => setSampleKey(`MRN-${Math.floor(100000 + Math.random() * 900000)}`)}
                  className="px-2.5 py-1 text-xs border border-slate-200 rounded hover:bg-slate-50 whitespace-nowrap cursor-pointer"
                >
                  Random MRN
                </button>
              </div>
            </div>

            <div className="grid grid-cols-2 gap-3">
              <div>
                <label className="block text-slate-700 font-medium mb-1">Target Partitions:</label>
                <select
                  value={samplePartitions}
                  onChange={(e) => setSamplePartitions(Number(e.target.value))}
                  className="w-full px-2 py-1.5 border border-slate-200 rounded text-xs"
                >
                  <option value={8}>8 Partitions</option>
                  <option value={12}>12 Partitions (ADT default)</option>
                  <option value={16}>16 Partitions</option>
                  <option value={64}>64 Partitions (Telemetry)</option>
                </select>
              </div>

              <div>
                <label className="block text-slate-700 font-medium mb-1">Apply ICU Time-Window Salt:</label>
                <button
                  onClick={() => setUseSalt(!useSalt)}
                  className={`w-full py-1.5 px-3 rounded border text-xs font-semibold cursor-pointer transition-colors ${
                    useSalt ? 'bg-slate-900 text-white border-slate-900' : 'bg-white text-slate-700 border-slate-200 hover:bg-slate-50'
                  }`}
                >
                  {useSalt ? 'Salt ON (#minute_bucket)' : 'Salt OFF (Pure Patient ID)'}
                </button>
              </div>
            </div>
          </div>

          <div className="p-3 bg-slate-900 text-slate-100 rounded text-xs space-y-1 font-mono">
            <div className="text-slate-400 text-[11px]">MURMUR2 HASH ROUTING RESULT:</div>
            <div className="flex items-center justify-between pt-1">
              <span>Resolved Target Partition:</span>
              <span className="text-emerald-400 font-bold text-sm">Partition #{computedPartition}</span>
            </div>
            <div className="text-[11px] text-slate-400 pt-1 border-t border-slate-800">
              {useSalt ? (
                <span>Salted: Distributed across partitions every 60s to prevent ICU bed hotspots.</span>
              ) : (
                <span>Unsalted: All events for this patient strictly land on Partition #{computedPartition} in FIFO order.</span>
              )}
            </div>
          </div>
        </div>
      </div>
    </div>
  );
};
