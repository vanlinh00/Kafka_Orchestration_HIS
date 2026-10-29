import React, { useState } from 'react';
import { Copy, Check, FileCode, CheckCircle2 } from 'lucide-react';
import { HIS_CODE_SNIPPETS, CodeSnippet } from '../data/code-snippets';

export const CodeInspector: React.FC = () => {
  const [selectedSnippet, setSelectedSnippet] = useState<CodeSnippet>(HIS_CODE_SNIPPETS[0]);
  const [copiedId, setCopiedId] = useState<string | null>(null);

  const handleCopy = (snippet: CodeSnippet) => {
    navigator.clipboard.writeText(snippet.code);
    setCopiedId(snippet.id);
    setTimeout(() => setCopiedId(null), 2500);
  };

  return (
    <div className="space-y-6">
      <div className="bg-white rounded-lg border border-slate-200 p-6">
        <div className="flex flex-col md:flex-row md:items-center justify-between gap-4 border-b border-slate-100 pb-4 mb-4">
          <div>
            <div className="flex items-center gap-2 text-xs font-medium text-slate-500 mb-1">
              <span>Technical Execution Guide</span>
              <span aria-hidden="true">·</span>
              <span>Java 17 / Spring Boot 3.4 / PostgreSQL / Kafka</span>
            </div>
            <h2 className="text-lg font-bold text-slate-900 tracking-tight">
              Production Codebase & Configuration Artifacts
            </h2>
            <p className="text-xs text-slate-500 mt-0.5">
              Enterprise-ready implementations of the Saga Orchestrator, Custom Partitioner, Envelope Encryption Serializer, and Error Handler.
            </p>
          </div>
          <div className="flex items-center gap-2">
            <button
              onClick={() => handleCopy(selectedSnippet)}
              className="flex items-center gap-1.5 px-3 py-1.5 bg-slate-900 hover:bg-slate-800 text-white font-semibold rounded text-xs transition-colors cursor-pointer"
            >
              {copiedId === selectedSnippet.id ? (
                <>
                  <Check className="w-3.5 h-3.5 text-emerald-400" />
                  <span>Copied to Clipboard</span>
                </>
              ) : (
                <>
                  <Copy className="w-3.5 h-3.5" />
                  <span>Copy File Content</span>
                </>
              )}
            </button>
          </div>
        </div>

        {/* File Tabs Bar */}
        <div className="flex flex-wrap gap-1.5 p-1 bg-slate-100 rounded border border-slate-200 mb-4">
          {HIS_CODE_SNIPPETS.map((snippet) => (
            <button
              key={snippet.id}
              onClick={() => setSelectedSnippet(snippet)}
              className={`flex items-center gap-1.5 px-3 py-1.5 rounded text-xs font-medium transition-colors cursor-pointer ${
                selectedSnippet.id === snippet.id
                  ? 'bg-white text-slate-900 shadow-xs font-semibold'
                  : 'text-slate-600 hover:text-slate-900'
              }`}
            >
              <FileCode className="w-3.5 h-3.5 text-slate-500" />
              <span>{snippet.fileName}</span>
            </button>
          ))}
        </div>

        {/* Code Metadata Header */}
        <div className="p-3.5 bg-slate-50 border border-slate-200 rounded mb-3 flex flex-col sm:flex-row sm:items-center justify-between gap-2 text-xs">
          <div>
            <span className="font-bold text-slate-900">{selectedSnippet.title}</span>
            <span className="text-slate-400 mx-2">|</span>
            <span className="text-slate-600">{selectedSnippet.description}</span>
          </div>
          <div className="flex items-center gap-2 shrink-0">
            <span className="font-mono text-[11px] bg-slate-200 text-slate-800 px-2 py-0.5 rounded font-semibold uppercase">
              {selectedSnippet.language}
            </span>
            <span className="font-mono text-[11px] text-slate-500">
              {selectedSnippet.category}
            </span>
          </div>
        </div>

        {/* Syntax Viewer */}
        <div className="relative">
          <pre className="p-4 bg-slate-900 text-slate-100 rounded-lg text-xs font-mono overflow-x-auto leading-relaxed border border-slate-800 max-h-[580px]">
            <code>{selectedSnippet.code}</code>
          </pre>
        </div>
      </div>
    </div>
  );
};
