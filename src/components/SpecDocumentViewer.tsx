import React, { useState } from 'react';
import { Download, Copy, Check, FileText } from 'lucide-react';
import { HIS_FULL_SPEC_MARKDOWN } from '../data/spec-document';

export const SpecDocumentViewer: React.FC<{ onDownload: () => void }> = ({ onDownload }) => {
  const [copied, setCopied] = useState(false);

  const handleCopyMarkdown = () => {
    navigator.clipboard.writeText(HIS_FULL_SPEC_MARKDOWN);
    setCopied(true);
    setTimeout(() => setCopied(false), 2500);
  };

  return (
    <div className="space-y-6">
      <div className="bg-white rounded-lg border border-slate-200 p-6">
        <div className="flex flex-col md:flex-row md:items-center justify-between gap-4 border-b border-slate-100 pb-4 mb-4">
          <div>
            <div className="flex items-center gap-2 text-xs font-medium text-slate-500 mb-1">
              <span>Official Architectural Deliverable</span>
              <span aria-hidden="true">·</span>
              <span>Healthcare Information System</span>
            </div>
            <h2 className="text-lg font-bold text-slate-900 tracking-tight">
              Hospital Information System (HIS) Kafka Architectural Specification
            </h2>
            <p className="text-xs text-slate-500 mt-0.5">
              Complete formal design document with mathematical partition sizing, CloudEvents schema, and compliance invariants.
            </p>
          </div>
          <div className="flex items-center gap-2">
            <button
              onClick={handleCopyMarkdown}
              className="flex items-center gap-1.5 px-3 py-1.5 border border-slate-300 hover:bg-slate-50 text-slate-700 rounded text-xs font-medium transition-colors cursor-pointer"
            >
              {copied ? <Check className="w-3.5 h-3.5 text-emerald-600" /> : <Copy className="w-3.5 h-3.5" />}
              <span>{copied ? 'Copied' : 'Copy Raw Markdown'}</span>
            </button>
            <button
              onClick={onDownload}
              className="flex items-center gap-1.5 px-3.5 py-1.5 bg-slate-900 hover:bg-slate-800 text-white rounded text-xs font-semibold transition-colors cursor-pointer shadow-xs"
            >
              <Download className="w-3.5 h-3.5" />
              <span>Download Spec (.MD)</span>
            </button>
          </div>
        </div>

        {/* Clean Formatted Document View */}
        <div className="bg-slate-50 border border-slate-200 rounded-lg p-6 max-h-[720px] overflow-y-auto">
          <pre className="font-mono text-xs text-slate-800 whitespace-pre-wrap leading-relaxed">
            {HIS_FULL_SPEC_MARKDOWN}
          </pre>
        </div>
      </div>
    </div>
  );
};
