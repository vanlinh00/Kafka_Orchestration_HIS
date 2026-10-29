/**
 * @license
 * SPDX-License-Identifier: Apache-2.0
 */

import React, { useState } from 'react';
import { Header } from './components/Header';
import { TopologyDiagram } from './components/TopologyDiagram';
import { TopicTaxonomySection } from './components/TopicTaxonomySection';
import { SagaOrchestratorSimulator } from './components/SagaOrchestratorSimulator';
import { ReliabilityAndDlqSection } from './components/ReliabilityAndDlqSection';
import { ComplianceAndSecuritySection } from './components/ComplianceAndSecuritySection';
import { CodeInspector } from './components/CodeInspector';
import { SpecDocumentViewer } from './components/SpecDocumentViewer';
import { HIS_FULL_SPEC_MARKDOWN } from './data/spec-document';

export default function App() {
  const [activeTab, setActiveTab] = useState<string>('topology');

  const handleExportSpec = () => {
    const blob = new Blob([HIS_FULL_SPEC_MARKDOWN], { type: 'text/markdown;charset=utf-8' });
    const url = URL.createObjectURL(blob);
    const link = document.createElement('a');
    link.href = url;
    link.download = 'HIS-Kafka-Enterprise-Backbone-Specification.md';
    document.body.appendChild(link);
    link.click();
    document.body.removeChild(link);
    URL.revokeObjectURL(url);
  };

  return (
    <div className="min-h-screen bg-slate-50 text-slate-900 flex flex-col font-sans">
      {/* 3-zone Top Bar Contract */}
      <Header
        activeTab={activeTab}
        setActiveTab={setActiveTab}
        onExportSpec={handleExportSpec}
      />

      {/* Main Content Area */}
      <main className="flex-1 max-w-7xl w-full mx-auto px-6 py-6">
        {activeTab === 'topology' && (
          <TopologyDiagram
            onNavigateToTopics={() => setActiveTab('topics')}
            onNavigateToSaga={() => setActiveTab('saga')}
          />
        )}

        {activeTab === 'topics' && <TopicTaxonomySection />}

        {activeTab === 'saga' && <SagaOrchestratorSimulator />}

        {activeTab === 'reliability' && <ReliabilityAndDlqSection />}

        {activeTab === 'compliance' && <ComplianceAndSecuritySection />}

        {activeTab === 'code' && <CodeInspector />}

        {activeTab === 'spec' && <SpecDocumentViewer onDownload={handleExportSpec} />}
      </main>

      {/* Subtle, Clean Enterprise Footer */}
      <footer className="border-t border-slate-200 bg-white py-4 px-6 text-xs text-slate-500">
        <div className="max-w-7xl mx-auto flex flex-col sm:flex-row items-center justify-between gap-3">
          <div className="flex items-center gap-2">
            <span className="font-semibold text-slate-800">Hospital Information System (HIS)</span>
            <span aria-hidden="true">·</span>
            <span>Apache Kafka Distributed Architecture Standard</span>
            <span aria-hidden="true">·</span>
            <span>Java 17 / Spring Boot 3.4 / PostgreSQL</span>
          </div>
          <div>
            <span>Target Compliance: HIPAA Security Rule § 164.312 · GDPR Article 17 · HL7 FHIR R4</span>
          </div>
        </div>
      </footer>
    </div>
  );
}
