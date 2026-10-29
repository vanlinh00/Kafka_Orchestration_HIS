import React from 'react';
import { Download, FileText } from 'lucide-react';

interface HeaderProps {
  activeTab: string;
  setActiveTab: (tab: string) => void;
  onExportSpec: () => void;
}

export const Header: React.FC<HeaderProps> = ({ activeTab, setActiveTab, onExportSpec }) => {
  const navItems = [
    { id: 'topology', label: 'Topology' },
    { id: 'topics', label: 'Topic Taxonomy' },
    { id: 'saga', label: 'Saga Simulator' },
    { id: 'reliability', label: 'Delivery & DLQ' },
    { id: 'compliance', label: 'HIPAA & Crypto' },
    { id: 'code', label: 'Java 17 Code' },
    { id: 'spec', label: 'Full Specification' }
  ];

  return (
    <header className="sticky top-0 z-40 bg-white/95 backdrop-blur-md border-b border-slate-200">
      <div className="max-w-7xl mx-auto flex items-center justify-between gap-8 px-6 py-3.5">
        {/* Zone 1: Single text element wordmark */}
        <div className="flex items-center gap-2.5 shrink-0">
          <div className="w-8 h-8 rounded bg-slate-900 flex items-center justify-center text-white font-bold text-sm tracking-wider">
            HIS
          </div>
          <span className="text-base font-bold tracking-tight text-slate-900 whitespace-nowrap shrink-0">
            Kafka Enterprise Backbone
          </span>
        </div>

        {/* Zone 2: 4-7 clean single-line text navigation links */}
        <nav className="hidden lg:flex items-center gap-6 text-sm font-medium text-slate-600">
          {navItems.map((item) => (
            <button
              key={item.id}
              onClick={() => setActiveTab(item.id)}
              className={`transition-colors whitespace-nowrap shrink-0 pb-0.5 border-b-2 cursor-pointer ${
                activeTab === item.id
                  ? 'text-slate-900 border-slate-900 font-semibold'
                  : 'text-slate-500 border-transparent hover:text-slate-900'
              }`}
            >
              {item.label}
            </button>
          ))}
        </nav>

        {/* Zone 3: 1 primary action */}
        <div className="flex items-center gap-3 shrink-0">
          <button
            onClick={onExportSpec}
            className="flex items-center gap-2 px-3.5 py-1.5 text-xs font-semibold text-white bg-slate-900 rounded-md hover:bg-slate-800 transition-colors whitespace-nowrap shrink-0 cursor-pointer shadow-xs"
          >
            <Download className="w-3.5 h-3.5" />
            <span>Export Spec (.MD)</span>
          </button>
        </div>
      </div>
    </header>
  );
};
