import React, { useState, useEffect } from 'react';
import { Navbar } from './components/Navbar';
import { TopologyView } from './components/TopologyView';
import { ChaosStudio } from './components/ChaosStudio';
import { AIModelLab } from './components/AIModelLab';
import { WindowingLab } from './components/WindowingLab';
import { RocksDBInspector } from './components/RocksDBInspector';
import { FleetMonitor } from './components/FleetMonitor';
import { MetricsDashboard } from './components/MetricsDashboard';
import { CodebaseExplorer } from './components/CodebaseExplorer';
import { Member1Handbook } from './components/Member1Handbook';
import { streamSimulation } from './engine/simulationEngine';

export default function App() {
  const [activeTab, setActiveTab] = useState<string>('topology');

  useEffect(() => {
    // Start distributed streaming simulation loop on mount
    streamSimulation.startSimulation();
    return () => {
      streamSimulation.stopSimulation();
    };
  }, []);

  return (
    <div className="min-h-screen bg-[#0a0c10] text-slate-100 flex flex-col antialiased selection:bg-orange-500 selection:text-white">
      {/* Top Navigation & Metrics Bar */}
      <Navbar activeTab={activeTab} setActiveTab={setActiveTab} />

      {/* Main Interactive Workspace Content */}
      <main className="flex-1 max-w-7xl w-full mx-auto px-4 py-4">
        {activeTab === 'topology' && <TopologyView />}
        {activeTab === 'chaos' && <ChaosStudio />}
        {activeTab === 'aimodel' && <AIModelLab />}
        {activeTab === 'windowing' && <WindowingLab />}
        {activeTab === 'rocksdb' && <RocksDBInspector />}
        {activeTab === 'fleet' && <FleetMonitor />}
        {activeTab === 'metrics' && <MetricsDashboard />}
        {activeTab === 'code' && <CodebaseExplorer />}
        {activeTab === 'handbook' && <Member1Handbook />}
      </main>

      {/* Bento Grid Footer */}
      <footer className="border-t border-[#1e293b] bg-[#111827] text-slate-400 text-[10px] py-4 px-4 font-mono uppercase tracking-widest">
        <div className="max-w-7xl mx-auto flex flex-col sm:flex-row items-center justify-between gap-3">
          <div className="flex items-center gap-3">
            <span className="font-bold text-white">Cluster ID: SF-PRD-EUS-01</span>
            <span className="text-slate-600">•</span>
            <span className="text-orange-400 font-semibold">Distributed Stateful Engine</span>
          </div>
          <div className="flex items-center gap-4 text-slate-400">
            <span>Uptime: 14d 02h 11m 45s</span>
            <span className="text-slate-600">•</span>
            <span className="text-slate-300">Kafka Offsets: [442,109,223 | 442,110,001]</span>
          </div>
        </div>
      </footer>
    </div>
  );
}

