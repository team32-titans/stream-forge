import React, { useState, useEffect } from 'react';
import { streamSimulation } from './engine/simulationEngine';
import { IS_DEMO } from './lib/api';
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

const BOOT_TIME = Date.now();

function formatUptime(): string {
  const s = Math.floor((Date.now() - BOOT_TIME) / 1000);
  const d = Math.floor(s / 86400);
  const h = Math.floor((s % 86400) / 3600);
  const m = Math.floor((s % 3600) / 60);
  const sec = s % 60;
  return `${d}d ${String(h).padStart(2, '0')}h ${String(m).padStart(2, '0')}m ${String(sec).padStart(2, '0')}s`;
}

export default function App() {
  const [activeTab, setActiveTab] = useState<string>('topology');
  const [, forceTick] = useState(0);

  useEffect(() => {
    // Only start the simulation tick loop in DEMO mode.
    // In LIVE mode the streamSimulation object still exists (components read
    // from it as a shared state bus) but the local tick loop does NOT run —
    // data comes exclusively from the backend via WebSocket / REST.
    if (IS_DEMO) {
      streamSimulation.startSimulation();
    }
    const t = window.setInterval(() => forceTick((x) => x + 1), 1000);
    return () => {
      window.clearInterval(t);
      if (IS_DEMO) {
        streamSimulation.stopSimulation();
      }
    };
  }, []);

  const modeLabel = IS_DEMO ? 'demo' : 'live';

  return (
    <div className="app-shell min-h-screen bg-[#0a0c10] text-slate-100 flex flex-col antialiased selection:bg-orange-500 selection:text-white">
      <div className="app-glow app-glow-1" />
      <div className="app-glow app-glow-2" />
      <div className="app-grid" />

      {/* DEMO mode banner */}
      {IS_DEMO && (
        <div className="bg-yellow-500/90 text-black text-xs font-bold text-center py-1.5 relative z-50 uppercase tracking-wider">
          ⚠ DEMO MODE — Data is simulated via simulationEngine.ts, not from a live Kafka cluster
        </div>
      )}

      {/* Top Navigation & Metrics Bar */}
      <Navbar activeTab={activeTab} setActiveTab={setActiveTab} />

      {/* Main Interactive Workspace Content */}
      <main className="app-main flex-1 max-w-7xl w-full mx-auto px-4 py-4 relative z-10">
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
      <footer className="panel-surface border-t border-[#1e293b] bg-[#111827]/90 text-slate-400 text-[10px] py-4 px-4 font-mono uppercase tracking-widest relative z-10">
        <div className="max-w-7xl mx-auto flex flex-col sm:flex-row items-center justify-between gap-3">
          <div className="flex items-center gap-3">
            <span className="font-bold text-white">Cluster ID: SF-PRD-EUS-01</span>
            <span className="text-slate-600">•</span>
            <span className="text-orange-400 font-semibold">Distributed Stateful Engine</span>
          </div>
          <div className="flex items-center gap-4 text-slate-400">
            <span>Uptime: {formatUptime()} ({modeLabel} session)</span>
            <span className="text-slate-600">•</span>
            <span className="text-slate-300">
              Events: {streamSimulation.metrics.totalEventsProcessed.toLocaleString()}
            </span>
          </div>
        </div>
      </footer>
    </div>
  );
}
