import React, { useEffect, useState } from 'react';
import {
  Activity,
  AlertTriangle,
  Award,
  BookOpen,
  Boxes,
  CheckCircle2,
  Code2,
  Cpu,
  Database,
  Gauge,
  Layers,
  Pause,
  Play,
  Radio,
  RefreshCw,
  Server,
  ShieldAlert,
  Sparkles,
  Zap,
} from 'lucide-react';
import { streamSimulation } from '../engine/simulationEngine';
import { StreamMetrics } from '../types/stream';

interface NavbarProps {
  activeTab: string;
  setActiveTab: (tab: string) => void;
}

export const Navbar: React.FC<NavbarProps> = ({ activeTab, setActiveTab }) => {
  const [metrics, setMetrics] = useState<StreamMetrics>(streamSimulation.metrics);
  const [isRunning, setIsRunning] = useState<boolean>(streamSimulation.getIsRunning());
  const [rate, setRate] = useState<number>(streamSimulation.getRate());

  useEffect(() => {
    const unsubscribe = streamSimulation.subscribe(() => {
      setMetrics({ ...streamSimulation.metrics });
      setIsRunning(streamSimulation.getIsRunning());
      setRate(streamSimulation.getRate());
    });
    return unsubscribe;
  }, []);

  const handleToggle = () => {
    const running = streamSimulation.togglePlay();
    setIsRunning(running);
  };

  const handleRateChange = (newRate: number) => {
    streamSimulation.setRate(newRate);
    setRate(newRate);
  };

  return (
    <header className="sticky top-0 z-50 bg-[#05070a]/90 backdrop-blur-md border-b border-slate-800 text-slate-100 shadow-xl">
      {/* Main Header Bar */}
      <div className="max-w-7xl mx-auto px-4 py-3">
        <div className="flex flex-wrap items-center justify-between gap-4 bg-[#111620]/80 backdrop-blur-md border border-slate-700/50 rounded-2xl px-6 py-3.5 shadow-lg">
          {/* Brand */}
          <div className="flex items-center gap-4">
            <div className="w-10 h-10 bg-orange-500 rounded-xl flex items-center justify-center shadow-[0_0_15px_rgba(249,115,22,0.4)] shrink-0">
              <Zap className="w-6 h-6 text-white" />
            </div>
            <div>
              <div className="flex items-center gap-2">
                <h1 className="text-lg font-bold tracking-tight text-white leading-none">
                  STREAM FORGE
                </h1>
                <span className="text-[10px] uppercase tracking-[0.15em] px-2 py-0.5 rounded-full bg-slate-800/90 text-orange-400 border border-slate-700 font-mono font-bold">
                  v2.4.0
                </span>
              </div>
              <p className="text-[10px] text-slate-400 uppercase tracking-[0.2em] mt-1">
                Distributed Event Processor & Stateful Engine
              </p>
            </div>
          </div>

          {/* Live Bento Metrics Ticker */}
          <div className="hidden lg:flex items-center gap-6 bg-[#05070a]/60 px-5 py-2 rounded-xl border border-slate-700/40">
            <div className="text-left">
              <p className="text-[10px] text-slate-500 uppercase font-bold tracking-wider">System Health</p>
              <div className="flex items-center gap-2">
                <span className="text-green-400 font-mono text-sm font-bold">99.9%</span>
                <div className="w-2 h-2 rounded-full bg-green-500 animate-pulse shadow-[0_0_8px_rgba(16,185,129,0.6)]" />
              </div>
            </div>

            <div className="h-7 w-px bg-slate-800" />

            <div className="text-left">
              <p className="text-[10px] text-slate-500 uppercase font-bold tracking-wider">Active Workers</p>
              <p className="text-white font-mono text-sm font-bold">
                {metrics.healthyWorkers} <span className="text-slate-500 text-xs">/ 20</span>
              </p>
            </div>

            <div className="h-7 w-px bg-slate-800" />

            <div className="text-left">
              <p className="text-[10px] text-slate-500 uppercase font-bold tracking-wider">Throughput</p>
              <p className="text-indigo-400 font-mono text-sm font-bold">
                {(metrics.currentThroughput / 1000).toFixed(1)}<span className="text-xs text-indigo-300 font-normal">k/s</span>
              </p>
            </div>

            <div className="h-7 w-px bg-slate-800" />

            <div className="text-left">
              <p className="text-[10px] text-slate-500 uppercase font-bold tracking-wider">p99 Latency</p>
              <p className="text-emerald-400 font-mono text-sm font-bold">
                {metrics.p99LatencyMs} <span className="text-xs text-slate-400 font-normal">ms</span>
              </p>
            </div>
          </div>

          {/* Engine Controls */}
          <div className="flex items-center gap-2.5">
            {/* Rate Selector */}
            <div className="flex items-center bg-[#05070a]/80 p-1 rounded-xl border border-slate-700/40 text-xs">
              <button
                onClick={() => handleRateChange(10000)}
                className={`px-2.5 py-1 rounded-lg transition text-xs ${
                  rate === 10000 ? 'bg-indigo-500 text-white font-bold shadow-[0_0_10px_rgba(99,102,241,0.4)]' : 'text-slate-400 hover:text-slate-200'
                }`}
              >
                10k/s
              </button>
              <button
                onClick={() => handleRateChange(25000)}
                className={`px-2.5 py-1 rounded-lg transition text-xs ${
                  rate === 25000 ? 'bg-indigo-500 text-white font-bold shadow-[0_0_10px_rgba(99,102,241,0.4)]' : 'text-slate-400 hover:text-slate-200'
                }`}
              >
                25k/s
              </button>
              <button
                onClick={() => handleRateChange(100000)}
                className={`px-2.5 py-1 rounded-lg transition text-xs ${
                  rate === 100000 ? 'bg-orange-500 text-white font-bold shadow-[0_0_12px_rgba(249,115,22,0.5)]' : 'text-slate-400 hover:text-slate-200'
                }`}
              >
                ⚡ 100k
              </button>
            </div>

            {/* Play / Pause Toggle */}
            <button
              onClick={handleToggle}
              className={`flex items-center gap-1.5 px-3.5 py-1.5 rounded-xl text-xs font-semibold shadow-md transition ${
                isRunning
                  ? 'bg-slate-800/80 hover:bg-slate-700 text-amber-300 border border-slate-700/60'
                  : 'bg-emerald-500 hover:bg-emerald-400 text-slate-950 font-bold shadow-[0_0_12px_rgba(16,185,129,0.4)]'
              }`}
            >
              {isRunning ? (
                <>
                  <Pause className="w-3.5 h-3.5" /> Pause
                </>
              ) : (
                <>
                  <Play className="w-3.5 h-3.5" /> Run Engine
                </>
              )}
            </button>
          </div>
        </div>
      </div>

      {/* Navigation Tabs */}
      <div className="max-w-7xl mx-auto px-4 pb-2">
        <div className="flex items-center overflow-x-auto gap-1.5 p-1 bg-[#111620]/60 backdrop-blur-sm border border-slate-700/40 rounded-2xl scrollbar-none">
          {[
            { id: 'topology', label: 'Topology DAG', icon: Boxes },
            { id: 'chaos', label: 'Chaos & Failover', icon: ShieldAlert, highlight: true },
            { id: 'windowing', label: '5-Min Windowing', icon: Layers },
            { id: 'rocksdb', label: 'RocksDB State', icon: Database },
            { id: 'fleet', label: '50k Fleet Stream', icon: Radio },
            { id: 'metrics', label: 'Prometheus Metrics', icon: Activity },
            { id: 'code', label: 'Python OOP Code', icon: Code2, badge: 'PEP 8' },
            { id: 'handbook', label: 'Viva Handbook', icon: BookOpen, badge: 'Defense' },
          ].map((tab) => {
            const Icon = tab.icon;
            const isActive = activeTab === tab.id;
            return (
              <button
                key={tab.id}
                onClick={() => setActiveTab(tab.id)}
                className={`flex items-center gap-2 px-3.5 py-2 text-xs font-semibold rounded-xl transition whitespace-nowrap ${
                  isActive
                    ? 'bg-indigo-600/20 text-indigo-300 border border-indigo-500/40 shadow-[0_0_12px_rgba(99,102,241,0.25)]'
                    : 'text-slate-400 hover:text-slate-200 hover:bg-slate-800/40'
                }`}
              >
                <Icon className={`w-3.5 h-3.5 ${isActive ? 'text-indigo-400' : 'text-slate-400'}`} />
                <span>{tab.label}</span>
                {tab.badge && (
                  <span className="text-[9px] px-1.5 py-0.2 rounded-full bg-slate-800 text-orange-300 border border-slate-700 font-mono font-bold">
                    {tab.badge}
                  </span>
                )}
              </button>
            );
          })}
        </div>
      </div>
    </header>
  );
};
