import React, { useEffect, useState } from 'react';
import {
  Activity,
  AlertTriangle,
  Award,
  BookOpen,
  Boxes,
  Brain,
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
import { streamApi, ConnectionStatus } from '../lib/api';
import { StreamMetrics } from '../types/stream';

interface NavbarProps {
  activeTab: string;
  setActiveTab: (tab: string) => void;
}

export const Navbar: React.FC<NavbarProps> = ({ activeTab, setActiveTab }) => {
  const [metrics, setMetrics] = useState<StreamMetrics>(streamSimulation.metrics);
  const [isRunning, setIsRunning] = useState<boolean>(streamSimulation.getIsRunning());
  const [rate, setRate] = useState<number>(streamSimulation.getRate());
  const [streamMode, setStreamMode] = useState<'live' | 'demo'>('live');
  const [apiLatency, setApiLatency] = useState<number>(streamApi.getLatency());
  const [connStatus, setConnStatus] = useState<ConnectionStatus>(streamSimulation.connectionStatus);

  useEffect(() => {
    const unsubSim = streamSimulation.subscribe(() => {
      setMetrics({ ...streamSimulation.metrics });
      setIsRunning(streamSimulation.getIsRunning());
      setRate(streamSimulation.getRate());
      setConnStatus(streamSimulation.connectionStatus);
      setApiLatency(streamApi.getLatency());
    });

    const unsubApi = streamApi.onStatusChange((status) => {
      setConnStatus(status);
      setApiLatency(streamApi.getLatency());
    });

    return () => {
      unsubSim();
      unsubApi();
    };
  }, []);

  const handleToggle = () => {
    const running = streamSimulation.togglePlay();
    setIsRunning(running);
  };

  const handleRateChange = (newRate: number) => {
    streamSimulation.setRate(newRate);
    setRate(newRate);
  };

  const handleModeSwitch = (mode: 'live' | 'demo') => {
    setStreamMode(mode);
    if (mode === 'demo') {
      streamSimulation.setLiveBackendEnabled(false);
      streamSimulation.setRate(100000);
      setRate(100000);
    } else {
      streamSimulation.setLiveBackendEnabled(true);
      streamSimulation.setRate(25000);
      setRate(25000);
    }
  };

  const getConnectionBadge = () => {
    switch (connStatus) {
      case 'CONNECTED_WS':
        return {
          text: 'WS CONNECTED',
          subtext: 'WebSocket /ws',
          color: 'bg-emerald-500',
          textColor: 'text-emerald-400',
        };
      case 'CONNECTED_HTTP':
        return {
          text: 'FASTAPI PROXY',
          subtext: 'HTTP /api/py',
          color: 'bg-cyan-500',
          textColor: 'text-cyan-400',
        };
      case 'CONNECTING':
        return {
          text: 'CONNECTING...',
          subtext: 'Port 3000',
          color: 'bg-amber-500',
          textColor: 'text-amber-400',
        };
      default:
        return {
          text: 'LOCAL ENGINE',
          subtext: 'Simulation Fallback',
          color: 'bg-orange-500',
          textColor: 'text-orange-400',
        };
    }
  };

  const badge = getConnectionBadge();

  return (
    <header className="sticky top-0 z-50 bg-[#0b0f17]/95 backdrop-blur-md border-b border-[#1e293b] text-slate-100 shadow-xl">
      {/* Main Header Bar */}
      <div className="max-w-7xl mx-auto px-4 py-3">
        <div className="flex flex-wrap items-center justify-between gap-4 bg-[#111827] border border-[#1e293b] rounded-2xl px-5 py-3 shadow-lg">
          {/* Brand */}
          <div className="flex items-center gap-3.5">
            <div className="w-10 h-10 bg-orange-500 rounded-xl flex items-center justify-center shadow-[0_0_15px_rgba(249,115,22,0.4)] shrink-0">
              <Zap className="w-6 h-6 text-white" />
            </div>
            <div>
              <div className="flex items-center gap-2">
                <h1 className="text-lg font-bold tracking-tight text-white leading-none font-sans">
                  STREAM FORGE
                </h1>
                <span className="text-[10px] uppercase tracking-[0.15em] px-2 py-0.5 rounded bg-[#16202e] text-orange-400 border border-[#223348] font-mono font-bold">
                  v2.4.0
                </span>
              </div>
              <p className="text-[10px] text-slate-400 uppercase tracking-[0.15em] mt-1 font-mono">
                Industrial Distributed Stateful Stream Engine
              </p>
            </div>
          </div>

          {/* Industrial Mode & Connection Pill */}
          <div className="flex items-center gap-2 bg-[#16202e] px-3.5 py-1.5 rounded-xl border border-[#223348] text-xs font-mono">
            <div className="flex items-center gap-2">
              <span className={`w-2.5 h-2.5 rounded-full ${badge.color} animate-pulse shadow-[0_0_8px_rgba(16,185,129,0.7)]`} />
              <span className={`${badge.textColor} font-bold`}>
                {streamMode === 'live' ? badge.text : 'DEMO ACCELERATED'}
              </span>
              <span className="text-slate-600">|</span>
              <span className="text-slate-400">{badge.subtext}</span>
              <span className="text-slate-600">|</span>
              <span className="text-cyan-400">{apiLatency}ms</span>
            </div>

            {/* Quick Switch Button */}
            <div className="ml-2 flex items-center bg-[#0b0f17] p-0.5 rounded-lg border border-[#223348] text-[10px]">
              <button
                onClick={() => handleModeSwitch('live')}
                className={`px-2 py-0.5 rounded transition font-bold ${
                  streamMode === 'live' ? 'bg-emerald-500 text-slate-950 shadow-sm' : 'text-slate-400 hover:text-white'
                }`}
              >
                LIVE
              </button>
              <button
                onClick={() => handleModeSwitch('demo')}
                className={`px-2 py-0.5 rounded transition font-bold ${
                  streamMode === 'demo' ? 'bg-orange-500 text-white shadow-sm' : 'text-slate-400 hover:text-white'
                }`}
              >
                DEMO
              </button>
            </div>
          </div>

          {/* Live Bento Metrics Ticker */}
          <div className="hidden xl:flex items-center gap-5 bg-[#16202e] px-4 py-1.5 rounded-xl border border-[#223348]">
            <div className="text-left">
              <p className="text-[9px] text-slate-400 uppercase font-bold tracking-wider">Health</p>
              <span className="text-emerald-400 font-mono text-xs font-bold">99.99%</span>
            </div>
            <div className="h-6 w-px bg-[#223348]" />
            <div className="text-left">
              <p className="text-[9px] text-slate-400 uppercase font-bold tracking-wider">Workers</p>
              <p className="text-white font-mono text-xs font-bold">{metrics.healthyWorkers} / 20</p>
            </div>
            <div className="h-6 w-px bg-[#223348]" />
            <div className="text-left">
              <p className="text-[9px] text-slate-400 uppercase font-bold tracking-wider">Rate</p>
              <p className="text-indigo-400 font-mono text-xs font-bold">
                {(metrics.currentThroughput / 1000).toFixed(1)}k/s
              </p>
            </div>
            <div className="h-6 w-px bg-[#223348]" />
            <div className="text-left">
              <p className="text-[9px] text-slate-400 uppercase font-bold tracking-wider">p99</p>
              <p className="text-cyan-400 font-mono text-xs font-bold">{metrics.p99LatencyMs}ms</p>
            </div>
          </div>

          {/* Engine Controls */}
          <div className="flex items-center gap-2">
            {/* Rate Selector */}
            <div className="flex items-center bg-[#16202e] p-1 rounded-xl border border-[#223348] text-xs">
              <button
                onClick={() => handleRateChange(10000)}
                className={`px-2 py-0.5 rounded text-xs font-mono transition ${
                  rate === 10000 ? 'bg-orange-500 text-white font-bold' : 'text-slate-400 hover:text-slate-200'
                }`}
              >
                10k
              </button>
              <button
                onClick={() => handleRateChange(25000)}
                className={`px-2 py-0.5 rounded text-xs font-mono transition ${
                  rate === 25000 ? 'bg-orange-500 text-white font-bold' : 'text-slate-400 hover:text-slate-200'
                }`}
              >
                25k
              </button>
              <button
                onClick={() => handleRateChange(100000)}
                className={`px-2 py-0.5 rounded text-xs font-mono transition ${
                  rate === 100000 ? 'bg-orange-500 text-white font-bold' : 'text-slate-400 hover:text-slate-200'
                }`}
              >
                ⚡ 100k
              </button>
            </div>

            {/* Play / Pause Toggle */}
            <button
              onClick={handleToggle}
              className={`flex items-center gap-1.5 px-3 py-1.5 rounded-xl text-xs font-semibold shadow-md transition ${
                isRunning
                  ? 'bg-[#16202e] hover:bg-[#1e2a3c] text-amber-300 border border-[#223348]'
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
        <div className="flex items-center overflow-x-auto gap-1.5 p-1 bg-[#111827] border border-[#1e293b] rounded-2xl scrollbar-none">
          {[
            { id: 'topology', label: 'Topology DAG', icon: Boxes },
            { id: 'chaos', label: 'Chaos & Failover', icon: ShieldAlert, highlight: true },
            { id: 'aimodel', label: 'AI Cold-Chain Model', icon: Brain, badge: 'Gemini 3.8', highlight: true },
            { id: 'windowing', label: '5-Min Windowing', icon: Layers },
            { id: 'rocksdb', label: 'RocksDB State', icon: Database },
            { id: 'fleet', label: '50k Fleet Stream', icon: Radio },
            { id: 'metrics', label: 'Prometheus Metrics', icon: Activity },
            { id: 'code', label: 'Python Engine & CLI', icon: Code2, badge: 'Python 3.10' },
            { id: 'handbook', label: 'Viva Handbook', icon: BookOpen, badge: 'Defense' },
          ].map((tab) => {
            const Icon = tab.icon;
            const isActive = activeTab === tab.id;
            return (
              <button
                key={tab.id}
                onClick={() => setActiveTab(tab.id)}
                className={`flex items-center gap-2 px-3 py-1.5 text-xs font-semibold rounded-xl transition whitespace-nowrap ${
                  isActive
                    ? 'bg-orange-500/20 text-orange-300 border border-orange-500/40 shadow-[0_0_12px_rgba(249,115,22,0.25)]'
                    : 'text-slate-400 hover:text-slate-200 hover:bg-[#16202e]'
                }`}
              >
                <Icon className={`w-3.5 h-3.5 ${isActive ? 'text-orange-400' : 'text-slate-400'}`} />
                <span>{tab.label}</span>
                {tab.badge && (
                  <span className={`text-[9px] px-1.5 py-0.2 rounded-md font-mono font-bold border ${
                    tab.badge.includes('Gemini')
                      ? 'bg-orange-500/20 text-orange-300 border-orange-500/40'
                      : 'bg-[#16202e] text-slate-300 border-[#223348]'
                  }`}>
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
