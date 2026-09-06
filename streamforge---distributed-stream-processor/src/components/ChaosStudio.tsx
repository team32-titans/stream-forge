import React, { useState, useEffect } from 'react';
import {
  AlertOctagon,
  AlertTriangle,
  ArrowRight,
  CheckCircle2,
  Clock,
  Database,
  Flame,
  Layers,
  Play,
  RefreshCw,
  Server,
  ShieldAlert,
  ShieldCheck,
  Terminal,
  Trash2,
  Undo2,
  Zap,
} from 'lucide-react';
import { streamSimulation } from '../engine/simulationEngine';
import { ChaosEvent, WorkerNode } from '../types/stream';

export const ChaosStudio: React.FC = () => {
  const [workers, setWorkers] = useState<WorkerNode[]>(streamSimulation.workers);
  const [currentChaos, setCurrentChaos] = useState<ChaosEvent | null>(streamSimulation.currentChaosEvent);
  const [chaosHistory, setChaosHistory] = useState<ChaosEvent[]>(streamSimulation.chaosHistory);
  const [isKilling, setIsKilling] = useState<boolean>(false);
  const [selectedKillTarget, setSelectedKillTarget] = useState<string>('worker-04');

  useEffect(() => {
    const unsubscribe = streamSimulation.subscribe(() => {
      setWorkers([...streamSimulation.workers]);
      setCurrentChaos(streamSimulation.currentChaosEvent);
      setChaosHistory([...streamSimulation.chaosHistory]);
    });
    return unsubscribe;
  }, []);

  const handleKillWorker = async (targetId: string) => {
    setIsKilling(true);
    await streamSimulation.triggerKillWorker(targetId);
    setIsKilling(false);
  };

  const handleReviveWorker = (targetId: string) => {
    streamSimulation.reviveWorker(targetId);
  };

  const handleLateData = () => {
    streamSimulation.injectLateDataBurst();
  };

  const handleColdChainSpike = () => {
    streamSimulation.injectColdChainSpike();
  };

  const worker4 = workers.find((w) => w.id === 'worker-04');
  const worker5 = workers.find((w) => w.id === 'worker-05');

  return (
    <div className="space-y-4">
      {/* Header Banner Bento Card */}
      <div className="bg-[#111827] border border-[#1e293b] rounded-3xl p-6 shadow-xl relative overflow-hidden">
        <div className="flex flex-col lg:flex-row lg:items-center justify-between gap-4">
          <div className="flex items-center gap-3.5">
            <div className="w-10 h-10 rounded-xl bg-rose-500/20 border border-rose-500/40 flex items-center justify-center shadow-[0_0_15px_rgba(244,63,94,0.3)]">
              <ShieldAlert className="w-5 h-5 text-rose-400" />
            </div>
            <div>
              <div className="flex items-center gap-2">
                <h2 className="text-base font-bold text-white tracking-tight">
                  CHAOS ENGINEERING & FAULT TOLERANCE
                </h2>
                <span className="text-[9px] px-2 py-0.5 rounded-full bg-rose-500/20 text-rose-300 font-mono font-bold border border-rose-500/30 uppercase tracking-wider">
                  Failover Milestone
                </span>
              </div>
              <p className="text-[11px] text-slate-400 uppercase tracking-wider mt-0.5">
                Simulate worker crashes mid-calculation, trigger Kafka rebalances, and verify RocksDB state recovery with zero loss
              </p>
            </div>
          </div>

          {/* Core Action Trigger */}
          <div className="flex items-center gap-3">
            <button
              onClick={() => handleKillWorker(selectedKillTarget)}
              disabled={isKilling || workers.find((w) => w.id === selectedKillTarget)?.status === 'CRASHED'}
              className="flex items-center gap-2 px-5 py-2.5 rounded-2xl bg-rose-500 hover:bg-rose-400 text-slate-950 font-bold text-xs shadow-[0_0_20px_rgba(244,63,94,0.4)] transition disabled:opacity-50 disabled:cursor-not-allowed"
            >
              {isKilling ? (
                <>
                  <RefreshCw className="w-4 h-4 animate-spin text-slate-950" />
                  Executing Failover...
                </>
              ) : (
                <>
                  <Flame className="w-4 h-4 text-slate-950" />
                  KILL {selectedKillTarget.toUpperCase()} (MID-STREAM)
                </>
              )}
            </button>
          </div>
        </div>
      </div>

      {/* Week 3 Core Scenario: Worker #4 ➔ Worker #5 State Migration Inspector */}
      <div className="bg-[#111827] border border-[#1e293b] rounded-3xl p-6 shadow-xl">
        <div className="flex items-center justify-between border-b border-[#223348] pb-4 mb-4">
          <div className="flex items-center gap-2">
            <Database className="w-4 h-4 text-indigo-400" />
            <h3 className="text-xs font-bold uppercase tracking-widest text-white">
              Stateful Recovery: Worker #4 ➔ Worker #5 Partition Migration
            </h3>
          </div>
          <span className="text-[10px] text-slate-400 font-mono uppercase tracking-wider">
            Protocol: <span className="text-indigo-300 font-bold">Cooperative Sticky Assignor + RocksDB Changelog</span>
          </span>
        </div>

        {/* Two-Node Visual Comparison */}
        <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
          {/* Worker #4 Box */}
          <div
            className={`p-5 rounded-2xl border transition ${
              worker4?.status === 'CRASHED'
                ? 'bg-rose-950/20 border-rose-500/50 shadow-[0_0_15px_rgba(244,63,94,0.15)]'
                : 'bg-[#16202e] border-[#223348]'
            }`}
          >
            <div className="flex items-center justify-between mb-3.5">
              <div className="flex items-center gap-2">
                <Server className="w-4 h-4 text-indigo-400" />
                <span className="font-bold text-sm text-white font-mono">Worker Node #4</span>
              </div>
              <span
                className={`text-[9px] font-bold px-2 py-0.5 rounded-full font-mono uppercase tracking-wider ${
                  worker4?.status === 'CRASHED'
                    ? 'bg-rose-500/20 text-rose-300 border border-rose-500/30 animate-pulse'
                    : 'bg-emerald-500/20 text-emerald-400 border border-emerald-500/30'
                }`}
              >
                {worker4?.status}
              </span>
            </div>

            <div className="space-y-2.5 text-xs font-mono">
              <div className="flex justify-between text-slate-400">
                <span className="font-sans">Assigned Partitions:</span>
                <span className="text-indigo-300 font-bold">
                  {worker4?.assignedPartitions.length ? `[${worker4.assignedPartitions.join(', ')}]` : 'None (Revoked)'}
                </span>
              </div>
              <div className="flex justify-between text-slate-400">
                <span className="font-sans">RocksDB Active MemTable:</span>
                <span className="text-amber-300 font-bold">
                  {worker4?.status === 'CRASHED' ? 'Flushed / Orphaned' : `${worker4?.rocksDbState.memTableEntries} active keys`}
                </span>
              </div>
              <div className="flex justify-between text-slate-400">
                <span className="font-sans">Kafka Changelog Offset:</span>
                <span className="text-slate-300">
                  #{worker4?.rocksDbState.lastChangelogOffset} (Committed)
                </span>
              </div>
            </div>

            {worker4?.status === 'CRASHED' && (
              <div className="mt-4 pt-3.5 border-t border-rose-900/40 flex items-center justify-between">
                <span className="text-[11px] text-rose-300 font-mono">Worker terminated via SIGKILL</span>
                <button
                  onClick={() => handleReviveWorker('worker-04')}
                  className="flex items-center gap-1.5 px-3 py-1.5 rounded-xl bg-[#1f2d40] hover:bg-[#273852] text-white text-xs font-semibold border border-[#223348] transition"
                >
                  <Undo2 className="w-3.5 h-3.5 text-indigo-400" /> Revive Node
                </button>
              </div>
            )}
          </div>

          {/* Worker #5 Box */}
          <div
            className={`p-5 rounded-2xl border transition ${
              worker5?.status === 'RECOVERING'
                ? 'bg-amber-950/20 border-amber-500/50 shadow-[0_0_15px_rgba(245,158,11,0.15)]'
                : 'bg-[#16202e] border-[#223348]'
            }`}
          >
            <div className="flex items-center justify-between mb-3.5">
              <div className="flex items-center gap-2">
                <Server className="w-4 h-4 text-emerald-400" />
                <span className="font-bold text-sm text-white font-mono">Worker Node #5 (Receiver)</span>
              </div>
              <span
                className={`text-[9px] font-bold px-2 py-0.5 rounded-full font-mono uppercase tracking-wider ${
                  worker5?.status === 'RECOVERING'
                    ? 'bg-amber-500/20 text-amber-300 border border-amber-500/30 animate-pulse'
                    : 'bg-emerald-500/20 text-emerald-400 border border-emerald-500/30'
                }`}
              >
                {worker5?.status}
              </span>
            </div>

            <div className="space-y-2.5 text-xs font-mono">
              <div className="flex justify-between text-slate-400">
                <span className="font-sans">Assigned Partitions:</span>
                <span className="text-indigo-300 font-bold">
                  [{worker5?.assignedPartitions.join(', ')}]
                </span>
              </div>
              <div className="flex justify-between text-slate-400">
                <span className="font-sans">RocksDB Restored State:</span>
                <span className="text-emerald-300 font-bold">
                  {worker5?.rocksDbState.memTableEntries} keys restored
                </span>
              </div>
              <div className="flex justify-between text-slate-400">
                <span className="font-sans">State Integrity Check:</span>
                <span className="text-emerald-400 font-bold flex items-center gap-1">
                  <CheckCircle2 className="w-3.5 h-3.5 text-emerald-400" /> 100% Exact Rolling Avg
                </span>
              </div>
            </div>

            <div className="mt-4 pt-3.5 border-t border-[#223348] text-[11px] text-slate-400 flex items-center justify-between font-mono">
              <span className="font-sans">Changelog Replay:</span>
              <span className="text-indigo-300">Offset #0 ➔ Current (#45,240)</span>
            </div>
          </div>
        </div>
      </div>

      {/* Real-time Failover Execution Log */}
      {currentChaos && (
        <div className="bg-[#111827] border border-[#1e293b] rounded-3xl p-6 shadow-xl">
          <div className="flex items-center justify-between mb-3.5 border-b border-[#223348] pb-3">
            <div className="flex items-center gap-2">
              <Terminal className="w-4 h-4 text-indigo-400" />
              <h4 className="text-xs font-bold uppercase tracking-widest text-white">
                Live State Recovery & Failover Execution Log
              </h4>
            </div>
            <span
              className={`text-[9px] font-bold px-2 py-0.5 rounded-full font-mono uppercase tracking-wider ${
                currentChaos.status === 'RESOLVED'
                  ? 'bg-emerald-500/20 text-emerald-400 border border-emerald-500/30'
                  : 'bg-amber-500/20 text-amber-300 border border-amber-500/30 animate-pulse'
              }`}
            >
              {currentChaos.status}
            </span>
          </div>

          <div className="bg-[#0a0c10] p-4 rounded-2xl border border-[#223348] font-mono text-xs space-y-2 max-h-48 overflow-y-auto">
            {currentChaos.recoveryLog.map((line, idx) => (
              <div key={idx} className="text-slate-200 flex items-start gap-2">
                <ArrowRight className="w-3.5 h-3.5 text-indigo-400 shrink-0 mt-0.5" />
                <span>{line}</span>
              </div>
            ))}
          </div>
        </div>
      )}

      {/* Auxiliary Chaos Injections: Late Data & Fleet Spikes */}
      <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
        {/* Late Data Injection */}
        <div className="bg-[#111827] border border-[#1e293b] rounded-3xl p-6 shadow-xl flex flex-col justify-between">
          <div>
            <div className="flex items-center gap-2 mb-2">
              <Clock className="w-4 h-4 text-amber-400" />
              <h4 className="text-xs font-bold text-white uppercase tracking-wider">
                Inject Out-of-Order / Late Telemetry (4-Min Delay)
              </h4>
            </div>
            <p className="text-xs text-slate-400 leading-relaxed">
              Simulates trucks exiting cellular dead zones (mountain tunnels). Tests whether the Bounded Out-of-Order Watermark correctly absorbs or routes late data without crashing window averages.
            </p>
          </div>

          <div className="mt-5">
            <button
              onClick={handleLateData}
              className="w-full flex items-center justify-center gap-2 px-4 py-2.5 rounded-2xl bg-amber-500/15 hover:bg-amber-500/25 text-amber-300 border border-amber-500/30 text-xs font-bold transition"
            >
              <Zap className="w-3.5 h-3.5" /> Blast 6 Delayed Telemetry Packets
            </button>
          </div>
        </div>

        {/* Cold-Chain Refrigeration Spike */}
        <div className="bg-[#111827] border border-[#1e293b] rounded-3xl p-6 shadow-xl flex flex-col justify-between">
          <div>
            <div className="flex items-center gap-2 mb-2">
              <Flame className="w-4 h-4 text-rose-400" />
              <h4 className="text-xs font-bold text-white uppercase tracking-wider">
                Inject Fleet Refrigeration Compressor Failure
              </h4>
            </div>
            <p className="text-xs text-slate-400 leading-relaxed">
              Simulates sudden temperature spikes (+8.5°C) across 8 trucks. Tests the 5-minute rolling average alert trigger for perishable cargo spoilage prevention.
            </p>
          </div>

          <div className="mt-5">
            <button
              onClick={handleColdChainSpike}
              className="w-full flex items-center justify-center gap-2 px-4 py-2.5 rounded-2xl bg-rose-500/15 hover:bg-rose-500/25 text-rose-300 border border-rose-500/30 text-xs font-bold transition"
            >
              <Flame className="w-3.5 h-3.5" /> Trigger Thawing Temperature Spike
            </button>
          </div>
        </div>
      </div>

      {/* Target Worker Selector Table */}
      <div className="bg-[#111827] border border-[#1e293b] rounded-3xl p-6 shadow-xl">
        <h4 className="text-xs font-bold uppercase tracking-widest text-white mb-4 flex items-center gap-2">
          <Server className="w-4 h-4 text-indigo-400" />
          Worker Crash Target Selector (Choose any node 1–20 to kill)
        </h4>

        <div className="grid grid-cols-2 sm:grid-cols-4 md:grid-cols-5 gap-2.5">
          {workers.map((w) => (
            <button
              key={w.id}
              onClick={() => setSelectedKillTarget(w.id)}
              className={`p-3 rounded-2xl border text-xs font-mono text-left transition flex items-center justify-between ${
                selectedKillTarget === w.id
                  ? 'bg-rose-500/20 border-rose-500 text-rose-300 font-bold shadow-[0_0_12px_rgba(244,63,94,0.2)]'
                  : 'bg-[#16202e] border-[#223348] text-slate-200 hover:border-slate-500'
              }`}
            >
              <span className="font-bold">{w.id}</span>
              <span
                className={`text-[9px] px-1.5 py-0.5 rounded-full font-bold uppercase ${
                  w.status === 'HEALTHY'
                    ? 'bg-emerald-500/20 text-emerald-400 border border-emerald-500/30'
                    : 'bg-rose-500/20 text-rose-300 border border-rose-500/30'
                }`}
              >
                {w.status}
              </span>
            </button>
          ))}
        </div>
      </div>
    </div>
  );
};
