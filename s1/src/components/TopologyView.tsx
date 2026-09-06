import React, { useState, useEffect } from 'react';
import {
  Activity,
  AlertTriangle,
  ArrowRight,
  Boxes,
  CheckCircle,
  Clock,
  Cpu,
  Database,
  Eye,
  Filter,
  HardDrive,
  Layers,
  Radio,
  RefreshCw,
  Server,
  ShieldAlert,
  Terminal,
  Zap,
} from 'lucide-react';
import { streamSimulation } from '../engine/simulationEngine';
import { PartitionState, WorkerNode } from '../types/stream';
import { KafkaPartitionVisualizer, computePartitionRanges } from './KafkaPartitionVisualizer';

export const TopologyView: React.FC = () => {
  const [workers, setWorkers] = useState<WorkerNode[]>(streamSimulation.workers);
  const [partitions, setPartitions] = useState<PartitionState[]>(streamSimulation.partitions);
  const [selectedWorker, setSelectedWorker] = useState<WorkerNode | null>(null);
  const [filterQuery, setFilterQuery] = useState<string>('');

  useEffect(() => {
    const unsubscribe = streamSimulation.subscribe(() => {
      setWorkers([...streamSimulation.workers]);
      setPartitions([...streamSimulation.partitions]);
      if (selectedWorker) {
        const updated = streamSimulation.workers.find((w) => w.id === selectedWorker.id);
        if (updated) setSelectedWorker(updated);
      }
    });
    return unsubscribe;
  }, [selectedWorker]);

  const healthyWorkers = workers.filter((w) => w.status === 'HEALTHY');
  const crashedWorkers = workers.filter((w) => w.status === 'CRASHED');
  const recoveringWorkers = workers.filter((w) => w.status === 'RECOVERING');

  const filteredWorkers = workers.filter((w) =>
    w.id.toLowerCase().includes(filterQuery.toLowerCase()) ||
    w.assignedPartitions.some((p) => p.toString().includes(filterQuery))
  );

  return (
    <div className="space-y-4">
      {/* Bento Section 1: Overview Header / Architecture Summary */}
      <div className="bg-[#111620]/60 backdrop-blur-sm border border-slate-700/40 rounded-3xl p-6 shadow-xl relative overflow-hidden">
        <div className="flex flex-col md:flex-row md:items-center justify-between gap-4 border-b border-slate-700/40 pb-5">
          <div className="flex items-center gap-3.5">
            <div className="w-10 h-10 rounded-xl bg-indigo-500/20 border border-indigo-500/40 flex items-center justify-center shadow-[0_0_15px_rgba(99,102,241,0.3)]">
              <Boxes className="w-5 h-5 text-indigo-400" />
            </div>
            <div>
              <h2 className="text-base font-bold text-white tracking-tight flex items-center gap-2">
                TOPOLOGY VISUALIZATION (DAG) & 20-WORKER CLUSTER
              </h2>
              <p className="text-[11px] text-slate-400 uppercase tracking-wider mt-0.5">
                Industrial Kafka partitioned pipeline with embedded RocksDB state stores and 5-min rolling window aggregations
              </p>
            </div>
          </div>

          {/* Quick cluster health pills */}
          <div className="flex flex-wrap items-center gap-2 text-xs">
            <div className="flex items-center gap-2 px-3 py-1.5 rounded-full bg-slate-800/80 border border-slate-700/60 text-emerald-400 font-mono text-[11px]">
              <div className="w-2 h-2 rounded-full bg-emerald-500 shadow-[0_0_8px_rgba(16,185,129,0.5)]" />
              <span>{healthyWorkers.length} / 20 Healthy</span>
            </div>
            {recoveringWorkers.length > 0 && (
              <div className="flex items-center gap-1.5 px-3 py-1.5 rounded-full bg-indigo-950/60 border border-indigo-500/40 text-indigo-300 font-mono text-[11px] animate-pulse">
                <RefreshCw className="w-3 h-3 animate-spin" />
                <span>{recoveringWorkers.length} Rebalancing</span>
              </div>
            )}
            {crashedWorkers.length > 0 && (
              <div className="flex items-center gap-1.5 px-3 py-1.5 rounded-full bg-rose-950/60 border border-rose-500/40 text-rose-300 font-mono text-[11px]">
                <AlertTriangle className="w-3 h-3" />
                <span>{crashedWorkers.length} Crashed</span>
              </div>
            )}
            <div className="px-3 py-1.5 rounded-full bg-slate-800/80 border border-slate-700/60 text-slate-300 font-mono text-[11px]">
              32 Partitions
            </div>
          </div>
        </div>

        {/* Visual Streaming DAG Flow Bento Blocks */}
        <div className="mt-6 grid grid-cols-1 md:grid-cols-5 gap-3.5">
          {/* Node 1: Kafka Source */}
          <div className="bg-[#05070a]/70 border border-slate-700/50 rounded-2xl p-4 relative overflow-hidden flex flex-col justify-between">
            <div className="absolute top-0 left-0 right-0 h-1 bg-gradient-to-r from-indigo-500 to-cyan-500" />
            <div>
              <div className="text-[10px] uppercase font-bold text-indigo-400 tracking-widest flex items-center gap-1.5 mb-1.5">
                <Radio className="w-3.5 h-3.5 text-indigo-400" /> 1. Kafka Ingestion
              </div>
              <div className="text-xs font-bold text-white">iot.telemetry.raw</div>
              <div className="text-[11px] text-slate-400 mt-1 font-mono">
                32 Partitions • 50k Trucks
              </div>
            </div>
            <div className="mt-3 text-[10px] text-emerald-400 bg-emerald-500/10 border border-emerald-500/20 px-2.5 py-1 rounded-xl flex items-center justify-between font-mono">
              <span>Murmur2 Hashed</span>
              <span className="font-bold">~25k msg/s</span>
            </div>
          </div>

          {/* Node 2: Filter & Deserializer */}
          <div className="bg-[#05070a]/70 border border-slate-700/50 rounded-2xl p-4 relative overflow-hidden flex flex-col justify-between">
            <div className="absolute top-0 left-0 right-0 h-1 bg-gradient-to-r from-cyan-500 to-blue-500" />
            <div>
              <div className="text-[10px] uppercase font-bold text-cyan-400 tracking-widest flex items-center gap-1.5 mb-1.5">
                <Filter className="w-3.5 h-3.5 text-cyan-400" /> 2. Filter & Schema
              </div>
              <div className="text-xs font-bold text-white">Zero-Copy Pydantic V2</div>
              <div className="text-[11px] text-slate-400 mt-1">
                Temp &gt; -50°C & Checksum
              </div>
            </div>
            <div className="mt-3 text-[10px] text-cyan-300 bg-cyan-500/10 border border-cyan-500/20 px-2.5 py-1 rounded-xl flex items-center justify-between font-mono">
              <span>Error Rate</span>
              <span className="font-bold">&lt; 0.001%</span>
            </div>
          </div>

          {/* Node 3: 20-Worker Stream Cluster (Member 1) */}
          <div className="bg-indigo-950/20 border border-indigo-500/40 rounded-2xl p-4 relative overflow-hidden flex flex-col justify-between shadow-[0_0_15px_rgba(99,102,241,0.15)]">
            <div className="absolute top-0 left-0 right-0 h-1 bg-gradient-to-r from-indigo-400 to-orange-400" />
            <div>
              <div className="text-[10px] uppercase font-bold text-indigo-300 tracking-widest flex items-center gap-1.5 mb-1.5">
                <Cpu className="w-3.5 h-3.5 text-indigo-400" /> 3. 20 Python Workers
              </div>
              <div className="text-xs font-bold text-white">Faust / Bytewax Runtime</div>
              <div className="text-[11px] text-slate-400 mt-1">
                Cooperative Sticky Group
              </div>
            </div>
            <div className="mt-3 text-[10px] text-indigo-300 bg-indigo-500/15 border border-indigo-500/30 px-2.5 py-1 rounded-xl flex items-center justify-between font-mono">
              <span>Parallelism</span>
              <span className="font-bold">{healthyWorkers.length}/20 Nodes</span>
            </div>
          </div>

          {/* Node 4: RocksDB & Window Aggregator (Member 1) */}
          <div className="bg-[#05070a]/70 border border-slate-700/50 rounded-2xl p-4 relative overflow-hidden flex flex-col justify-between">
            <div className="absolute top-0 left-0 right-0 h-1 bg-gradient-to-r from-amber-500 to-orange-500" />
            <div>
              <div className="text-[10px] uppercase font-bold text-amber-400 tracking-widest flex items-center gap-1.5 mb-1.5">
                <Database className="w-3.5 h-3.5 text-amber-400" /> 4. RocksDB & Window
              </div>
              <div className="text-xs font-bold text-white">LSM-Tree State Store</div>
              <div className="text-[11px] text-slate-400 mt-1">
                Welford O(1) 5-Min Rolling Avg
              </div>
            </div>
            <div className="mt-3 text-[10px] text-amber-300 bg-amber-500/10 border border-amber-500/20 px-2.5 py-1 rounded-xl flex items-center justify-between font-mono">
              <span>Watermark</span>
              <span className="font-bold">15s Grace</span>
            </div>
          </div>

          {/* Node 5: Output Sinks & Changelog */}
          <div className="bg-[#05070a]/70 border border-slate-700/50 rounded-2xl p-4 relative overflow-hidden flex flex-col justify-between">
            <div className="absolute top-0 left-0 right-0 h-1 bg-gradient-to-r from-emerald-500 to-teal-500" />
            <div>
              <div className="text-[10px] uppercase font-bold text-emerald-400 tracking-widest flex items-center gap-1.5 mb-1.5">
                <Zap className="w-3.5 h-3.5 text-emerald-400" /> 5. Sink & Changelog
              </div>
              <div className="text-xs font-bold text-white">fleet.telemetry.5min_avg</div>
              <div className="text-[11px] text-slate-400 mt-1">
                Compacted State Topic
              </div>
            </div>
            <div className="mt-3 text-[10px] text-emerald-400 bg-emerald-500/10 border border-emerald-500/20 px-2.5 py-1 rounded-xl flex items-center justify-between font-mono">
              <span>Delivery</span>
              <span className="font-bold">Exactly-Once</span>
            </div>
          </div>
        </div>
      </div>

      {/* Bento Section 2: Real-Time Kafka Partition Visualizer */}
      <KafkaPartitionVisualizer
        workers={workers}
        partitions={partitions}
        selectedWorker={selectedWorker}
        onSelectWorker={setSelectedWorker}
      />

      {/* Bento Section 3: 20 Python Worker Nodes Grid */}
      <div className="bg-[#111620]/60 backdrop-blur-sm border border-slate-700/40 rounded-3xl p-6 shadow-xl">
        <div className="flex flex-col sm:flex-row sm:items-center justify-between gap-3 mb-5">
          <div>
            <h3 className="text-sm font-bold text-white flex items-center gap-2">
              <Server className="w-4 h-4 text-indigo-400" />
              ACTIVE PYTHON WORKER NODES (20 PROCESSES)
            </h3>
            <p className="text-[11px] text-slate-400 uppercase tracking-wider mt-0.5">
              Each worker runs Faust/Bytewax event loops, local RocksDB store, and independent 5-min window accumulators
            </p>
          </div>

          <div className="flex items-center gap-2">
            <input
              type="text"
              placeholder="Filter worker or partition..."
              value={filterQuery}
              onChange={(e) => setFilterQuery(e.target.value)}
              className="bg-[#05070a] border border-slate-700/60 rounded-xl px-3.5 py-1.5 text-xs text-slate-200 focus:outline-none focus:border-indigo-500 w-52 font-mono"
            />
          </div>
        </div>

        {/* Worker Cards Grid */}
        <div className="grid grid-cols-1 sm:grid-cols-2 md:grid-cols-3 lg:grid-cols-4 xl:grid-cols-5 gap-3.5">
          {filteredWorkers.map((w) => {
            const isSelected = selectedWorker?.id === w.id;
            const isCrashed = w.status === 'CRASHED';
            const isRecovering = w.status === 'RECOVERING';

            let statusBorder = 'border-slate-700/40 hover:border-slate-600 bg-[#05070a]/60';
            if (isCrashed) statusBorder = 'border-rose-500/50 bg-rose-950/20 shadow-[0_0_12px_rgba(244,63,94,0.15)]';
            else if (isRecovering) statusBorder = 'border-amber-500/50 bg-amber-950/20 shadow-[0_0_12px_rgba(245,158,11,0.15)]';
            else if (isSelected) statusBorder = 'border-indigo-500 bg-[#05070a] shadow-[0_0_15px_rgba(99,102,241,0.2)]';

            return (
              <div
                key={w.id}
                onClick={() => setSelectedWorker(w)}
                className={`p-4 rounded-2xl border transition cursor-pointer flex flex-col justify-between ${statusBorder}`}
              >
                <div>
                  {/* Card Header */}
                  <div className="flex items-center justify-between mb-2.5">
                    <span className="font-mono text-xs font-bold text-slate-200 flex items-center gap-2">
                      <div
                        className={`w-2 h-2 rounded-full ${
                          isCrashed
                            ? 'bg-rose-400'
                            : isRecovering
                            ? 'bg-amber-400 animate-spin'
                            : 'bg-emerald-400 shadow-[0_0_6px_rgba(16,185,129,0.6)]'
                        }`}
                      />
                      {w.id}
                    </span>

                    <span
                      className={`text-[9px] font-bold px-2 py-0.5 rounded-full font-mono uppercase tracking-wider ${
                        isCrashed
                          ? 'bg-rose-500/20 text-rose-300 border border-rose-500/30'
                          : isRecovering
                          ? 'bg-amber-500/20 text-amber-300 border border-amber-500/30 animate-pulse'
                          : 'bg-emerald-500/20 text-emerald-400 border border-emerald-500/30'
                      }`}
                    >
                      {w.status}
                    </span>
                  </div>

                  {/* Partition Range & Partitions Badge */}
                  <div className="bg-[#05070a]/70 p-2 rounded-xl border border-slate-800/80 mb-3 space-y-1">
                    <div className="text-[10px] text-slate-400 flex items-center justify-between">
                      <span>Partition Range:</span>
                      <span className="font-mono text-indigo-300 font-bold">
                        {computePartitionRanges(w.assignedPartitions).map((r) => r.label).join(', ') || 'None (Crashed)'}
                      </span>
                    </div>
                    <div className="text-[9px] text-slate-500 font-mono truncate flex items-center justify-between">
                      <span>Partitions ({w.assignedPartitions.length}):</span>
                      <span>[{w.assignedPartitions.join(', ')}]</span>
                    </div>
                  </div>

                  {/* CPU / Rate bar */}
                  <div className="space-y-1.5 text-[11px]">
                    <div className="flex justify-between text-slate-400">
                      <span className="flex items-center gap-1">
                        <Cpu className="w-3 h-3 text-slate-500" /> CPU
                      </span>
                      <span className="font-mono text-slate-200">{w.cpuUsage}%</span>
                    </div>
                    <div className="w-full h-1.5 bg-slate-800 rounded-full overflow-hidden">
                      <div
                        className={`h-full rounded-full ${
                          w.cpuUsage > 80 ? 'bg-amber-500' : 'bg-indigo-500 shadow-[0_0_8px_rgba(99,102,241,0.6)]'
                        }`}
                        style={{ width: `${w.cpuUsage}%` }}
                      />
                    </div>
                  </div>
                </div>

                {/* RocksDB mini footer */}
                <div className="mt-3.5 pt-2.5 border-t border-slate-800/80 flex items-center justify-between text-[10px] text-slate-400">
                  <span className="flex items-center gap-1 text-amber-400/90 font-mono">
                    <Database className="w-3 h-3" /> RocksDB
                  </span>
                  <span className="font-mono text-slate-300">{w.rocksDbState.memTableEntries} keys</span>
                </div>
              </div>
            );
          })}
        </div>
      </div>

      {/* Selected Worker Detailed Modal / Drawer */}
      {selectedWorker && (
        <div className="bg-[#111620]/90 backdrop-blur-md border border-indigo-500/50 rounded-3xl p-6 shadow-2xl space-y-4">
          <div className="flex items-center justify-between border-b border-slate-700/50 pb-4">
            <div className="flex items-center gap-3.5">
              <div className="w-10 h-10 rounded-xl bg-indigo-500/20 border border-indigo-500/30 flex items-center justify-center shadow-[0_0_15px_rgba(99,102,241,0.2)]">
                <Server className="w-5 h-5 text-indigo-400" />
              </div>
              <div>
                <h4 className="text-sm font-bold text-white font-mono flex items-center gap-2">
                  Worker Node Deep-Dive: {selectedWorker.id}
                  <span
                    className={`text-xs px-2 py-0.5 rounded-full font-mono font-bold ${
                      selectedWorker.status === 'HEALTHY'
                        ? 'bg-emerald-500/20 text-emerald-400 border border-emerald-500/30'
                        : 'bg-rose-500/20 text-rose-300 border border-rose-500/30'
                    }`}
                  >
                    {selectedWorker.status}
                  </span>
                </h4>
                <p className="text-[11px] text-slate-400 mt-0.5">
                  Faust AsyncIO Event Loop • Local RocksDB instance • Murmur2 Partition Worker
                </p>
              </div>
            </div>

            <button
              onClick={() => setSelectedWorker(null)}
              className="text-xs px-4 py-1.5 rounded-xl bg-slate-800 hover:bg-slate-700 text-slate-200 font-semibold border border-slate-700 transition"
            >
              Close Details
            </button>
          </div>

          <div className="grid grid-cols-1 md:grid-cols-3 gap-4 text-xs">
            {/* Box 1: Worker Execution Stats */}
            <div className="bg-[#05070a] p-4 rounded-2xl border border-slate-700/40 space-y-2.5">
              <div className="font-bold text-indigo-300 uppercase tracking-widest text-[10px] flex items-center gap-1.5">
                <Cpu className="w-3.5 h-3.5 text-indigo-400" /> Runtime Stats
              </div>
              <div className="flex justify-between py-1 border-b border-slate-800 font-mono">
                <span className="text-slate-400 font-sans">Partition Range:</span>
                <span className="text-indigo-300 font-bold">
                  {computePartitionRanges(selectedWorker.assignedPartitions).map((r) => r.label).join(', ') || 'None (Crashed)'}
                </span>
              </div>
              <div className="flex justify-between py-1 border-b border-slate-800 font-mono">
                <span className="text-slate-400 font-sans">Assigned Partitions:</span>
                <span className="text-slate-200">
                  [{selectedWorker.assignedPartitions.join(', ')}]
                </span>
              </div>
              <div className="flex justify-between py-1 border-b border-slate-800 font-mono">
                <span className="text-slate-400 font-sans">Throughput Rate:</span>
                <span className="text-emerald-400 font-bold">
                  {selectedWorker.processingRate.toLocaleString()} evt/s
                </span>
              </div>
              <div className="flex justify-between py-1 font-mono">
                <span className="text-slate-400 font-sans">Memory RSS:</span>
                <span className="text-slate-200">{selectedWorker.memoryMb} MB</span>
              </div>
            </div>

            {/* Box 2: RocksDB Embedded State */}
            <div className="bg-[#05070a] p-4 rounded-2xl border border-slate-700/40 space-y-2.5">
              <div className="font-bold text-amber-400 uppercase tracking-widest text-[10px] flex items-center gap-1.5">
                <Database className="w-3.5 h-3.5 text-amber-400" /> RocksDB State Store
              </div>
              <div className="flex justify-between py-1 border-b border-slate-800 font-mono">
                <span className="text-slate-400 font-sans">Active MemTable:</span>
                <span className="text-amber-300 font-bold">
                  {selectedWorker.rocksDbState.memTableEntries} keys
                </span>
              </div>
              <div className="flex justify-between py-1 border-b border-slate-800 font-mono">
                <span className="text-slate-400 font-sans">SSTable Files:</span>
                <span className="text-slate-300">
                  {selectedWorker.rocksDbState.sstCount} Level-0 files
                </span>
              </div>
              <div className="flex justify-between py-1 font-mono">
                <span className="text-slate-400 font-sans">Block Cache Hit Ratio:</span>
                <span className="text-emerald-400 font-bold">
                  {(selectedWorker.rocksDbState.cacheHitRatio * 100).toFixed(2)}%
                </span>
              </div>
            </div>

            {/* Box 3: Rebalance & Recovery History */}
            <div className="bg-[#05070a] p-4 rounded-2xl border border-slate-700/40 space-y-2.5">
              <div className="font-bold text-indigo-300 uppercase tracking-widest text-[10px] flex items-center gap-1.5">
                <Terminal className="w-3.5 h-3.5 text-indigo-400" /> Rebalance History
              </div>
              <div className="space-y-1.5 max-h-28 overflow-y-auto font-mono text-[10px] pr-1">
                {selectedWorker.rebalanceHistory.map((h, i) => (
                  <div key={i} className="text-slate-400 bg-slate-900/70 p-1.5 rounded-xl border border-slate-800">
                    <span className="text-indigo-400 font-bold">[{new Date(h.timestamp).toLocaleTimeString()}]</span> {h.event}
                  </div>
                ))}
              </div>
            </div>
          </div>
        </div>
      )}
    </div>
  );
};
