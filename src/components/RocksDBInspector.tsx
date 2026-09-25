import React, { useState, useEffect } from 'react';
import {
  Activity,
  ArrowDown,
  CheckCircle2,
  Database,
  Eye,
  FileCode,
  HardDrive,
  Layers,
  Radio,
  RefreshCw,
  Search,
  Server,
  Sliders,
  Terminal,
  Zap,
} from 'lucide-react';
import { streamSimulation } from '../engine/simulationEngine';
import { ChangelogRecord, WindowAggregate } from '../types/stream';

export const RocksDBInspector: React.FC = () => {
  const [changelog, setChangelog] = useState<ChangelogRecord[]>(streamSimulation.changelogRecords);
  const [aggregates, setAggregates] = useState<Map<string, WindowAggregate>>(
    new Map(streamSimulation.activeWindowAggregates)
  );
  const [searchKey, setSearchKey] = useState<string>('');
  const [selectedKey, setSelectedKey] = useState<string | null>(null);

  useEffect(() => {
    const unsubscribe = streamSimulation.subscribe(() => {
      setChangelog([...streamSimulation.changelogRecords]);
      setAggregates(new Map(streamSimulation.activeWindowAggregates));
    });
    return unsubscribe;
  }, []);

  const aggregateList: WindowAggregate[] = (Array.from(aggregates.values()) as WindowAggregate[]).filter(
    (agg) => agg.truckId.toLowerCase().includes(searchKey.toLowerCase())
  );

  const activeDetail = selectedKey
    ? aggregates.get(selectedKey) || aggregateList[0]
    : aggregateList[0];

  return (
    <div className="space-y-4">
      {/* Header Overview Bento Card */}
      <div className="bg-[#111620]/60 backdrop-blur-sm border border-slate-700/40 rounded-3xl p-6 shadow-xl relative overflow-hidden">
        <div className="flex flex-col md:flex-row md:items-center justify-between gap-4">
          <div className="flex items-center gap-3.5">
            <div className="w-10 h-10 rounded-xl bg-amber-500/20 border border-amber-500/40 flex items-center justify-center shadow-[0_0_15px_rgba(245,158,11,0.3)]">
              <Database className="w-5 h-5 text-amber-400" />
            </div>
            <div>
              <div className="flex items-center gap-2">
                <h2 className="text-base font-bold text-white tracking-tight">
                  ROCKSDB LSM-TREE STATE STORE & CHANGELOG MIRROR
                </h2>
                <span className="text-[9px] px-2 py-0.5 rounded-full bg-slate-800 text-amber-300 font-mono font-bold border border-slate-700 uppercase tracking-wider">
                  Sub-ms Local State
                </span>
              </div>
              <p className="text-[11px] text-slate-400 uppercase tracking-wider mt-0.5">
                Embedded C++ LSM-Tree engine running in-process on Python workers with write-ahead logs and changelog replication
              </p>
            </div>
          </div>

          <div className="flex items-center gap-2 text-xs font-mono">
            <div className="bg-[#05070a] px-4 py-2 rounded-2xl border border-slate-700/60 text-slate-300">
              Active Keys in State: <span className="text-amber-400 font-bold">{aggregates.size}</span>
            </div>
          </div>
        </div>
      </div>

      {/* LSM-Tree Architecture Diagram & Data Flow */}
      <div className="bg-[#111620]/60 backdrop-blur-sm border border-slate-700/40 rounded-3xl p-6 shadow-xl">
        <h3 className="text-xs font-bold uppercase tracking-widest text-slate-300 mb-4 flex items-center gap-2">
          <Layers className="w-4 h-4 text-indigo-400" />
          RocksDB In-Memory vs. Disk vs. Kafka Replication Tier
        </h3>

        <div className="grid grid-cols-1 md:grid-cols-4 gap-3">
          {/* Tier 1: Write-Ahead Log (WAL) */}
          <div className="bg-[#05070a] p-4 rounded-2xl border border-slate-700/50 relative">
            <div className="text-[10px] uppercase font-bold text-rose-400 flex items-center justify-between mb-1">
              <span>1. Write-Ahead Log</span>
              <span className="text-[9px] px-1.5 py-0.5 bg-rose-500/20 text-rose-300 rounded font-mono">Disk Append</span>
            </div>
            <div className="text-xs font-semibold text-slate-200">Sequential Append-Only</div>
            <p className="text-[11px] text-slate-400 mt-1 leading-relaxed">
              Guarantees zero data loss if power fails before MemTable flush.
            </p>
            <div className="mt-3 text-[10px] font-mono text-slate-400 bg-slate-900/60 p-2 rounded-xl border border-slate-800">
              wal_offset: <span className="text-rose-300 font-bold">#45,820</span>
            </div>
          </div>

          {/* Tier 2: MemTable (RAM) */}
          <div className="bg-[#05070a] p-4 rounded-2xl border border-indigo-500/40 relative shadow-lg shadow-indigo-950/20">
            <div className="text-[10px] uppercase font-bold text-indigo-400 flex items-center justify-between mb-1">
              <span>2. Active MemTable</span>
              <span className="text-[9px] px-1.5 py-0.5 bg-indigo-500/20 text-indigo-300 rounded font-mono">RAM Skiplist</span>
            </div>
            <div className="text-xs font-semibold text-slate-200">Concurrent Skiplist</div>
            <p className="text-[11px] text-slate-400 mt-1 leading-relaxed">
              Sub-microsecond point reads & updates for 50,000 truck rolling averages.
            </p>
            <div className="mt-3 text-[10px] font-mono text-slate-400 bg-slate-900/60 p-2 rounded-xl border border-slate-800 flex justify-between">
              <span>Size: <span className="text-indigo-300">64 MB Buffer</span></span>
              <span>Hits: <span className="text-emerald-400 font-bold">99.1%</span></span>
            </div>
          </div>

          {/* Tier 3: Immutable MemTable & SSTable (L0/L1) */}
          <div className="bg-[#05070a] p-4 rounded-2xl border border-slate-700/50 relative">
            <div className="text-[10px] uppercase font-bold text-amber-400 flex items-center justify-between mb-1">
              <span>3. SSTable Files (L0/L1)</span>
              <span className="text-[9px] px-1.5 py-0.5 bg-amber-500/20 text-amber-300 rounded font-mono">NVMe Disk</span>
            </div>
            <div className="text-xs font-semibold text-slate-200">Sorted String Tables</div>
            <p className="text-[11px] text-slate-400 mt-1 leading-relaxed">
              Background compaction merges duplicate updates into immutable blocks.
            </p>
            <div className="mt-3 text-[10px] font-mono text-slate-400 bg-slate-900/60 p-2 rounded-xl border border-slate-800">
              L0 Tables: <span className="text-amber-300 font-bold">4 files (Block Cache 256MB)</span>
            </div>
          </div>

          {/* Tier 4: Kafka Compacted Changelog */}
          <div className="bg-[#05070a] p-4 rounded-2xl border border-emerald-500/40 relative">
            <div className="text-[10px] uppercase font-bold text-emerald-400 flex items-center justify-between mb-1">
              <span>4. Kafka Changelog</span>
              <span className="text-[9px] px-1.5 py-0.5 bg-emerald-500/20 text-emerald-300 rounded font-mono">Cluster Sync</span>
            </div>
            <div className="text-xs font-semibold text-slate-200">Compacted Topic</div>
            <p className="text-[11px] text-slate-400 mt-1 leading-relaxed">
              Replayed by replacement worker during partition failover in &lt;800ms.
            </p>
            <div className="mt-3 text-[10px] font-mono text-slate-400 bg-slate-900/60 p-2 rounded-xl border border-slate-800">
              Topic: <span className="text-emerald-300">streamforge.truck_state</span>
            </div>
          </div>
        </div>
      </div>

      {/* State Inspector & Live Changelog Stream */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
        {/* Left Column: Key-Value State Explorer */}
        <div className="bg-[#111620]/60 backdrop-blur-sm border border-slate-700/40 rounded-3xl p-6 shadow-xl flex flex-col justify-between">
          <div>
            <div className="flex items-center justify-between mb-3.5 border-b border-slate-700/50 pb-3">
              <div className="flex items-center gap-2">
                <Search className="w-4 h-4 text-indigo-400" />
                <h4 className="text-xs font-bold uppercase tracking-widest text-slate-200">
                  RocksDB State Key Browser (Truck States)
                </h4>
              </div>
            </div>

            <div className="mb-3">
              <input
                type="text"
                placeholder="Search Truck ID (e.g. TRK-04936)..."
                value={searchKey}
                onChange={(e) => setSearchKey(e.target.value)}
                className="w-full bg-[#05070a] border border-slate-700/50 rounded-2xl px-4 py-2 text-xs text-slate-200 focus:outline-none focus:border-indigo-500 font-mono"
              />
            </div>

            {/* List of Keys */}
            <div className="space-y-2 max-h-56 overflow-y-auto pr-1">
              {aggregateList.slice(0, 8).map((agg) => (
                <div
                  key={agg.truckId}
                  onClick={() => setSelectedKey(agg.truckId)}
                  className={`p-3 rounded-2xl border text-xs cursor-pointer transition flex items-center justify-between font-mono ${
                    activeDetail?.truckId === agg.truckId
                      ? 'bg-indigo-600/20 border-indigo-500 text-indigo-200 shadow-[0_0_12px_rgba(99,102,241,0.2)]'
                      : 'bg-[#05070a] border-slate-700/40 text-slate-300 hover:border-slate-600'
                  }`}
                >
                  <span className="font-bold text-white">{agg.truckId}</span>
                  <div className="flex items-center gap-3">
                    <span className="text-slate-400 text-[11px]">Readings: {agg.count}</span>
                    <span
                      className={`px-2 py-0.5 rounded-full font-bold ${
                        agg.avgTemp > 0 ? 'bg-rose-500/20 text-rose-300 border border-rose-500/30' : 'bg-indigo-500/20 text-indigo-300 border border-indigo-500/30'
                      }`}
                    >
                      {agg.avgTemp}°C
                    </span>
                  </div>
                </div>
              ))}
            </div>
          </div>

          {/* Key Value Payload View */}
          {activeDetail && (
            <div className="mt-4 pt-3.5 border-t border-slate-700/50">
              <div className="text-[11px] font-bold text-slate-300 mb-2 flex items-center justify-between">
                <span>Serialized State Payload for [{activeDetail.truckId}]</span>
                <span className="font-mono text-indigo-400 text-[10px]">Key: truck:{activeDetail.truckId}:window</span>
              </div>
              <pre className="bg-[#05070a] p-3.5 rounded-2xl border border-slate-700/50 text-[11px] font-mono text-indigo-300 overflow-x-auto">
{JSON.stringify(
  {
    truck_id: activeDetail.truckId,
    window_start: new Date(activeDetail.windowStart).toLocaleTimeString(),
    window_end: new Date(activeDetail.windowEnd).toLocaleTimeString(),
    sample_count: activeDetail.count,
    sum_temperature: activeDetail.sumTemp,
    rolling_5min_avg: activeDetail.avgTemp,
    min_observed: activeDetail.minTemp,
    max_observed: activeDetail.maxTemp,
    storage_engine: "RocksDB_v8.11_Embedded_Python",
    is_replicated_to_changelog: true
  },
  null,
  2
)}
              </pre>
            </div>
          )}
        </div>

        {/* Right Column: Kafka Compacted Changelog Topic Stream */}
        <div className="bg-[#111620]/60 backdrop-blur-sm border border-slate-700/40 rounded-3xl p-6 shadow-xl flex flex-col justify-between">
          <div>
            <div className="flex items-center justify-between mb-3.5 border-b border-slate-700/50 pb-3">
              <div className="flex items-center gap-2">
                <Radio className="w-4 h-4 text-emerald-400" />
                <h4 className="text-xs font-bold uppercase tracking-widest text-slate-200">
                  Live Kafka Changelog Stream
                </h4>
              </div>
              <span className="text-[10px] font-mono text-slate-400 uppercase tracking-wider">
                Compacted Partition Log
              </span>
            </div>

            <div className="space-y-2 max-h-96 overflow-y-auto pr-1">
              {changelog.slice(0, 10).map((rec, idx) => (
                <div
                  key={idx}
                  className="bg-[#05070a] p-3 rounded-2xl border border-slate-700/50 font-mono text-xs space-y-1"
                >
                  <div className="flex items-center justify-between text-[10px]">
                    <span className="text-slate-400">
                      Offset <span className="text-indigo-300 font-bold">#{rec.offset}</span> • P{rec.partition}
                    </span>
                    <span className="px-2 py-0.5 rounded-full bg-emerald-500/20 text-emerald-300 font-bold text-[9px] border border-emerald-500/30">
                      {rec.operation}
                    </span>
                  </div>

                  <div className="flex items-center justify-between text-slate-200">
                    <span className="text-amber-300 font-semibold">{rec.key}</span>
                    <span className="text-slate-400 text-[10px]">{rec.workerSource}</span>
                  </div>

                  <div className="text-[10px] text-slate-400 flex items-center justify-between pt-1.5 border-t border-slate-800">
                    <span>Count: {rec.value.count}</span>
                    <span>Sum: {rec.value.sum}</span>
                    <span className="text-emerald-400 font-bold">Avg: {rec.value.avg}°C</span>
                  </div>
                </div>
              ))}
            </div>
          </div>

          <div className="mt-4 pt-3 border-t border-slate-700/50 text-[10px] text-slate-400 flex items-center justify-between font-mono">
            <span className="flex items-center gap-1.5 text-emerald-400">
              <CheckCircle2 className="w-3.5 h-3.5" /> Auto-Compaction Active
            </span>
            <span className="uppercase tracking-wider">Retention: Compact By Key</span>
          </div>
        </div>
      </div>
    </div>
  );
};
