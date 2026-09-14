import React, { useState, useMemo } from 'react';
import {
  Activity,
  AlertTriangle,
  ArrowRight,
  BarChart2,
  CheckCircle2,
  Cpu,
  Database,
  ExternalLink,
  Eye,
  Filter,
  Flame,
  GitBranch,
  HardDrive,
  Hash,
  Layers,
  Radio,
  RefreshCw,
  Search,
  Server,
  Share2,
  ShieldAlert,
  Zap,
} from 'lucide-react';
import { PartitionState, WorkerNode } from '../types/stream';
import { streamSimulation } from '../engine/simulationEngine';

export interface PartitionRangeInfo {
  start: number;
  end: number;
  count: number;
  partitions: number[];
  label: string;
}

export function computePartitionRanges(partitions: number[]): PartitionRangeInfo[] {
  if (!partitions || partitions.length === 0) return [];
  const sorted = [...partitions].sort((a, b) => a - b);
  const ranges: PartitionRangeInfo[] = [];
  let start = sorted[0];
  let prev = sorted[0];
  let currentGroup = [sorted[0]];

  for (let i = 1; i < sorted.length; i++) {
    const curr = sorted[i];
    if (curr === prev + 1) {
      currentGroup.push(curr);
      prev = curr;
    } else {
      ranges.push({
        start,
        end: prev,
        count: currentGroup.length,
        partitions: [...currentGroup],
        label:
          start === prev
            ? `P${start.toString().padStart(2, '0')}`
            : `P${start.toString().padStart(2, '0')}–P${prev.toString().padStart(2, '0')}`,
      });
      start = curr;
      prev = curr;
      currentGroup = [curr];
    }
  }

  ranges.push({
    start,
    end: prev,
    count: currentGroup.length,
    partitions: [...currentGroup],
    label:
      start === prev
        ? `P${start.toString().padStart(2, '0')}`
        : `P${start.toString().padStart(2, '0')}–P${prev.toString().padStart(2, '0')}`,
  });

  return ranges;
}

const WORKER_PALETTES = [
  {
    bg: 'bg-indigo-500/15',
    text: 'text-indigo-400',
    border: 'border-indigo-500/40',
    badge: 'bg-indigo-500/20 text-indigo-300 border-indigo-500/40',
    bar: 'bg-indigo-500',
    dot: 'bg-indigo-400',
  },
  {
    bg: 'bg-cyan-500/15',
    text: 'text-cyan-400',
    border: 'border-cyan-500/40',
    badge: 'bg-cyan-500/20 text-cyan-300 border-cyan-500/40',
    bar: 'bg-cyan-500',
    dot: 'bg-cyan-400',
  },
  {
    bg: 'bg-emerald-500/15',
    text: 'text-emerald-400',
    border: 'border-emerald-500/40',
    badge: 'bg-emerald-500/20 text-emerald-300 border-emerald-500/40',
    bar: 'bg-emerald-500',
    dot: 'bg-emerald-400',
  },
  {
    bg: 'bg-amber-500/15',
    text: 'text-amber-400',
    border: 'border-amber-500/40',
    badge: 'bg-amber-500/20 text-amber-300 border-amber-500/40',
    bar: 'bg-amber-500',
    dot: 'bg-amber-400',
  },
  {
    bg: 'bg-purple-500/15',
    text: 'text-purple-400',
    border: 'border-purple-500/40',
    badge: 'bg-purple-500/20 text-purple-300 border-purple-500/40',
    bar: 'bg-purple-500',
    dot: 'bg-purple-400',
  },
  {
    bg: 'bg-pink-500/15',
    text: 'text-pink-400',
    border: 'border-pink-500/40',
    badge: 'bg-pink-500/20 text-pink-300 border-pink-500/40',
    bar: 'bg-pink-500',
    dot: 'bg-pink-400',
  },
  {
    bg: 'bg-teal-500/15',
    text: 'text-teal-400',
    border: 'border-teal-500/40',
    badge: 'bg-teal-500/20 text-teal-300 border-teal-500/40',
    bar: 'bg-teal-500',
    dot: 'bg-teal-400',
  },
  {
    bg: 'bg-blue-500/15',
    text: 'text-blue-400',
    border: 'border-blue-500/40',
    badge: 'bg-blue-500/20 text-blue-300 border-blue-500/40',
    bar: 'bg-blue-500',
    dot: 'bg-blue-400',
  },
  {
    bg: 'bg-orange-500/15',
    text: 'text-orange-400',
    border: 'border-orange-500/40',
    badge: 'bg-orange-500/20 text-orange-300 border-orange-500/40',
    bar: 'bg-orange-500',
    dot: 'bg-orange-400',
  },
  {
    bg: 'bg-lime-500/15',
    text: 'text-lime-400',
    border: 'border-lime-500/40',
    badge: 'bg-lime-500/20 text-lime-300 border-lime-500/40',
    bar: 'bg-lime-500',
    dot: 'bg-lime-400',
  },
];

export function getWorkerPalette(workerId: string | null | undefined) {
  if (!workerId) {
    return {
      bg: 'bg-slate-800/60',
      text: 'text-slate-400',
      border: 'border-slate-700',
      badge: 'bg-slate-800 text-slate-400 border-slate-700',
      bar: 'bg-slate-600',
      dot: 'bg-slate-500',
    };
  }
  const match = workerId.match(/\d+/);
  const num = match ? parseInt(match[0], 10) : 0;
  return WORKER_PALETTES[(num - 1 + WORKER_PALETTES.length) % WORKER_PALETTES.length];
}

interface KafkaPartitionVisualizerProps {
  workers: WorkerNode[];
  partitions: PartitionState[];
  selectedWorker: WorkerNode | null;
  onSelectWorker: (worker: WorkerNode | null) => void;
}

export const KafkaPartitionVisualizer: React.FC<KafkaPartitionVisualizerProps> = ({
  workers,
  partitions,
  selectedWorker,
  onSelectWorker,
}) => {
  const [selectedPartitionId, setSelectedPartitionId] = useState<number | null>(0);
  const [viewMode, setViewMode] = useState<'spectrum' | 'matrix' | 'flow'>('spectrum');
  const [searchQuery, setSearchQuery] = useState<string>('');
  const [hoveredPartition, setHoveredPartition] = useState<number | null>(null);
  const [isRebalancing, setIsRebalancing] = useState<boolean>(false);

  // Compute worker to ranges map
  const workerRanges = useMemo(() => {
    const map = new Map<string, { worker: WorkerNode; ranges: PartitionRangeInfo[]; formatted: string }>();
    workers.forEach((w) => {
      const ranges = computePartitionRanges(w.assignedPartitions);
      const formatted =
        ranges.length > 0 ? ranges.map((r) => r.label).join(', ') : 'Unassigned (Standby / Down)';
      map.set(w.id, { worker: w, ranges, formatted });
    });
    return map;
  }, [workers]);

  // Compute contiguous spectrum range blocks across the 32 partitions
  const spectrumBlocks = useMemo(() => {
    if (partitions.length === 0) return [];
    const sorted = [...partitions].sort((a, b) => a.partitionId - b.partitionId);
    const blocks: {
      workerId: string | null;
      start: number;
      end: number;
      partitions: PartitionState[];
      label: string;
    }[] = [];

    let currentWorker = sorted[0].assignedWorker;
    let blockStart = sorted[0].partitionId;
    let currentPartitions = [sorted[0]];

    for (let i = 1; i < sorted.length; i++) {
      const p = sorted[i];
      if (p.assignedWorker === currentWorker) {
        currentPartitions.push(p);
      } else {
        const prev = sorted[i - 1].partitionId;
        blocks.push({
          workerId: currentWorker,
          start: blockStart,
          end: prev,
          partitions: [...currentPartitions],
          label:
            blockStart === prev
              ? `P${blockStart.toString().padStart(2, '0')}`
              : `P${blockStart.toString().padStart(2, '0')}–P${prev.toString().padStart(2, '0')}`,
        });
        currentWorker = p.assignedWorker;
        blockStart = p.partitionId;
        currentPartitions = [p];
      }
    }

    const lastPrev = sorted[sorted.length - 1].partitionId;
    blocks.push({
      workerId: currentWorker,
      start: blockStart,
      end: lastPrev,
      partitions: [...currentPartitions],
      label:
        blockStart === lastPrev
          ? `P${blockStart.toString().padStart(2, '0')}`
          : `P${blockStart.toString().padStart(2, '0')}–P${lastPrev.toString().padStart(2, '0')}`,
    });

    return blocks;
  }, [partitions]);

  // Selected partition details
  const activePartition = useMemo(() => {
    if (selectedPartitionId === null) return null;
    return partitions.find((p) => p.partitionId === selectedPartitionId) || null;
  }, [partitions, selectedPartitionId]);

  // Responsible worker for active partition
  const activePartitionWorker = useMemo(() => {
    if (!activePartition || !activePartition.assignedWorker) return null;
    return workers.find((w) => w.id === activePartition.assignedWorker) || null;
  }, [activePartition, workers]);

  // Handle rebalance trigger
  const handleTriggerRebalance = () => {
    setIsRebalancing(true);
    streamSimulation.rebalancePartitions();
    setTimeout(() => setIsRebalancing(false), 800);
  };

  // Quick simulate worker failover from visualizer
  const handleSimulateKill = async (workerId: string) => {
    await streamSimulation.triggerKillWorker(workerId);
  };

  // Filtered workers for matrix view
  const filteredWorkersList = useMemo(() => {
    if (!searchQuery.trim()) return workers;
    const q = searchQuery.toLowerCase();
    return workers.filter((w) => {
      const rangeData = workerRanges.get(w.id);
      const matchesRange = rangeData ? rangeData.formatted.toLowerCase().includes(q) : false;
      const matchesId = w.id.toLowerCase().includes(q);
      const matchesPartition = w.assignedPartitions.some((p) => p.toString() === q || `p${p}` === q);
      return matchesId || matchesRange || matchesPartition;
    });
  }, [workers, searchQuery, workerRanges]);

  // Aggregates
  const totalLag = partitions.reduce((acc, p) => acc + p.lag, 0);
  const totalThroughput = partitions.reduce((acc, p) => acc + p.throughput, 0);
  const unassignedCount = partitions.filter((p) => !p.assignedWorker).length;

  return (
    <div className="bg-[#111620]/60 backdrop-blur-sm border border-slate-700/40 rounded-3xl p-6 shadow-xl space-y-6">
      {/* 1. Header with Title & KPI Bar */}
      <div className="flex flex-col lg:flex-row lg:items-center justify-between gap-4 border-b border-slate-700/40 pb-5">
        <div className="flex items-center gap-3.5">
          <div className="w-10 h-10 rounded-xl bg-indigo-500/20 border border-indigo-500/40 flex items-center justify-center shadow-[0_0_15px_rgba(99,102,241,0.3)]">
            <Layers className="w-5 h-5 text-indigo-400" />
          </div>
          <div>
            <div className="flex items-center gap-2 flex-wrap">
              <h3 className="text-base font-bold text-white tracking-tight uppercase">
                Real-Time Kafka Partition Range Visualizer
              </h3>
              <span className="text-[9px] px-2 py-0.5 rounded-full bg-indigo-500/20 text-indigo-300 font-mono font-bold border border-indigo-500/30 uppercase tracking-wider">
                Topic: fleet-telemetry • 32 Partitions
              </span>
              <span className="text-[9px] px-2 py-0.5 rounded-full bg-emerald-500/20 text-emerald-300 font-mono font-bold border border-emerald-500/30 uppercase tracking-wider">
                Sticky Cooperative Assignment
              </span>
            </div>
            <p className="text-[11px] text-slate-400 uppercase tracking-wider mt-0.5">
              Live mapping of 32 topic partitions to consumer worker nodes with contiguous range detection & failover monitoring
            </p>
          </div>
        </div>

        {/* View Mode Switcher and Actions */}
        <div className="flex items-center gap-2.5 flex-wrap">
          <div className="flex items-center bg-[#05070a] border border-slate-700/60 rounded-2xl p-1 text-xs font-mono">
            <button
              onClick={() => setViewMode('spectrum')}
              className={`flex items-center gap-1.5 px-3 py-1.5 rounded-xl font-bold transition cursor-pointer ${
                viewMode === 'spectrum' ? 'bg-indigo-600 text-white shadow' : 'text-slate-400 hover:text-white'
              }`}
            >
              <BarChart2 className="w-3.5 h-3.5" /> Spectrum Bar & Grid
            </button>
            <button
              onClick={() => setViewMode('matrix')}
              className={`flex items-center gap-1.5 px-3 py-1.5 rounded-xl font-bold transition cursor-pointer ${
                viewMode === 'matrix' ? 'bg-indigo-600 text-white shadow' : 'text-slate-400 hover:text-white'
              }`}
            >
              <Server className="w-3.5 h-3.5" /> Worker Range Matrix
            </button>
            <button
              onClick={() => setViewMode('flow')}
              className={`flex items-center gap-1.5 px-3 py-1.5 rounded-xl font-bold transition cursor-pointer ${
                viewMode === 'flow' ? 'bg-indigo-600 text-white shadow' : 'text-slate-400 hover:text-white'
              }`}
            >
              <GitBranch className="w-3.5 h-3.5" /> Range Topology Flow
            </button>
          </div>

          <button
            onClick={handleTriggerRebalance}
            disabled={isRebalancing}
            className="flex items-center gap-1.5 px-3.5 py-2 rounded-2xl bg-indigo-600 hover:bg-indigo-500 disabled:opacity-50 text-white text-xs font-bold transition shadow shadow-indigo-900/30 cursor-pointer uppercase tracking-wider"
          >
            <RefreshCw className={`w-3.5 h-3.5 ${isRebalancing ? 'animate-spin' : ''}`} />
            {isRebalancing ? 'Rebalancing...' : 'Trigger Rebalance'}
          </button>
        </div>
      </div>

      {/* 2. Top-Level Metric Badges (Bento Row) */}
      <div className="grid grid-cols-2 sm:grid-cols-4 gap-3 text-xs font-mono">
        <div className="bg-[#05070a]/70 p-3.5 rounded-2xl border border-slate-700/40">
          <div className="text-[10px] text-slate-400 uppercase tracking-widest flex items-center gap-1.5">
            <Hash className="w-3.5 h-3.5 text-indigo-400" /> Total Partitions
          </div>
          <div className="text-lg font-bold text-white mt-1">32 Partitions</div>
          <div className="text-[10px] text-emerald-400 mt-0.5 flex items-center gap-1">
            <CheckCircle2 className="w-3 h-3" />
            {unassignedCount === 0 ? '100% Assigned to Workers' : `${unassignedCount} Orphaned`}
          </div>
        </div>

        <div className="bg-[#05070a]/70 p-3.5 rounded-2xl border border-slate-700/40">
          <div className="text-[10px] text-slate-400 uppercase tracking-widest flex items-center gap-1.5">
            <Server className="w-3.5 h-3.5 text-cyan-400" /> Assigned Worker Nodes
          </div>
          <div className="text-lg font-bold text-white mt-1">
            {workers.filter((w) => w.assignedPartitions.length > 0).length} / {workers.length} Nodes
          </div>
          <div className="text-[10px] text-cyan-300 mt-0.5">
            Avg {(32 / Math.max(1, workers.filter((w) => w.status === 'HEALTHY').length)).toFixed(1)} Partitions / Worker
          </div>
        </div>

        <div className="bg-[#05070a]/70 p-3.5 rounded-2xl border border-slate-700/40">
          <div className="text-[10px] text-slate-400 uppercase tracking-widest flex items-center gap-1.5">
            <Zap className="w-3.5 h-3.5 text-amber-400" /> Ingestion Rate
          </div>
          <div className="text-lg font-bold text-amber-400 mt-1">
            {totalThroughput.toLocaleString()} msgs/s
          </div>
          <div className="text-[10px] text-slate-400 mt-0.5">
            Murmur2(truck_id) % 32
          </div>
        </div>

        <div className="bg-[#05070a]/70 p-3.5 rounded-2xl border border-slate-700/40">
          <div className="text-[10px] text-slate-400 uppercase tracking-widest flex items-center gap-1.5">
            <Activity className="w-3.5 h-3.5 text-emerald-400" /> Total Consumer Lag
          </div>
          <div className="text-lg font-bold text-emerald-400 mt-1">{totalLag} msgs</div>
          <div className="text-[10px] text-emerald-300 mt-0.5 flex items-center gap-1">
            <div className="w-1.5 h-1.5 rounded-full bg-emerald-500 animate-pulse" />
            Healthy &lt; 500 msgs threshold
          </div>
        </div>
      </div>

      {/* 3. CONTINUOUS REAL-TIME PARTITION SPECTRUM RIBBON */}
      <div className="bg-[#05070a]/90 border border-slate-700/60 rounded-2xl p-4.5 space-y-3 shadow-inner">
        <div className="flex flex-col sm:flex-row sm:items-center justify-between gap-2">
          <div className="flex items-center gap-2">
            <div className="w-2.5 h-2.5 rounded-full bg-indigo-500 animate-ping" />
            <span className="text-xs font-bold text-white uppercase tracking-wider font-mono">
              Live Partition Range Spectrum Ribbon (Partitions 0 to 31)
            </span>
          </div>
          <div className="flex items-center gap-3 text-[11px] text-slate-400 font-mono">
            <span>Hover or click partition for range telemetry</span>
            <span className="text-slate-600">•</span>
            <span className="text-indigo-400">Brackets = Contiguous Worker Range</span>
          </div>
        </div>

        {/* 32-Tile Spectrum Strip */}
        <div className="grid grid-cols-8 sm:grid-cols-16 lg:grid-cols-32 gap-1">
          {partitions.map((p) => {
            const isSelected = selectedPartitionId === p.partitionId;
            const isHovered = hoveredPartition === p.partitionId;
            const assigned = workers.find((w) => w.id === p.assignedWorker);
            const isCrashed = assigned?.status === 'CRASHED';
            const isRecovering = assigned?.status === 'RECOVERING';
            const palette = getWorkerPalette(p.assignedWorker);

            let stateBorder = palette.border;
            let stateBg = palette.bg;
            if (isCrashed) {
              stateBorder = 'border-rose-500';
              stateBg = 'bg-rose-950/40 text-rose-300';
            } else if (isRecovering) {
              stateBorder = 'border-amber-500 animate-pulse';
              stateBg = 'bg-amber-950/40 text-amber-300';
            }

            if (isSelected) {
              stateBorder = 'border-white ring-2 ring-indigo-500 shadow-[0_0_12px_rgba(99,102,241,0.6)] scale-105 z-10';
            }

            return (
              <button
                key={p.partitionId}
                onClick={() => {
                  setSelectedPartitionId(p.partitionId);
                  if (assigned) onSelectWorker(assigned);
                }}
                onMouseEnter={() => setHoveredPartition(p.partitionId)}
                onMouseLeave={() => setHoveredPartition(null)}
                className={`relative p-2 rounded-xl border text-center transition-all cursor-pointer font-mono select-none flex flex-col items-center justify-between min-h-[64px] ${stateBg} ${stateBorder}`}
                title={`Partition P${p.partitionId.toString().padStart(2, '0')} -> Responsible: ${
                  p.assignedWorker || 'Unassigned'
                } | Lag: ${p.lag} | Rate: ${p.throughput} msg/s`}
              >
                <div className="text-[11px] font-bold text-slate-100">
                  P{p.partitionId.toString().padStart(2, '0')}
                </div>

                <div
                  className={`text-[8px] font-bold px-1 rounded truncate w-full ${
                    isCrashed ? 'text-rose-400 bg-rose-500/20' : palette.text
                  }`}
                >
                  {p.assignedWorker ? p.assignedWorker.replace('worker-', 'W') : 'ORPHAN'}
                </div>

                {/* Lag mini bar */}
                <div className="w-full h-1 bg-slate-900 rounded-full overflow-hidden mt-1">
                  <div
                    className={`h-full ${p.lag > 25 ? 'bg-rose-500' : p.lag > 10 ? 'bg-amber-500' : 'bg-emerald-500'}`}
                    style={{ width: `${Math.min(100, Math.max(15, p.lag * 4))}%` }}
                  />
                </div>
              </button>
            );
          })}
        </div>

        {/* Visual Range Brackets grouping continuous assignments */}
        <div className="pt-2 border-t border-slate-800/80">
          <div className="text-[10px] text-slate-400 uppercase tracking-widest font-mono mb-2 flex items-center justify-between">
            <span>Detected Worker Responsible Partition Ranges:</span>
            <span className="text-indigo-400 font-bold">{spectrumBlocks.length} Active Range Segments</span>
          </div>

          <div className="flex flex-wrap gap-2">
            {spectrumBlocks.map((block, idx) => {
              const assignedWorker = workers.find((w) => w.id === block.workerId);
              const isCrashed = assignedWorker?.status === 'CRASHED';
              const palette = getWorkerPalette(block.workerId);
              const isBlockActive =
                selectedPartitionId !== null &&
                selectedPartitionId >= block.start &&
                selectedPartitionId <= block.end;

              return (
                <div
                  key={idx}
                  onClick={() => {
                    setSelectedPartitionId(block.start);
                    if (assignedWorker) onSelectWorker(assignedWorker);
                  }}
                  className={`flex items-center gap-2 px-3 py-1.5 rounded-xl border text-xs font-mono cursor-pointer transition ${
                    isBlockActive
                      ? 'border-indigo-400 bg-indigo-950/60 shadow-md ring-1 ring-indigo-500'
                      : `${palette.border} ${palette.bg} hover:border-slate-500`
                  }`}
                >
                  <span className={`w-2 h-2 rounded-full ${isCrashed ? 'bg-rose-500' : palette.dot}`} />
                  <span className="font-bold text-white">{block.label}</span>
                  <ArrowRight className="w-3 h-3 text-slate-500" />
                  <span className={`font-bold ${isCrashed ? 'text-rose-400' : palette.text}`}>
                    {block.workerId || 'UNASSIGNED'}
                  </span>
                  <span className="text-[9px] px-1.5 py-0.2 rounded bg-slate-900/80 text-slate-400 border border-slate-800">
                    {block.partitions.length} {block.partitions.length === 1 ? 'part' : 'parts'}
                  </span>
                </div>
              );
            })}
          </div>
        </div>
      </div>

      {/* 4. MAIN INTERACTIVE VIEWS: SPECTRUM GRID, WORKER RANGE MATRIX, OR TOPOLOGY FLOW */}
      {viewMode === 'spectrum' && (
        <div className="grid grid-cols-1 lg:grid-cols-12 gap-5">
          {/* Left: 32 Partitions Interactive Cards (8 Cols) */}
          <div className="lg:col-span-8 bg-[#05070a]/70 border border-slate-700/40 rounded-2xl p-4.5 space-y-3.5">
            <div className="flex flex-col sm:flex-row sm:items-center justify-between gap-2 border-b border-slate-800/80 pb-3">
              <div>
                <h4 className="text-xs font-bold text-white uppercase tracking-wider font-mono flex items-center gap-2">
                  <Hash className="w-3.5 h-3.5 text-indigo-400" />
                  Detailed Partition Grid & Responsible Worker Mapping
                </h4>
                <p className="text-[11px] text-slate-400 mt-0.5">
                  Click any partition to view real-time log end offsets, consumer lag, and assigned range context.
                </p>
              </div>

              <div className="relative">
                <Search className="w-3.5 h-3.5 text-slate-500 absolute left-3 top-2.5" />
                <input
                  type="text"
                  placeholder="Filter partition (e.g. P04, W02)..."
                  value={searchQuery}
                  onChange={(e) => setSearchQuery(e.target.value)}
                  className="bg-[#111620] border border-slate-700/60 rounded-xl pl-8 pr-3 py-1 text-xs text-slate-200 placeholder:text-slate-500 focus:outline-none focus:border-indigo-500 w-52 font-mono"
                />
              </div>
            </div>

            {/* Grid of 32 Cards */}
            <div className="grid grid-cols-2 sm:grid-cols-4 md:grid-cols-4 gap-2.5 max-h-[520px] overflow-y-auto pr-1">
              {partitions
                .filter((p) => {
                  if (!searchQuery.trim()) return true;
                  const q = searchQuery.toLowerCase();
                  return (
                    p.partitionId.toString().includes(q) ||
                    `p${p.partitionId}`.includes(q) ||
                    (p.assignedWorker && p.assignedWorker.toLowerCase().includes(q))
                  );
                })
                .map((p) => {
                  const isSelected = selectedPartitionId === p.partitionId;
                  const assignedWorker = workers.find((w) => w.id === p.assignedWorker);
                  const isCrashed = assignedWorker?.status === 'CRASHED';
                  const palette = getWorkerPalette(p.assignedWorker);
                  const rangeInfo = workerRanges.get(p.assignedWorker || '');

                  return (
                    <div
                      key={p.partitionId}
                      onClick={() => {
                        setSelectedPartitionId(p.partitionId);
                        if (assignedWorker) onSelectWorker(assignedWorker);
                      }}
                      className={`p-3 rounded-xl border text-xs font-mono transition cursor-pointer flex flex-col justify-between ${
                        isSelected
                          ? 'border-indigo-400 bg-indigo-950/40 shadow-lg ring-1 ring-indigo-500'
                          : 'bg-[#111620]/60 border-slate-800 hover:border-slate-600'
                      }`}
                    >
                      <div>
                        {/* Header */}
                        <div className="flex items-center justify-between mb-1.5">
                          <span className="font-bold text-slate-100 flex items-center gap-1.5">
                            <span className="text-indigo-400">P{p.partitionId.toString().padStart(2, '0')}</span>
                          </span>
                          <span
                            className={`text-[9px] px-1.5 py-0.5 rounded font-bold ${
                              p.lag > 20
                                ? 'bg-rose-500/20 text-rose-300'
                                : p.lag > 10
                                ? 'bg-amber-500/20 text-amber-300'
                                : 'bg-emerald-500/20 text-emerald-400'
                            }`}
                          >
                            Lag: {p.lag}
                          </span>
                        </div>

                        {/* Assigned Worker Banner */}
                        <div
                          className={`px-2 py-1 rounded-lg border text-[10px] mb-2 flex items-center justify-between ${
                            isCrashed
                              ? 'bg-rose-950/40 border-rose-500/50 text-rose-300'
                              : `${palette.bg} ${palette.border} ${palette.text}`
                          }`}
                        >
                          <span className="font-bold truncate">{p.assignedWorker || 'Unassigned'}</span>
                          <span className="text-[9px] opacity-75">Bkr {p.leaderBroker}</span>
                        </div>

                        {/* Range Label if part of a group */}
                        <div className="text-[10px] text-slate-400 truncate mb-1">
                          <span className="text-slate-500">Range: </span>
                          <span className="text-slate-300 font-semibold">{rangeInfo?.formatted || 'None'}</span>
                        </div>
                      </div>

                      {/* Footer: throughput */}
                      <div className="mt-2 pt-2 border-t border-slate-800/80 flex items-center justify-between text-[10px] text-slate-400">
                        <span>Throughput</span>
                        <span className="text-emerald-400 font-bold">{p.throughput} msg/s</span>
                      </div>
                    </div>
                  );
                })}
            </div>
          </div>

          {/* Right: Active Partition & Responsible Worker Inspector (4 Cols) */}
          <div className="lg:col-span-4 bg-[#05070a]/80 border border-slate-700/50 rounded-2xl p-5 space-y-4 shadow-xl">
            <div className="border-b border-slate-800 pb-3 flex items-center justify-between">
              <div className="flex items-center gap-2">
                <div className="w-8 h-8 rounded-lg bg-indigo-500/20 border border-indigo-500/30 flex items-center justify-center">
                  <Eye className="w-4 h-4 text-indigo-400" />
                </div>
                <div>
                  <h4 className="text-xs font-bold text-white uppercase font-mono">
                    Partition Inspector: P{activePartition ? activePartition.partitionId.toString().padStart(2, '0') : '--'}
                  </h4>
                  <p className="text-[10px] text-slate-400">Live partition & responsible node metrics</p>
                </div>
              </div>

              {activePartition && (
                <span className="text-[10px] px-2 py-0.5 rounded-full bg-indigo-500/20 text-indigo-300 border border-indigo-500/40 font-mono">
                  Active
                </span>
              )}
            </div>

            {activePartition ? (
              <div className="space-y-4 text-xs font-mono">
                {/* Worker Assignment Card */}
                <div className="bg-[#111620] p-3.5 rounded-xl border border-slate-700/60 space-y-2">
                  <div className="text-[10px] text-slate-400 uppercase tracking-wider flex items-center justify-between">
                    <span>Currently Responsible Worker:</span>
                    <span
                      className={`px-1.5 py-0.5 rounded text-[9px] font-bold ${
                        activePartitionWorker?.status === 'HEALTHY'
                          ? 'bg-emerald-500/20 text-emerald-400'
                          : activePartitionWorker?.status === 'CRASHED'
                          ? 'bg-rose-500/20 text-rose-300'
                          : 'bg-amber-500/20 text-amber-300'
                      }`}
                    >
                      {activePartitionWorker?.status || 'UNKNOWN'}
                    </span>
                  </div>

                  <div className="flex items-center justify-between">
                    <span className="text-base font-bold text-white">
                      {activePartition.assignedWorker || 'Orphaned (No active worker)'}
                    </span>
                    {activePartitionWorker && (
                      <button
                        onClick={() => onSelectWorker(activePartitionWorker)}
                        className="text-[10px] px-2 py-1 rounded bg-slate-800 hover:bg-slate-700 text-indigo-300 transition"
                      >
                        Deep Dive
                      </button>
                    )}
                  </div>

                  <div className="pt-2 border-t border-slate-800 flex justify-between text-[11px]">
                    <span className="text-slate-400">Worker's Full Range:</span>
                    <span className="text-indigo-300 font-bold">
                      {workerRanges.get(activePartition.assignedWorker || '')?.formatted || 'None'}
                    </span>
                  </div>
                </div>

                {/* Kafka Offset & Watermark Metrics */}
                <div className="bg-[#111620] p-3.5 rounded-xl border border-slate-700/60 space-y-2.5">
                  <div className="text-[10px] text-indigo-400 uppercase tracking-widest flex items-center gap-1.5 font-bold">
                    <Database className="w-3.5 h-3.5" /> Kafka Partition Offset Telemetry
                  </div>

                  <div className="flex justify-between py-1 border-b border-slate-800">
                    <span className="text-slate-400">Current Committed Offset:</span>
                    <span className="text-slate-100 font-bold">{activePartition.currentOffset.toLocaleString()}</span>
                  </div>

                  <div className="flex justify-between py-1 border-b border-slate-800">
                    <span className="text-slate-400">Log End Offset (LEO):</span>
                    <span className="text-slate-200">{activePartition.logEndOffset.toLocaleString()}</span>
                  </div>

                  <div className="flex justify-between py-1 border-b border-slate-800">
                    <span className="text-slate-400">High Watermark:</span>
                    <span className="text-cyan-300">{activePartition.highWatermark.toLocaleString()}</span>
                  </div>

                  <div className="flex justify-between py-1 border-b border-slate-800">
                    <span className="text-slate-400">Consumer Lag:</span>
                    <span
                      className={`font-bold ${
                        activePartition.lag > 20
                          ? 'text-rose-400'
                          : activePartition.lag > 10
                          ? 'text-amber-400'
                          : 'text-emerald-400'
                      }`}
                    >
                      {activePartition.lag} messages
                    </span>
                  </div>

                  <div className="flex justify-between py-1">
                    <span className="text-slate-400">Leader Broker:</span>
                    <span className="text-slate-300">Broker-{activePartition.leaderBroker} (US-East-Rack-A)</span>
                  </div>

                  {/* Offset progress visual */}
                  <div className="pt-2">
                    <div className="flex justify-between text-[10px] text-slate-400 mb-1">
                      <span>Offset Synchronization</span>
                      <span>
                        {(
                          (activePartition.currentOffset / Math.max(1, activePartition.logEndOffset)) *
                          100
                        ).toFixed(1)}
                        %
                      </span>
                    </div>
                    <div className="w-full h-1.5 bg-slate-900 rounded-full overflow-hidden">
                      <div className="h-full bg-indigo-500 rounded-full" style={{ width: '99.8%' }} />
                    </div>
                  </div>
                </div>

                {/* RocksDB State in Worker for this Partition */}
                {activePartitionWorker && (
                  <div className="bg-[#111620] p-3.5 rounded-xl border border-slate-700/60 space-y-2">
                    <div className="text-[10px] text-amber-400 uppercase tracking-widest flex items-center gap-1.5 font-bold">
                      <HardDrive className="w-3.5 h-3.5" /> Embedded State Store (RocksDB)
                    </div>
                    <div className="flex justify-between text-[11px]">
                      <span className="text-slate-400">Active MemTable:</span>
                      <span className="text-amber-300 font-bold">
                        {activePartitionWorker.rocksDbState.memTableEntries} keys
                      </span>
                    </div>
                    <div className="flex justify-between text-[11px]">
                      <span className="text-slate-400">SSTable Files:</span>
                      <span className="text-slate-300">
                        {activePartitionWorker.rocksDbState.sstCount} files (L0)
                      </span>
                    </div>
                    <div className="flex justify-between text-[11px]">
                      <span className="text-slate-400">WAL Changelog Mirror:</span>
                      <span className="text-emerald-400 font-bold">In-Sync (RPO=0)</span>
                    </div>
                  </div>
                )}

                {/* Chaos Action for this specific worker */}
                {activePartitionWorker && activePartitionWorker.status === 'HEALTHY' && (
                  <button
                    onClick={() => handleSimulateKill(activePartitionWorker.id)}
                    className="w-full flex items-center justify-center gap-2 py-2 px-3 rounded-xl bg-rose-500/15 hover:bg-rose-500/25 text-rose-300 border border-rose-500/30 transition text-xs font-bold uppercase tracking-wider cursor-pointer"
                  >
                    <Flame className="w-3.5 h-3.5 text-rose-400" />
                    Crash {activePartitionWorker.id} (Test Rebalance)
                  </button>
                )}
              </div>
            ) : (
              <div className="text-center py-12 text-slate-500 text-xs">
                Select a partition from the spectrum or grid above to inspect telemetry.
              </div>
            )}
          </div>
        </div>
      )}

      {/* 5. VIEW MODE: WORKER RANGE MATRIX */}
      {viewMode === 'matrix' && (
        <div className="space-y-4">
          <div className="flex flex-col sm:flex-row sm:items-center justify-between gap-2 border-b border-slate-800 pb-3">
            <div>
              <h4 className="text-xs font-bold text-white uppercase tracking-wider font-mono flex items-center gap-2">
                <Server className="w-4 h-4 text-indigo-400" />
                Worker Node ↔ Partition Range Allocation Matrix (20 Workers)
              </h4>
              <p className="text-[11px] text-slate-400 mt-0.5">
                Each card shows the precise partition range(s) owned by each worker, along with aggregate lag and throughput.
              </p>
            </div>

            <div className="relative">
              <Search className="w-3.5 h-3.5 text-slate-500 absolute left-3 top-2.5" />
              <input
                type="text"
                placeholder="Search worker or partition range..."
                value={searchQuery}
                onChange={(e) => setSearchQuery(e.target.value)}
                className="bg-[#05070a] border border-slate-700/60 rounded-xl pl-8 pr-3 py-1.5 text-xs text-slate-200 placeholder:text-slate-500 focus:outline-none focus:border-indigo-500 w-64 font-mono"
              />
            </div>
          </div>

          <div className="grid grid-cols-1 sm:grid-cols-2 md:grid-cols-3 lg:grid-cols-4 gap-3.5 font-mono text-xs">
            {filteredWorkersList.map((w) => {
              const rangeData = workerRanges.get(w.id);
              const palette = getWorkerPalette(w.id);
              const isCrashed = w.status === 'CRASHED';
              const isRecovering = w.status === 'RECOVERING';
              const isSelected = selectedWorker?.id === w.id;

              // Aggregate lag for this worker's partitions
              const workerPartitions = partitions.filter((p) => p.assignedWorker === w.id);
              const workerLag = workerPartitions.reduce((acc, p) => acc + p.lag, 0);
              const workerThroughput = workerPartitions.reduce((acc, p) => acc + p.throughput, 0);

              return (
                <div
                  key={w.id}
                  onClick={() => onSelectWorker(w)}
                  className={`p-4 rounded-2xl border transition cursor-pointer flex flex-col justify-between ${
                    isSelected
                      ? 'border-indigo-400 bg-indigo-950/30 ring-1 ring-indigo-500'
                      : isCrashed
                      ? 'border-rose-500/50 bg-rose-950/20'
                      : isRecovering
                      ? 'border-amber-500/50 bg-amber-950/20'
                      : 'border-slate-800 bg-[#05070a]/80 hover:border-slate-600'
                  }`}
                >
                  <div>
                    {/* Header */}
                    <div className="flex items-center justify-between mb-2">
                      <div className="flex items-center gap-2">
                        <span className={`w-2.5 h-2.5 rounded-full ${isCrashed ? 'bg-rose-500' : palette.dot}`} />
                        <span className="font-bold text-white text-sm">{w.id}</span>
                      </div>
                      <span
                        className={`text-[9px] font-bold px-2 py-0.5 rounded-full ${
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

                    {/* ASSIGNED PARTITION RANGE IN BOLD */}
                    <div className="bg-[#111620] p-2.5 rounded-xl border border-slate-700/60 mb-3">
                      <div className="text-[9px] text-slate-400 uppercase tracking-widest mb-1">
                        Responsible Partition Range:
                      </div>
                      <div className={`text-xs font-bold ${isCrashed ? 'text-rose-400' : palette.text}`}>
                        {rangeData?.formatted || 'None (Standby)'}
                      </div>
                      <div className="text-[10px] text-slate-400 mt-1 flex items-center justify-between">
                        <span>Partitions Count:</span>
                        <span className="font-bold text-slate-200">{w.assignedPartitions.length}</span>
                      </div>
                    </div>

                    {/* Metric Row */}
                    <div className="space-y-1 text-[11px] mb-3">
                      <div className="flex justify-between text-slate-400">
                        <span>Aggregate Lag:</span>
                        <span className="text-slate-200 font-bold">{workerLag} msgs</span>
                      </div>
                      <div className="flex justify-between text-slate-400">
                        <span>Total Throughput:</span>
                        <span className="text-emerald-400 font-bold">{workerThroughput} msg/s</span>
                      </div>
                      <div className="flex justify-between text-slate-400">
                        <span>RocksDB Keys:</span>
                        <span className="text-amber-400 font-bold">{w.rocksDbState.memTableEntries}</span>
                      </div>
                    </div>
                  </div>

                  {/* Quick Partition List Badges */}
                  <div className="pt-2.5 border-t border-slate-800/80 flex flex-wrap gap-1">
                    {w.assignedPartitions.map((pId) => (
                      <span
                        key={pId}
                        onClick={(e) => {
                          e.stopPropagation();
                          setSelectedPartitionId(pId);
                          setViewMode('spectrum');
                        }}
                        className={`text-[9px] px-1.5 py-0.5 rounded font-bold border transition ${palette.badge} hover:scale-105`}
                        title={`Jump to Partition P${pId}`}
                      >
                        P{pId.toString().padStart(2, '0')}
                      </span>
                    ))}
                    {w.assignedPartitions.length === 0 && (
                      <span className="text-[10px] text-slate-500 italic">No assigned partitions</span>
                    )}
                  </div>
                </div>
              );
            })}
          </div>
        </div>
      )}

      {/* 6. VIEW MODE: RANGE TOPOLOGY FLOW */}
      {viewMode === 'flow' && (
        <div className="space-y-4 font-mono text-xs">
          <div className="border-b border-slate-800 pb-3">
            <h4 className="text-xs font-bold text-white uppercase tracking-wider flex items-center gap-2">
              <GitBranch className="w-4 h-4 text-indigo-400" />
              Kafka Partition Range to Worker Architecture Topology
            </h4>
            <p className="text-[11px] text-slate-400 mt-0.5">
              Visualizes the 4 major partition quadrant tiers (P00-P07, P08-P15, P16-P23, P24-P31) and their current consumer worker node targets.
            </p>
          </div>

          <div className="grid grid-cols-1 md:grid-cols-4 gap-4">
            {[
              { title: 'Quadrant 1: P00 – P07', range: [0, 7], desc: 'Broker 1 Primary Replicas' },
              { title: 'Quadrant 2: P08 – P15', range: [8, 15], desc: 'Broker 2 Primary Replicas' },
              { title: 'Quadrant 3: P16 – P23', range: [16, 23], desc: 'Broker 3 Primary Replicas' },
              { title: 'Quadrant 4: P24 – P31', range: [24, 31], desc: 'Distributed Multi-Rack' },
            ].map((quad, idx) => {
              const quadPartitions = partitions.filter(
                (p) => p.partitionId >= quad.range[0] && p.partitionId <= quad.range[1]
              );
              const assignedWorkerIds = Array.from(
                new Set(quadPartitions.map((p) => p.assignedWorker).filter(Boolean))
              ) as string[];

              return (
                <div key={idx} className="bg-[#05070a]/90 border border-slate-700/60 rounded-2xl p-4 space-y-3">
                  <div className="border-b border-slate-800 pb-2">
                    <div className="text-xs font-bold text-white">{quad.title}</div>
                    <div className="text-[10px] text-slate-400">{quad.desc}</div>
                  </div>

                  {/* Partitions in this tier */}
                  <div className="grid grid-cols-4 gap-1">
                    {quadPartitions.map((p) => {
                      const palette = getWorkerPalette(p.assignedWorker);
                      const isSelected = selectedPartitionId === p.partitionId;
                      return (
                        <div
                          key={p.partitionId}
                          onClick={() => {
                            setSelectedPartitionId(p.partitionId);
                            setViewMode('spectrum');
                          }}
                          className={`p-1.5 rounded-lg border text-center cursor-pointer transition ${
                            isSelected ? 'ring-1 ring-indigo-400 border-white' : palette.border
                          } ${palette.bg}`}
                        >
                          <div className="text-[10px] font-bold text-white">P{p.partitionId}</div>
                          <div className="text-[8px] opacity-75 truncate">
                            {p.assignedWorker ? p.assignedWorker.replace('worker-', 'W') : 'None'}
                          </div>
                        </div>
                      );
                    })}
                  </div>

                  {/* Workers serving this tier */}
                  <div className="pt-2 border-t border-slate-800">
                    <div className="text-[9px] text-slate-400 uppercase tracking-widest mb-1.5">
                      Assigned Workers ({assignedWorkerIds.length}):
                    </div>
                    <div className="flex flex-wrap gap-1.5">
                      {assignedWorkerIds.map((wId) => {
                        const w = workers.find((item) => item.id === wId);
                        const palette = getWorkerPalette(wId);
                        return (
                          <span
                            key={wId}
                            onClick={() => {
                              if (w) onSelectWorker(w);
                            }}
                            className={`px-2 py-0.5 rounded-lg border text-[10px] font-bold cursor-pointer transition hover:scale-105 ${palette.badge}`}
                          >
                            {wId}
                          </span>
                        );
                      })}
                    </div>
                  </div>
                </div>
              );
            })}
          </div>
        </div>
      )}
    </div>
  );
};
