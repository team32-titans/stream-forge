import React, { useState, useEffect } from 'react';
import {
  Activity,
  ArrowUpRight,
  BarChart3,
  CheckCircle2,
  Clock,
  Cpu,
  Database,
  Download,
  Gauge,
  Layers,
  Radio,
  Server,
  Zap,
} from 'lucide-react';
import {
  AreaChart,
  Area,
  XAxis,
  YAxis,
  Tooltip,
  ResponsiveContainer,
  BarChart,
  Bar,
} from 'recharts';
import { streamSimulation } from '../engine/simulationEngine';
import { PartitionState, StreamMetrics } from '../types/stream';

export const MetricsDashboard: React.FC = () => {
  const [metrics, setMetrics] = useState<StreamMetrics>(streamSimulation.metrics);
  const [partitions, setPartitions] = useState<PartitionState[]>(streamSimulation.partitions);
  const [historyData, setHistoryData] = useState<{ time: string; throughput: number; latency: number }[]>([]);

  useEffect(() => {
    const unsubscribe = streamSimulation.subscribe(() => {
      setMetrics({ ...streamSimulation.metrics });
      setPartitions([...streamSimulation.partitions]);

      setHistoryData((prev) => {
        const timeStr = new Date().toLocaleTimeString();
        const next = [
          ...prev,
          {
            time: timeStr,
            throughput: streamSimulation.metrics.currentThroughput,
            latency: streamSimulation.metrics.p99LatencyMs,
          },
        ];
        return next.slice(-20);
      });
    });
    return unsubscribe;
  }, []);

  const rawPrometheusText = `# HELP streamforge_events_processed_total Total IoT events processed
# TYPE streamforge_events_processed_total counter
streamforge_events_processed_total{service="streamforge_engine"} ${metrics.totalEventsProcessed}

# HELP streamforge_throughput_events_per_sec Instantaneous events per second
# TYPE streamforge_throughput_events_per_sec gauge
streamforge_throughput_events_per_sec{service="streamforge_engine"} ${metrics.currentThroughput}

# HELP streamforge_processing_latency_ms Latency percentiles in milliseconds
# TYPE streamforge_processing_latency_ms gauge
streamforge_processing_latency_ms{quantile="0.50"} ${metrics.averageLatencyMs}
streamforge_processing_latency_ms{quantile="0.95"} ${metrics.p95LatencyMs}
streamforge_processing_latency_ms{quantile="0.99"} ${metrics.p99LatencyMs}

# HELP streamforge_active_workers Count of active Python worker processes
# TYPE streamforge_active_workers gauge
streamforge_active_workers{cluster="prod-streamforge-cluster"} ${metrics.activeWorkers}

# HELP streamforge_rocksdb_memory_mb In-process RocksDB MemTable + BlockCache RAM
# TYPE streamforge_rocksdb_memory_mb gauge
streamforge_rocksdb_memory_mb{engine="rocksdb_lsm"} ${metrics.rocksDbTotalMemoryMb}
`;

  return (
    <div className="space-y-4">
      {/* Header Bento Card */}
      <div className="bg-[#111620]/60 backdrop-blur-sm border border-slate-700/40 rounded-3xl p-6 shadow-xl relative overflow-hidden">
        <div className="flex flex-col md:flex-row md:items-center justify-between gap-4">
          <div className="flex items-center gap-3.5">
            <div className="w-10 h-10 rounded-xl bg-indigo-500/20 border border-indigo-500/40 flex items-center justify-center shadow-[0_0_15px_rgba(99,102,241,0.3)]">
              <Gauge className="w-5 h-5 text-indigo-400" />
            </div>
            <div>
              <div className="flex items-center gap-2">
                <h2 className="text-base font-bold text-white tracking-tight">
                  PROMETHEUS & GRAFANA PERFORMANCE METRICS
                </h2>
                <span className="text-[9px] px-2 py-0.5 rounded-full bg-slate-800 text-indigo-300 font-mono font-bold border border-slate-700 uppercase tracking-wider">
                  100k evt/s Ready
                </span>
              </div>
              <p className="text-[11px] text-slate-400 uppercase tracking-wider mt-0.5">
                Real-time latency histograms, partition consumer lag, RocksDB memory utilization, and scrape endpoints
              </p>
            </div>
          </div>
        </div>
      </div>

      {/* Top 4 KPI Cards (Bento Metric Tiles) */}
      <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-4">
        {/* KPI 1 */}
        <div className="bg-[#111620]/60 backdrop-blur-sm border border-slate-700/40 rounded-3xl p-5 shadow-xl">
          <div className="text-[10px] uppercase font-bold text-slate-400 flex items-center justify-between tracking-wider">
            <span>Throughput Rate</span>
            <Activity className="w-4 h-4 text-indigo-400" />
          </div>
          <div className="text-2xl font-mono font-bold text-white mt-2">
            {metrics.currentThroughput.toLocaleString()}{' '}
            <span className="text-xs text-slate-400 font-normal">evt/s</span>
          </div>
          <div className="text-[11px] text-emerald-400 mt-2 flex items-center gap-1 font-mono">
            <ArrowUpRight className="w-3.5 h-3.5" /> Peak: {metrics.peakThroughput.toLocaleString()} evt/s
          </div>
        </div>

        {/* KPI 2 */}
        <div className="bg-[#111620]/60 backdrop-blur-sm border border-slate-700/40 rounded-3xl p-5 shadow-xl">
          <div className="text-[10px] uppercase font-bold text-slate-400 flex items-center justify-between tracking-wider">
            <span>p99 Latency</span>
            <Clock className="w-4 h-4 text-indigo-400" />
          </div>
          <div className="text-2xl font-mono font-bold text-indigo-300 mt-2">
            {metrics.p99LatencyMs}{' '}
            <span className="text-xs text-slate-400 font-normal">ms</span>
          </div>
          <div className="text-[11px] text-slate-400 mt-2 flex items-center gap-1 font-mono">
            <span>p50: {metrics.averageLatencyMs}ms • p95: {metrics.p95LatencyMs}ms</span>
          </div>
        </div>

        {/* KPI 3 */}
        <div className="bg-[#111620]/60 backdrop-blur-sm border border-slate-700/40 rounded-3xl p-5 shadow-xl">
          <div className="text-[10px] uppercase font-bold text-slate-400 flex items-center justify-between tracking-wider">
            <span>RocksDB Memory</span>
            <Database className="w-4 h-4 text-amber-400" />
          </div>
          <div className="text-2xl font-mono font-bold text-amber-300 mt-2">
            {metrics.rocksDbTotalMemoryMb}{' '}
            <span className="text-xs text-slate-400 font-normal">MB</span>
          </div>
          <div className="text-[11px] text-emerald-400 mt-2 font-mono">
            Hit Ratio: 99.2% in RAM
          </div>
        </div>

        {/* KPI 4 */}
        <div className="bg-[#111620]/60 backdrop-blur-sm border border-slate-700/40 rounded-3xl p-5 shadow-xl">
          <div className="text-[10px] uppercase font-bold text-slate-400 flex items-center justify-between tracking-wider">
            <span>Total Kafka Lag</span>
            <Layers className="w-4 h-4 text-emerald-400" />
          </div>
          <div className="text-2xl font-mono font-bold text-emerald-400 mt-2">
            {metrics.totalLag}{' '}
            <span className="text-xs text-slate-400 font-normal">records</span>
          </div>
          <div className="text-[11px] text-slate-400 mt-2 font-mono">
            32 Partitions Balanced
          </div>
        </div>
      </div>

      {/* Live Charts Section */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
        {/* Real-time Throughput Area Chart */}
        <div className="bg-[#111620]/60 backdrop-blur-sm border border-slate-700/40 rounded-3xl p-6 shadow-xl">
          <div className="flex items-center justify-between mb-4 border-b border-slate-700/50 pb-3">
            <h3 className="text-xs font-bold uppercase tracking-widest text-slate-200 flex items-center gap-2">
              <Activity className="w-4 h-4 text-indigo-400" />
              Throughput (events/sec)
            </h3>
            <span className="text-[11px] font-mono text-indigo-400 uppercase tracking-wider">100k Benchmark</span>
          </div>

          <div className="h-56 w-full">
            <ResponsiveContainer width="100%" height="100%">
              <AreaChart data={historyData}>
                <defs>
                  <linearGradient id="throughputGrad" x1="0" y1="0" x2="0" y2="1">
                    <stop offset="5%" stopColor="#6366f1" stopOpacity={0.4} />
                    <stop offset="95%" stopColor="#6366f1" stopOpacity={0.0} />
                  </linearGradient>
                </defs>
                <XAxis dataKey="time" stroke="#64748b" fontSize={10} tickLine={false} />
                <YAxis stroke="#64748b" fontSize={10} tickLine={false} domain={[0, 120000]} />
                <Tooltip
                  contentStyle={{ backgroundColor: '#05070a', borderColor: '#334155', borderRadius: '16px' }}
                />
                <Area
                  type="monotone"
                  dataKey="throughput"
                  stroke="#6366f1"
                  strokeWidth={2}
                  fillOpacity={1}
                  fill="url(#throughputGrad)"
                />
              </AreaChart>
            </ResponsiveContainer>
          </div>
        </div>

        {/* Partition Lag Distribution Bar Chart */}
        <div className="bg-[#111620]/60 backdrop-blur-sm border border-slate-700/40 rounded-3xl p-6 shadow-xl">
          <div className="flex items-center justify-between mb-4 border-b border-slate-700/50 pb-3">
            <h3 className="text-xs font-bold uppercase tracking-widest text-slate-200 flex items-center gap-2">
              <Layers className="w-4 h-4 text-emerald-400" />
              Kafka Consumer Lag per Partition (P0 – P31)
            </h3>
            <span className="text-[11px] font-mono text-emerald-400 uppercase tracking-wider">Uniform Distribution</span>
          </div>

          <div className="h-56 w-full">
            <ResponsiveContainer width="100%" height="100%">
              <BarChart data={partitions.slice(0, 16)}>
                <XAxis dataKey="partitionId" stroke="#64748b" fontSize={10} tickFormatter={(val) => `P${val}`} />
                <YAxis stroke="#64748b" fontSize={10} />
                <Tooltip
                  contentStyle={{ backgroundColor: '#05070a', borderColor: '#334155', borderRadius: '16px' }}
                />
                <Bar dataKey="lag" fill="#10b981" radius={[4, 4, 0, 0]} />
              </BarChart>
            </ResponsiveContainer>
          </div>
        </div>
      </div>

      {/* Raw Prometheus Text Exporter Endpoint */}
      <div className="bg-[#111620]/60 backdrop-blur-sm border border-slate-700/40 rounded-3xl p-6 shadow-xl">
        <div className="flex items-center justify-between mb-3.5 border-b border-slate-700/50 pb-3">
          <div className="flex items-center gap-2">
            <Server className="w-4 h-4 text-indigo-400" />
            <h3 className="text-xs font-bold uppercase tracking-widest text-slate-200">
              Prometheus HTTP Scrape Endpoint (GET /metrics)
            </h3>
          </div>
          <span className="text-[10px] font-mono text-slate-400 uppercase tracking-wider">
            text/plain; version=0.0.4
          </span>
        </div>

        <pre className="bg-[#05070a] p-4 rounded-2xl border border-slate-700/50 font-mono text-xs text-indigo-300 overflow-x-auto">
          {rawPrometheusText}
        </pre>
      </div>
    </div>
  );
};
