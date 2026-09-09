export type WindowType = 'tumbling' | 'hopping' | 'session';

export interface TelemetryEvent {
  truckId: string;
  timestamp: number; // Unix timestamp in ms
  temperature: number; // in Celsius (e.g. -22.5 to 35.0)
  engineRpm: number;
  latitude: number;
  longitude: number;
  speedKmH: number;
  partition: number;
  refrigerationStatus: 'OPTIMAL' | 'WARNING' | 'CRITICAL' | 'DEFROST';
  isLate?: boolean;
}

export interface WindowAggregate {
  truckId: string;
  windowStart: number;
  windowEnd: number;
  count: number;
  sumTemp: number;
  avgTemp: number;
  minTemp: number;
  maxTemp: number;
  lastReadingTime: number;
  status: 'OPEN' | 'EMITTED' | 'LATE_UPDATED';
}

export interface WorkerNode {
  id: string; // e.g. "worker-04"
  workerIndex: number;
  status: 'HEALTHY' | 'CRASHED' | 'REBALANCING' | 'RECOVERING' | 'DRAINING';
  assignedPartitions: number[];
  cpuUsage: number; // percentage
  memoryMb: number;
  eventsProcessed: number;
  processingRate: number; // events/sec
  rocksDbState: {
    memTableEntries: number;
    memTableBytes: number;
    immutableMemTables: number;
    sstCount: number;
    walOffset: number;
    lastChangelogOffset: number;
    cacheHitRatio: number;
  };
  lastHeartbeat: number;
  rebalanceHistory: {
    timestamp: number;
    event: string;
    durationMs?: number;
  }[];
}

export interface PartitionState {
  partitionId: number;
  assignedWorker: string | null;
  leaderBroker: number;
  highWatermark: number;
  logEndOffset: number;
  currentOffset: number;
  lag: number;
  throughput: number; // msgs/sec
}

export interface ChangelogRecord {
  offset: number;
  partition: number;
  key: string; // e.g. "TRK-10492:1709283600"
  value: {
    count: number;
    sum: number;
    avg: number;
    min: number;
    max: number;
  };
  timestamp: number;
  operation: 'PUT' | 'MERGE' | 'DELETE' | 'CHECKPOINT';
  workerSource: string;
}

export interface ChaosEvent {
  id: string;
  timestamp: number;
  type: 'WORKER_KILL' | 'NETWORK_PARTITION' | 'LATE_DATA_BURST' | 'SLOW_DISK' | 'REBALANCE_TRIGGER';
  targetWorker?: string;
  targetPartition?: number;
  description: string;
  recoveryLog: string[];
  status: 'ACTIVE' | 'RESOLVING' | 'RESOLVED';
}

export interface StreamMetrics {
  totalEventsProcessed: number;
  currentThroughput: number; // msgs/sec
  peakThroughput: number;
  averageLatencyMs: number;
  p95LatencyMs: number;
  p99LatencyMs: number;
  totalPartitions: number;
  activeWorkers: number;
  healthyWorkers: number;
  totalLag: number;
  rocksDbTotalMemoryMb: number;
  changelogReplicationRate: number;
  lateEventsHandled: number;
  windowsEmitted: number;
}
