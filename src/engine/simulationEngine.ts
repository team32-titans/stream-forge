import {
  ChaosEvent,
  ChangelogRecord,
  PartitionState,
  StreamMetrics,
  TelemetryEvent,
  WindowAggregate,
  WorkerNode,
} from '../types/stream';

export class StreamForgeSimulation {
  public workers: WorkerNode[] = [];
  public partitions: PartitionState[] = [];
  public changelogRecords: ChangelogRecord[] = [];
  public recentEvents: TelemetryEvent[] = [];
  public activeWindowAggregates: Map<string, WindowAggregate> = new Map();
  public emittedWindows: WindowAggregate[] = [];
  public chaosHistory: ChaosEvent[] = [];
  public currentChaosEvent: ChaosEvent | null = null;
  public metrics: StreamMetrics;
  
  private isRunning: boolean = true;
  private eventRate: number = 25000; // default 25,000 events/sec simulated
  private totalTrucks: number = 50000;
  private totalPartitionsCount: number = 32;
  private totalWorkersCount: number = 20;
  private tickInterval: number | null = null;
  private listeners: Set<() => void> = new Set();
  private watermarkDelayMs: number = 15000; // 15 sec watermark delay
  private currentWatermark: number = Date.now() - 15000;

  constructor() {
    this.metrics = {
      totalEventsProcessed: 1428500,
      currentThroughput: 24800,
      peakThroughput: 104200,
      averageLatencyMs: 0.84,
      p95LatencyMs: 1.22,
      p99LatencyMs: 1.85,
      totalPartitions: 32,
      activeWorkers: 20,
      healthyWorkers: 20,
      totalLag: 142,
      rocksDbTotalMemoryMb: 684.5,
      changelogReplicationRate: 24750,
      lateEventsHandled: 84,
      windowsEmitted: 1240,
    };

    this.initializeCluster();
    this.seedInitialState();
  }

  public subscribe(listener: () => void): () => void {
    this.listeners.add(listener);
    return () => this.listeners.delete(listener);
  }

  private notify(): void {
    this.listeners.forEach((l) => l());
  }

  public initializeCluster(): void {
    // Create 32 partitions
    this.partitions = [];
    for (let p = 0; p < this.totalPartitionsCount; p++) {
      this.partitions.push({
        partitionId: p,
        assignedWorker: null,
        leaderBroker: (p % 3) + 1,
        highWatermark: 50000 + p * 120,
        logEndOffset: 50000 + p * 120 + Math.floor(Math.random() * 20),
        currentOffset: 50000 + p * 120,
        lag: Math.floor(Math.random() * 15),
        throughput: Math.floor(700 + Math.random() * 200),
      });
    }

    // Create 20 Workers
    this.workers = [];
    for (let w = 1; w <= this.totalWorkersCount; w++) {
      const workerId = `worker-${w.toString().padStart(2, '0')}`;
      this.workers.push({
        id: workerId,
        workerIndex: w,
        status: 'HEALTHY',
        assignedPartitions: [],
        cpuUsage: 25 + Math.floor(Math.random() * 20),
        memoryMb: 110 + Math.floor(Math.random() * 45),
        eventsProcessed: 71000 + Math.floor(Math.random() * 5000),
        processingRate: Math.floor(1200 + Math.random() * 100),
        rocksDbState: {
          memTableEntries: 180 + Math.floor(Math.random() * 80),
          memTableBytes: (180 + Math.floor(Math.random() * 80)) * 140,
          immutableMemTables: Math.floor(Math.random() * 2),
          sstCount: 4 + Math.floor(Math.random() * 4),
          walOffset: 45000 + Math.floor(Math.random() * 2000),
          lastChangelogOffset: 45000 + Math.floor(Math.random() * 2000),
          cacheHitRatio: 0.982 + Math.random() * 0.015,
        },
        lastHeartbeat: Date.now(),
        rebalanceHistory: [
          { timestamp: Date.now() - 3600000, event: 'Cluster bootstrap sticky assignment' },
        ],
      });
    }

    // Sticky distribute partitions across workers
    this.rebalancePartitions();
  }

  public rebalancePartitions(): void {
    const healthyWorkers = this.workers.filter((w) => w.status === 'HEALTHY' || w.status === 'RECOVERING');
    if (healthyWorkers.length === 0) return;

    // Clear assignments
    this.workers.forEach((w) => (w.assignedPartitions = []));

    // Evenly assign 32 partitions to healthy workers
    this.partitions.forEach((p, idx) => {
      const targetWorker = healthyWorkers[idx % healthyWorkers.length];
      p.assignedWorker = targetWorker.id;
      targetWorker.assignedPartitions.push(p.partitionId);
    });

    this.metrics.activeWorkers = this.workers.filter((w) => w.status !== 'CRASHED').length;
    this.metrics.healthyWorkers = this.workers.filter((w) => w.status === 'HEALTHY').length;
  }

  private seedInitialState(): void {
    const now = Date.now();
    const windowStart = now - (now % 300000); // 5 min interval
    const windowEnd = windowStart + 300000;

    // Seed 15 sample truck states
    for (let i = 1; i <= 25; i++) {
      const truckId = `TRK-${(i * 1234).toString().padStart(5, '0')}`;
      const baseTemp = -22.0 + (i % 8) * 1.5;
      const count = 18 + Math.floor(Math.random() * 10);
      const sum = baseTemp * count + (Math.random() - 0.5) * 4;
      const avg = Number((sum / count).toFixed(2));

      this.activeWindowAggregates.set(truckId, {
        truckId,
        windowStart,
        windowEnd,
        count,
        sumTemp: Number(sum.toFixed(2)),
        avgTemp: avg,
        minTemp: Number((baseTemp - 1.2).toFixed(2)),
        maxTemp: Number((baseTemp + 1.4).toFixed(2)),
        lastReadingTime: now - Math.floor(Math.random() * 5000),
        status: 'OPEN',
      });

      // Add to changelog
      this.changelogRecords.unshift({
        offset: 1000 + i,
        partition: i % this.totalPartitionsCount,
        key: `${truckId}:${windowStart}`,
        value: {
          count,
          sum: Number(sum.toFixed(2)),
          avg,
          min: Number((baseTemp - 1.2).toFixed(2)),
          max: Number((baseTemp + 1.4).toFixed(2)),
        },
        timestamp: now - i * 1000,
        operation: 'PUT',
        workerSource: `worker-${((i % 20) + 1).toString().padStart(2, '0')}`,
      });
    }
  }

  public startSimulation(): void {
    if (this.tickInterval) return;
    this.isRunning = true;
    this.tickInterval = window.setInterval(() => {
      if (!this.isRunning) return;
      this.tick();
    }, 1000);
  }

  public stopSimulation(): void {
    if (this.tickInterval) {
      clearInterval(this.tickInterval);
      this.tickInterval = null;
    }
    this.isRunning = false;
    this.notify();
  }

  public togglePlay(): boolean {
    if (this.isRunning) {
      this.stopSimulation();
      return false;
    } else {
      this.startSimulation();
      return true;
    }
  }

  public setRate(rate: number): void {
    this.eventRate = Math.max(1000, Math.min(150000, rate));
    this.notify();
  }

  public getRate(): number {
    return this.eventRate;
  }

  public getIsRunning(): boolean {
    return this.isRunning;
  }

  private tick(): void {
    const now = Date.now();
    this.currentWatermark = now - this.watermarkDelayMs;

    // Simulate batch of telemetry events based on eventRate
    const batchSize = Math.floor(this.eventRate / 10);
    const newEvents: TelemetryEvent[] = [];

    const healthyWorkers = this.workers.filter((w) => w.status === 'HEALTHY');
    const totalHealthy = healthyWorkers.length || 1;

    for (let i = 0; i < Math.min(10, batchSize); i++) {
      const truckNum = Math.floor(Math.random() * this.totalTrucks) + 1;
      const truckId = `TRK-${truckNum.toString().padStart(5, '0')}`;
      const partition = Math.abs(this.hashString(truckId)) % this.totalPartitionsCount;
      const isCritical = Math.random() < 0.03;
      const baseTemp = isCritical ? 4.2 : -20.0 + (truckNum % 10) * 0.8;
      const reading = Number((baseTemp + (Math.random() - 0.5) * 1.5).toFixed(2));

      const event: TelemetryEvent = {
        truckId,
        timestamp: now - Math.floor(Math.random() * 2000),
        temperature: reading,
        engineRpm: Math.floor(1400 + Math.random() * 800),
        latitude: Number((37.77 + (Math.random() - 0.5) * 4).toFixed(4)),
        longitude: Number((-122.41 + (Math.random() - 0.5) * 4).toFixed(4)),
        speedKmH: Number((65 + Math.random() * 25).toFixed(1)),
        partition,
        refrigerationStatus: isCritical ? 'CRITICAL' : reading > 0 ? 'WARNING' : 'OPTIMAL',
      };

      newEvents.push(event);
      this.updateWindowAggregate(event);
    }

    this.recentEvents = [...newEvents, ...this.recentEvents].slice(0, 30);

    // Update worker stats
    this.workers.forEach((w) => {
      if (w.status === 'HEALTHY') {
        const shareOfEvents = Math.floor(this.eventRate / totalHealthy);
        w.eventsProcessed += shareOfEvents;
        w.processingRate = shareOfEvents + Math.floor((Math.random() - 0.5) * 200);
        w.cpuUsage = Math.min(95, Math.floor(30 + (this.eventRate / 100000) * 45 + Math.random() * 10));
        w.rocksDbState.walOffset += Math.floor(shareOfEvents * 0.4);
        w.rocksDbState.lastChangelogOffset = w.rocksDbState.walOffset;
        w.rocksDbState.memTableEntries = (w.rocksDbState.memTableEntries + 8) % 500;
        w.lastHeartbeat = now;
      }
    });

    // Update metrics
    this.metrics.totalEventsProcessed += this.eventRate;
    this.metrics.currentThroughput = Math.floor(this.eventRate * (0.95 + Math.random() * 0.1));
    this.metrics.peakThroughput = Math.max(this.metrics.peakThroughput, this.metrics.currentThroughput);
    this.metrics.averageLatencyMs = Number((0.6 + (this.eventRate / 100000) * 0.8 + Math.random() * 0.1).toFixed(2));
    this.metrics.p95LatencyMs = Number((this.metrics.averageLatencyMs * 1.45).toFixed(2));
    this.metrics.p99LatencyMs = Number((this.metrics.averageLatencyMs * 2.1).toFixed(2));
    this.metrics.totalLag = Math.floor(Math.random() * 180);
    this.metrics.changelogReplicationRate = Math.floor(this.metrics.currentThroughput * 0.998);

    // Update partition lag
    this.partitions.forEach((p) => {
      p.currentOffset += Math.floor(this.eventRate / this.totalPartitionsCount);
      p.logEndOffset = p.currentOffset + Math.floor(Math.random() * 12);
      p.lag = p.logEndOffset - p.currentOffset;
      p.throughput = Math.floor(this.metrics.currentThroughput / this.totalPartitionsCount);
    });

    this.notify();
  }

  private updateWindowAggregate(event: TelemetryEvent): void {
    const now = event.timestamp;
    const windowStart = now - (now % 300000);
    const windowEnd = windowStart + 300000;

    let agg = this.activeWindowAggregates.get(event.truckId);
    if (!agg || agg.windowStart !== windowStart) {
      agg = {
        truckId: event.truckId,
        windowStart,
        windowEnd,
        count: 1,
        sumTemp: event.temperature,
        avgTemp: event.temperature,
        minTemp: event.temperature,
        maxTemp: event.temperature,
        lastReadingTime: now,
        status: 'OPEN',
      };
    } else {
      agg.count += 1;
      agg.sumTemp = Number((agg.sumTemp + event.temperature).toFixed(2));
      agg.avgTemp = Number((agg.sumTemp / agg.count).toFixed(2));
      agg.minTemp = Math.min(agg.minTemp, event.temperature);
      agg.maxTemp = Math.max(agg.maxTemp, event.temperature);
      agg.lastReadingTime = now;
    }

    this.activeWindowAggregates.set(event.truckId, agg);

    // Publish to changelog stream (keep last 50)
    const workerId = this.partitions[event.partition]?.assignedWorker || 'worker-01';
    this.changelogRecords.unshift({
      offset: 10000 + this.changelogRecords.length,
      partition: event.partition,
      key: `${event.truckId}:${windowStart}`,
      value: {
        count: agg.count,
        sum: agg.sumTemp,
        avg: agg.avgTemp,
        min: agg.minTemp,
        max: agg.maxTemp,
      },
      timestamp: now,
      operation: 'MERGE',
      workerSource: workerId,
    });

    if (this.changelogRecords.length > 60) {
      this.changelogRecords.pop();
    }
  }

  private hashString(str: string): number {
    let hash = 0;
    for (let i = 0; i < str.length; i++) {
      hash = (hash << 5) - hash + str.charCodeAt(i);
      hash |= 0;
    }
    return hash;
  }

  // CHAOS ENGINEERING METHODS

  public async triggerKillWorker(workerId: string = 'worker-04'): Promise<void> {
    const target = this.workers.find((w) => w.id === workerId);
    if (!target) return;

    const orphanedPartitions = [...target.assignedPartitions];
    target.status = 'CRASHED';
    target.cpuUsage = 0;
    target.processingRate = 0;
    target.assignedPartitions = [];

    const chaosEvent: ChaosEvent = {
      id: `chaos-${Date.now()}`,
      timestamp: Date.now(),
      type: 'WORKER_KILL',
      targetWorker: workerId,
      description: `Simulated SIGKILL on ${workerId}. Rebalancing Partitions [${orphanedPartitions.join(', ')}] with zero data loss.`,
      recoveryLog: [
        `[T+00ms] SIGKILL sent to ${workerId}. Process terminated abruptly mid-window aggregation.`,
        `[T+40ms] Kafka Broker missed consumer heartbeat. Consumer Group coordinator triggered REBALANCE_REQUIRED.`,
        `[T+95ms] Partitions [${orphanedPartitions.join(', ')}] unassigned and state locks released.`,
      ],
      status: 'ACTIVE',
    };

    this.currentChaosEvent = chaosEvent;
    this.chaosHistory.unshift(chaosEvent);
    this.metrics.healthyWorkers = this.workers.filter((w) => w.status === 'HEALTHY').length;
    this.notify();

    // Step 1: Assign to Worker 05 (or next available healthy worker)
    await new Promise((r) => setTimeout(r, 700));
    const standbyWorker = this.workers.find((w) => w.id === 'worker-05' && w.status === 'HEALTHY') ||
      this.workers.find((w) => w.status === 'HEALTHY');

    if (standbyWorker) {
      standbyWorker.status = 'RECOVERING';
      chaosEvent.recoveryLog.push(
        `[T+210ms] Cooperative Sticky Assignor selected ${standbyWorker.id} to adopt orphaned partitions [${orphanedPartitions.join(', ')}].`
      );
      this.notify();

      // Step 2: Restore RocksDB State from Kafka Changelog
      await new Promise((r) => setTimeout(r, 900));
      chaosEvent.recoveryLog.push(
        `[T+480ms] ${standbyWorker.id} initialized RocksDB state store. Replaying changelog topic 'streamforge.truck_state.changelog' from offset 0...`
      );
      standbyWorker.rocksDbState.memTableEntries += 140;
      standbyWorker.rocksDbState.walOffset += 240;
      this.notify();

      // Step 3: Complete State Restoration & Resume
      await new Promise((r) => setTimeout(r, 800));
      standbyWorker.assignedPartitions.push(...orphanedPartitions);
      orphanedPartitions.forEach((pId) => {
        if (this.partitions[pId]) {
          this.partitions[pId].assignedWorker = standbyWorker.id;
        }
      });
      standbyWorker.status = 'HEALTHY';
      chaosEvent.status = 'RESOLVED';
      chaosEvent.recoveryLog.push(
        `[T+790ms] SUCCESS: 100% of rolling 5-minute truck averages restored into RocksDB. Processing resumed with 0 missing readings!`
      );

      standbyWorker.rebalanceHistory.unshift({
        timestamp: Date.now(),
        event: `Recovered partitions [${orphanedPartitions.join(', ')}] from ${workerId} via changelog`,
      });

      this.metrics.healthyWorkers = this.workers.filter((w) => w.status === 'HEALTHY').length;
      this.notify();
    }
  }

  public reviveWorker(workerId: string): void {
    const target = this.workers.find((w) => w.id === workerId);
    if (!target) return;

    target.status = 'HEALTHY';
    target.cpuUsage = 35;
    target.processingRate = 1250;
    this.rebalancePartitions();
    this.notify();
  }

  public injectLateDataBurst(): void {
    const now = Date.now();
    const staleTime = now - 240000; // 4 minutes late
    const lateEvents: TelemetryEvent[] = [];

    for (let i = 1; i <= 6; i++) {
      const truckId = `TRK-${(i * 3333).toString().padStart(5, '0')}`;
      const partition = Math.abs(this.hashString(truckId)) % this.totalPartitionsCount;
      lateEvents.push({
        truckId,
        timestamp: staleTime,
        temperature: -18.5,
        engineRpm: 1800,
        latitude: 37.7749,
        longitude: -122.4194,
        speedKmH: 72.0,
        partition,
        refrigerationStatus: 'OPTIMAL',
        isLate: true,
      });
    }

    this.recentEvents = [...lateEvents, ...this.recentEvents].slice(0, 30);
    this.metrics.lateEventsHandled += lateEvents.length;

    const chaosEvent: ChaosEvent = {
      id: `chaos-${Date.now()}`,
      timestamp: Date.now(),
      type: 'LATE_DATA_BURST',
      description: `Injected 6 delayed telemetry events (4 minutes out-of-order) to test Watermark tolerance.`,
      recoveryLog: [
        `[Event-Time Watermark] Current Watermark: ${new Date(this.currentWatermark).toLocaleTimeString()}`,
        `[Watermark Check] Late events timestamp: ${new Date(staleTime).toLocaleTimeString()}`,
        `[Result] Late readings merged into active open 5-minute window or routed to dead-letter side-output.`,
      ],
      status: 'RESOLVED',
    };
    this.chaosHistory.unshift(chaosEvent);
    this.notify();
  }

  public injectColdChainSpike(): void {
    const now = Date.now();
    const anomalyEvents: TelemetryEvent[] = [];

    for (let i = 1; i <= 8; i++) {
      const truckId = `TRK-${(i * 1111).toString().padStart(5, '0')}`;
      const partition = Math.abs(this.hashString(truckId)) % this.totalPartitionsCount;
      const spikeTemp = 8.5 + (i % 4) * 2.1; // dangerous thawing temperature!

      anomalyEvents.push({
        truckId,
        timestamp: now,
        temperature: Number(spikeTemp.toFixed(2)),
        engineRpm: 2400,
        latitude: 37.7749,
        longitude: -122.4194,
        speedKmH: 80.0,
        partition,
        refrigerationStatus: 'CRITICAL',
      });

      this.updateWindowAggregate(anomalyEvents[anomalyEvents.length - 1]);
    }

    this.recentEvents = [...anomalyEvents, ...this.recentEvents].slice(0, 30);
    this.notify();
  }
}

// Global simulation singleton
export const streamSimulation = new StreamForgeSimulation();
