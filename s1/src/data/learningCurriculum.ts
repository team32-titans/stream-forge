export interface CurriculumTopic {
  id: string;
  title: string;
  week: number;
  roleFocus: string;
  summary: string;
  deepDive: string;
  codeSnippets: { title: string; lang: string; code: string }[];
  vivaQuestions: { q: string; a: string; tip: string }[];
  designPatterns: string[];
}

export const MEMBER1_CURRICULUM: CurriculumTopic[] = [
  {
    id: 'week1-kafka-state-foundation',
    title: 'Week 1: Kafka Partitioning, Murmur2 Hashing & Streaming Foundation',
    week: 1,
    roleFocus: 'Kafka Foundation & Producer-Consumer State Contracts',
    summary: 'Mastering partitioned event ingestion for 50,000 IoT refrigerated trucks and designing strict schemas.',
    deepDive: `As **Member 1 (Stream Processing & Stateful Engine)**, your foundation begins with Kafka's distributed commit log.

1. **Why Kafka Partitions are Key**:
   - In stream processing, parallelization is bounded by the number of Kafka partitions. With 32 partitions and 20 worker nodes, each worker handles 1 to 2 partitions.
   - Using consistent hashing (Murmur2 on \`truck_id\`) ensures that all events for a specific truck (\`TRK-00042\`) ALWAYS route to the exact same partition.
   - **Critical Rule**: Because all readings for a truck arrive on the same partition, a worker can compute rolling averages locally in its embedded RocksDB state store without costly inter-node network shuffles!

2. **Schema Contracts with Pydantic & Zero-Copy Deserialization**:
   - Industrial standard stream processors avoid arbitrary JSON parsing overhead. We use strict Pydantic V2 models with pre-compiled validators to ensure corrupt sensor readings (-1000°C or non-numeric values) are rejected immediately at the filter stage.`,
    codeSnippets: [
      {
        title: 'Murmur2 Consistent Partition Hashing in Python',
        lang: 'python',
        code: `def get_truck_partition(truck_id: str, num_partitions: int = 32) -> int:
    """Murmur2 hash simulation ensuring all telemetry for truck_id lands on the same worker."""
    import mmh3  # MurmurHash3
    return abs(mmh3.hash(truck_id)) % num_partitions`
      }
    ],
    vivaQuestions: [
      {
        q: 'Why do we partition by truck_id rather than timestamp or random round-robin?',
        a: 'Partitioning by truck_id guarantees that all sequential sensor readings from a single truck land on the same partition and thus the same worker node. This enables local stateful window aggregation inside RocksDB without cross-network data shuffles between workers.',
        tip: 'Highlight "Keyed Stream Processing" and "Zero Network Shuffle for State Accumulation".'
      },
      {
        q: 'What happens if we have 20 workers but only 10 Kafka partitions?',
        a: 'In Kafka consumer groups, only one consumer per group can read from a single partition. If there are 10 partitions and 20 workers, 10 workers will sit idle. Hence, partition count must be greater than or equal to the maximum planned worker parallelism (e.g. 32 partitions for 20 workers).',
        tip: 'Mention "Consumer Group Partition Assignment Limits".'
      }
    ],
    designPatterns: ['Strategy Pattern', 'Producer-Consumer Pattern', 'Data Transfer Object (DTO)']
  },
  {
    id: 'week2-windowing-aggregations',
    title: 'Week 2: 5-Minute Windowing Mathematics & Watermark Engine',
    week: 2,
    roleFocus: 'Tumbling/Hopping Windows, Watermarking & Rolling Average Accumulator',
    summary: 'Building high-throughput 5-minute rolling averages for 50,000 trucks with out-of-order event tolerance.',
    deepDive: `Windowing is the heart of Member 1's responsibilities.

1. **Tumbling vs. Hopping Windows**:
   - **Tumbling Window (5 min)**: Fixed, non-overlapping intervals: \`[12:00-12:05)\`, \`[12:05-12:10)\`. Each reading belongs to exactly 1 window.
   - **Hopping Window (5 min window, 1 min slide)**: Overlapping intervals: \`[12:00-12:05)\`, \`[12:01-12:06)\`. A reading belongs to 5 overlapping windows simultaneously.

2. **Event-Time vs. Processing-Time**:
   - **Processing Time**: The clock time on the Python worker machine when the event arrives. Prone to skew when network lags.
   - **Event Time**: The sensor timestamp generated inside the truck. Required for true historical accuracy.

3. **Watermarks & Late Data Handling**:
   - A Watermark \`W(t) = MaxEventTime - AllowedLateness\`. When \`W(t) >= WindowEnd\`, the window is mathematically sealed and its 5-minute average is emitted downstream.
   - Readings arriving with \`timestamp < CurrentWatermark\` are handled via side-output or late-update triggers without crashing the engine.

4. **Welford's Algorithm for O(1) Memory Aggregations**:
   - Rather than storing thousands of raw float readings in memory, Member 1 uses incremental accumulators: \`count\`, \`sum\`, \`min\`, \`max\`, and Welford's variance algorithm, using only ~48 bytes of RAM per truck window!`,
    codeSnippets: [
      {
        title: 'Incremental O(1) Memory Rolling Average Accumulator',
        lang: 'python',
        code: `class TemperatureAccumulator:
    __slots__ = ("count", "sum_temp", "min_temp", "max_temp")
    
    def __init__(self):
        self.count = 0
        self.sum_temp = 0.0
        self.min_temp = float("inf")
        self.max_temp = float("-inf")

    def add(self, temp: float) -> None:
        self.count += 1
        self.sum_temp += temp
        if temp < self.min_temp: self.min_temp = temp
        if temp > self.max_temp: self.max_temp = temp

    @property
    def average(self) -> float:
        return self.sum_temp / self.count if self.count else 0.0`
      }
    ],
    vivaQuestions: [
      {
        q: 'How does your engine handle out-of-order data or trucks that go through tunnels with no cellular signal?',
        a: 'We use Bounded Out-of-Order Watermarking. The watermark lags behind the maximum observed event-time by a configurable latency budget (e.g. 15-30 seconds). If delayed data arrives within this budget, it is included in the window before sealing. If it arrives after the watermark has passed, it is directed to a Dead Letter / Late-Data side output.',
        tip: 'Emphasize "Deterministic Event-Time Processing over Processing-Time".'
      },
      {
        q: 'Why not simply keep a list of all raw sensor temperatures in Python lists for 5 minutes?',
        a: 'Storing 50,000 trucks * 30 readings/window = 1.5 million float objects in Python lists creates severe garbage collection pauses (GC pressure) and memory bloat. By using Welford incremental accumulators with __slots__, we achieve constant O(1) memory per truck and zero GC overhead.',
        tip: 'Mention "Python Memory Optimization with __slots__ and GC Avoidance".'
      }
    ],
    designPatterns: ['Accumulator Pattern', 'Template Method', 'Strategy Pattern (Windowing)']
  },
  {
    id: 'week3-rocksdb-state-recovery',
    title: 'Week 3: Embedded RocksDB State Store & Chaos Recovery',
    week: 3,
    roleFocus: 'RocksDB LSM-Tree, Write-Ahead-Log, Kafka Changelog Replication & Failover',
    summary: 'Ensuring zero data loss when Worker #4 crashes: seamless partition rebalance and state restoration in Worker #5.',
    deepDive: `State management separates amateur stream processors from industrial-grade engines.

1. **Why RocksDB (LSM-Tree) Embedded in Python?**:
   - RocksDB is an embedded C++ key-value engine that runs in the same process space as the Python worker (via \`pyrocksdb\` or CFFI bindings).
   - Unlike Redis or Postgres, reads and writes require **NO network round-trip** (sub-microsecond memory lookups).
   - Writes first hit the **MemTable** (in-memory skiplist) and **Write-Ahead Log (WAL)** sequentially. When full, MemTables are flushed to Level-0 **SSTables (Sorted String Tables)** on disk.

2. **Changelog Topic Pattern for Disaster Recovery**:
   - Every mutation in RocksDB (\`put\`, \`delete\`, \`merge\`) publishes a record to an internal Kafka compacted topic: \`streamforge.truck_state.changelog\`.
   - When **Worker #4 is killed mid-calculation**:
     1. Kafka Consumer Group detects heartbeat loss (e.g., 3000ms timeout).
     2. Cooperative Sticky Rebalancer assigns Worker #4's partitions (e.g., Partition 3 and 7) to **Worker #5**.
     3. Worker #5 spins up a fresh RocksDB instance for Partitions 3 and 7.
     4. Worker #5 replays the Changelog topic from offset 0 up to the committed snapshot.
     5. The 5-minute rolling averages resume with **100% exact state and ZERO missing sensor readings**!`,
    codeSnippets: [
      {
        title: 'State Replication & Replay Cycle',
        lang: 'python',
        code: `# On state change:
rocksdb_store.put(state_key, accumulator.to_dict())
changelog_mgr.publish_state_change(partition=7, key=state_key, value=accumulator.to_dict())

# On worker failure & rebalance:
restored_records = changelog_mgr.restore_partition_state(partition=7, target_store=new_worker_rocksdb)`
      }
    ],
    vivaQuestions: [
      {
        q: 'Explain the lifecycle of a write in RocksDB from Python.',
        a: 'When put(key, value) is called, RocksDB first appends the write to the Write-Ahead Log (WAL) on disk for durability, then inserts it into the active in-memory MemTable (a concurrent skiplist). When the MemTable reaches write_buffer_size (e.g. 64MB), it becomes an Immutable MemTable and a background thread flushes it to a Level-0 SSTable on disk.',
        tip: 'Draw the LSM-Tree write path: Client -> WAL + MemTable -> Immutable MemTable -> Flush to SSTable L0 -> Compaction to L1/L2.'
      },
      {
        q: 'How does StreamForge guarantee Exactly-Once Processing (EOS) during a worker crash?',
        a: 'We use the Kafka Transactional API combined with RocksDB Checkpoints. The state changelog offsets, output topic emissions, and input partition consumer offsets are committed atomically in a single two-phase commit transaction. If a worker crashes mid-batch, the uncommitted transaction aborts, and the recovering worker resumes from the last committed transaction offset.',
        tip: 'Mention "Two-Phase Commit (2PC) and Atomic Offset & State Commits".'
      }
    ],
    designPatterns: ['Write-Ahead Log (WAL)', 'State Pattern', 'Compacted Changelog', 'Observer Pattern']
  },
  {
    id: 'week4-prometheus-performance',
    title: 'Week 4: 100k Events/sec Benchmark & Prometheus Telemetry',
    week: 4,
    roleFocus: 'Industrial Auditing, Performance Profiling & Production Metrics Export',
    summary: 'Benchmarking the Python streaming engine to 100,000 events/second and exposing Prometheus metrics.',
    deepDive: `Member 1's final deliverable proves industrial enterprise readiness.

1. **Achieving 100,000 Events/sec in Python**:
   - Python's GIL (Global Interpreter Lock) can be a bottleneck if running in a single process.
   - StreamForge spins up **20 independent OS processes** (1 worker per CPU core or container).
   - Fast C-extensions (\`confluent-kafka\`, \`rocksdb\`, \`orjson\`) execute serialization and storage operations outside the Python GIL.
   - Batching incoming Kafka messages (e.g. \`poll(max_records=5000)\`) amortizes Python loop overhead.

2. **Prometheus Metrics Standard**:
   - \`streamforge_events_processed_total\`: Monotonic counter of ingested events.
   - \`streamforge_window_calculation_latency_seconds\`: Histogram tracking p50 (<0.8ms), p95 (<1.2ms), and p99 (<2.1ms).
   - \`streamforge_partition_lag\`: Real-time gauge of unprocessed messages per partition.
   - \`streamforge_rocksdb_memtable_bytes\`: In-memory footprint of active state stores.`,
    codeSnippets: [
      {
        title: 'Prometheus Metrics Setup in Python',
        lang: 'python',
        code: `from prometheus_client import Counter, Histogram, Gauge

PROCESSED_EVENTS = Counter('streamforge_events_total', 'Total processed IoT events')
CALCULATION_LATENCY = Histogram('streamforge_latency_seconds', '5-min window calc latency')
PARTITION_LAG = Gauge('streamforge_partition_lag', 'Kafka consumer lag', ['partition_id'])`
      }
    ],
    vivaQuestions: [
      {
        q: 'How does StreamForge compare against Apache Flink or Apache Spark Streaming?',
        a: 'Flink and Spark are heavyweight JVM ecosystems requiring JVM tuning, complex cluster managers (YARN/Kubernetes), and cross-language PySpark overhead. StreamForge provides a pure Python native developer experience with zero JVM overhead, direct sub-millisecond RocksDB access, lightweight container footprint (under 150MB RAM per worker), and seamless Python ML library integration (NumPy, PyTorch) directly in the streaming loop.',
        tip: 'Emphasize "Native Python Data Science Ergonomics + Zero JVM Overhead".'
      },
      {
        q: 'What was your specific contribution as Member 1?',
        a: 'I architected and engineered the Core Stream Processing & Stateful Engine. Specifically: 1) Designed the O(1) incremental 5-minute rolling average accumulator, 2) Implemented the Bounded Out-of-Order Watermark generator, 3) Built the embedded RocksDB state store with WAL and changelog replication, and 4) Engineered the automatic partition failover recovery protocol ensuring zero data loss during worker crashes.',
        tip: 'Deliver this with confidence during team presentations!'
      }
    ],
    designPatterns: ['Facade Pattern', 'Observer Pattern', 'Registry Pattern']
  }
];
