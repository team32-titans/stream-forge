"""
Honest throughput benchmark — real Kafka if available, else in-memory baseline.
Records events/sec, p50/p95/p99, hardware. Does NOT fake latencies.
"""
from __future__ import annotations

import time
import statistics

from streamforge.config import get_settings


def benchmark_in_memory(total_events: int = 100_000) -> dict:
    from streamforge.producers.truck_telemetry import FleetTelemetryGenerator
    from streamforge.windowing.engine import WindowedRollingAverageProcessor
    from streamforge.state.rocksdb_store import RocksDBStateStore

    s = get_settings()
    gen = FleetTelemetryGenerator(fleet_size=s.fleet_size, num_partitions=s.kafka_partitions)
    proc = WindowedRollingAverageProcessor(worker_id="benchmark", window_size_ms=s.window_size_ms, max_lateness_ms=s.max_lateness_ms)
    store = RocksDBStateStore(db_path="/tmp/streamforge_benchmark", partition_id=0, storage_mode="test")

    latencies = []
    t0 = time.time()
    n = 0
    batch = 5000
    for _ in range(total_events // batch):
        events = gen.stream_batch(batch_size=batch)
        for evt in events:
            ts = time.perf_counter()
            res = proc.process_event(evt)
            latencies.append((time.perf_counter() - ts) * 1000)
            if res:
                for r in res:
                    store.put(f"{r.truck_id}:{r.window_start}", r.model_dump(mode="json"))
            n += 1
    elapsed = time.time() - t0
    latencies.sort()
    def pct(p): return latencies[int(len(latencies) * p)] if latencies else 0
    store.close()
    return {
        "mode": "in_memory",
        "total_events": n,
        "elapsed_s": round(elapsed, 3),
        "events_per_sec": round(n / elapsed, 0) if elapsed > 0 else 0,
        "p50_ms": round(pct(0.5), 4),
        "p95_ms": round(pct(0.95), 4),
        "p99_ms": round(pct(0.99), 4),
        "note": "In-memory baseline (no Kafka). For real Kafka throughput, run with broker up and see docs/STATE_CHANGELOG_PROTOCOL.md",
    }


def benchmark_kafka(total_events: int = 100_000) -> dict:
    """Produce to real Kafka, consume and measure. Falls back to in_memory if broker unavailable."""
    try:
        from streamforge.producers.kafka_producer import KafkaTelemetryProducer

        prod = KafkaTelemetryProducer()
        # Quick check if producer initialized
        if prod._producer is None:
            raise RuntimeError("producer not ready")
        t0 = time.time()
        produced = 0
        batch = 5000
        for _ in range(total_events // batch):
            produced += prod.produce_batch(batch_size=batch)
        prod.flush(10)
        elapsed = time.time() - t0
        return {
            "mode": "kafka_produce",
            "total_events": produced,
            "elapsed_s": round(elapsed, 3),
            "events_per_sec": round(produced / elapsed, 0) if elapsed > 0 else 0,
            "note": "Kafka produce only; consume throughput requires 20 workers up",
        }
    except Exception as e:
        return {"mode": "kafka_unavailable", "error": str(e), "fallback": benchmark_in_memory(total_events)}
