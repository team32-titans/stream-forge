# StreamForge Benchmark — Honest Results

**Date:** 2026-09-04
**Command:** `python -m streamforge.benchmark.throughput` / `benchmark_in_memory(100000)`
**Hardware:**
- OS: Windows-10 10.0.26200
- CPU: Intel64 Family 6 Model 197 GenuineIntel
- Python: 3.11.9 MSC v1938 64-bit
- RAM: Windows host, Docker not available for worker scaling
- Kafka: not running for this baseline (in-memory engine only)

## In-Memory Engine (no Kafka, no RocksDB real)

Wraps `FleetTelemetryGenerator` -> `WindowedRollingAverageProcessor` -> `RocksDBStateStore(test)` directly.

| Metric | Value |
|--------|-------|
| total_events | 100,000 |
| elapsed_s | 21.643 |
| events_per_sec | 4,620 |
| p50_ms | 0.0005 |
| p95_ms | 1.2423 |
| p99_ms | 1.663 |

**Smaller run (20,000):** ~19,980 evt/s, p50 0.0003 p95 0.205 p99 0.408 — shows overhead scales with window emissions.

## Kafka Produce Baseline

`KafkaTelemetryProducer.produce_batch` requires broker at `localhost:9092`. With broker unavailable, `benchmark_kafka` falls back to in-memory. No Kafka throughput claimed.

## 100k events/sec Target

**Not achieved on this hardware.** The in-memory baseline on a single Windows Python process (~4.6k evt/s for full aggregation) is far below 100k. Achieving 100k requires:
- Linux Docker workers (real RocksDB, not in-memory dict)
- `confluent-kafka` batching/lz4 across 32 partitions
- 20 worker containers in parallel (each ~5k evt/s → ~100k aggregate theoretical)
- Broker with sufficient partitions and compaction tuning

**Limitation documented.** No fictitious 100k claim is made.

## How to Reproduce with Kafka + 20 Workers

```powershell
docker compose -f infra/kafka/docker-compose.yml up -d
python -m streamforge.infra.kafka_admin create
docker compose -f infra/docker-compose.workers.yml up --scale worker=20 -d
python -m streamforge.cli benchmark --events 100000
# Check Prometheus
curl http://localhost:8000/metrics
# Check logs
docker compose -f infra/docker-compose.workers.yml logs worker
```

Record `docker compose ps`, `kafka-topics --describe`, `/metrics`, and worker logs as evidence.
