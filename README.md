# StreamForge — Distributed Python Event Processor (Project 2)

> **Distributed stateful streaming for 50,000 cold-chain trucks — 32 Kafka partitions, 5-min event-time windows, RocksDB, changelog recovery, 20 scalable worker containers.**

[![Python](https://img.shields.io/badge/Python-3.11-3776AB?style=for-the-badge&logo=python)](#) [![Kafka](https://img.shields.io/badge/Kafka-3.7-231F20?style=for-the-badge&logo=apachekafka)](#) [![RocksDB](https://img.shields.io/badge/RocksDB-rocksdict-8B0000?style=for-the-badge)](#) [![FastAPI](https://img.shields.io/badge/FastAPI-0.141-009688?style=for-the-badge)](#) [![React](https://img.shields.io/badge/React-19-61DAFB?style=for-the-badge&logo=react)](#)

## Contents

- [Project Overview](#1-project-overview)
- [Architecture](#2-architecture)
- [Quick Start](#6-quick-start)
- [Kafka Topics](#7-kafka-topics)
- [State & Changelog Protocol](#10-state--changelog-protocol)
- [Tests](#15-tests)
- [Benchmark](#16-benchmark)
- [Limitations](#17-chaos--limitations)

## 1. Project Overview

StreamForge is Project 2 — a distributed Python event processor that:

- Generates mock IoT truck telemetry (50k fleet, temperature, GPS, refrigeration state)
- Publishes to Kafka `fleet-telemetry` (32 partitions, key=`truck_id` via CRC32)
- Consumes with 20 scalable worker containers (Kafka consumer group `streamforge-workers`, cooperative-sticky)
- Processes: `Consume → Filter T>0 → Map → Event-Time 5-min tumbling window → Aggregation (count/sum/avg/min/max + Welford stddev)`
- Watermark `W = max_event_time - MAX_LATENESS_MS` (default 15s, configurable) with `LATE_EVENT_POLICY=side_output|drop`
- Persists window state in RocksDB (`rocksdict` in Docker, ephemeral tmpfs per worker) and mirrors to Kafka compacted changelog `streamforge.truck_state.changelog` (32 partitions)
- Recovers via changelog replay (durable version = Kafka source offset)
- Exposes Prometheus `/metrics`, FastAPI control plane, WebSocket live metrics, React dashboard with explicit DEMO vs LIVE modes

**Honest semantics:** `at-least-once` Kafka delivery + `idempotent` state application (version = source offset) + durable changelog = **effectively-once state processing**. Exactly-once, zero loss, RPO/RTO not claimed until measured. “20 worker containers” (not 20 physical nodes). 100k evt/s target honestly audited (see Benchmark).

## 2. Architecture

```
50k IoT Vehicles (FleetTelemetryGenerator, CRC32 truck_id -> partition)
        | key=truck_id, value=JSON, acks=all, idempotent, lz4
        v
Kafka KRaft (infra/kafka/docker-compose.yml)
  fleet-telemetry 32p  +  streamforge.truck_state.changelog 32p compacted
        | consumer group: streamforge-workers cooperative-sticky
        v
20 Worker Containers (infra/docker-compose.workers.yml, tmpfs /data/rocksdb)
  on_assign(p) -> open RocksDB p{pid} -> replay changelog from earliest
  on_revoke(p) -> flush/close
  pipeline: poll -> deserialize -> filter T>0 -> Watermark -> Window -> put RocksDB -> produce changelog (ack) -> commit offset
        | \ metrics -> Prometheus
        v
FastAPI (streamforge/api/main.py)  GET /api/health, /workers, /partitions, /windows/{truck_id}, /state/{p}, /changelog, /metrics, WS /ws/metrics
        |
React 18 Vite  LIVE fetches FastAPI/WS, DEMO uses simulationEngine.ts (?demo or VITE_DEMO_MODE=true) with banner
```

Config single source: `streamforge/config.py` (pydantic-settings, `.env`).

## 3. Technology Stack

- Python 3.11, `confluent-kafka` (librdkafka) **single Kafka client**, `pydantic`/`pydantic-settings`, `prometheus-client`, `fastapi`/`uvicorn`, `typer` CLI, `rocksdict` (Linux Docker only)
- Infra: Kafka 3.7 KRaft, Docker Compose, `infra/worker/Dockerfile` (fails if rocksdict missing)
- Frontend: React 18, Vite 6, TypeScript 5, Tailwind 4, `lucide-react`, `recharts`

## 4. Repository Structure

```
streamforge/
  config.py, cli.py
  core/interfaces.py
  producers/truck_telemetry.py (CRC32), kafka_producer.py
  windowing/engine.py (Welford, Watermark, WindowAssigner, WindowedRollingAverageProcessor)
  state/rocksdb_store.py (rocksdict prod / in-memory demo), changelog_manager.py (source_offset version)
  workers/consumer.py, worker_process.py
  metrics/exporter.py (real Counter/Gauge/Histogram)
  api/main.py, schemas.py
  benchmark/throughput.py
  infra/kafka_admin.py
infra/kafka/docker-compose.yml, infra/docker-compose.workers.yml, infra/worker/Dockerfile
src/lib/api.ts, hooks/useLiveMetrics.ts, engine/simulationEngine.ts (DEMO only), components/*
docs/STATE_CHANGELOG_PROTOCOL.md, docs/BENCHMARK.md
```

## 5. Prerequisites

- Python 3.11, Node 18+, Docker (Linux for RocksDB/20 workers), `py -m venv .venv` on Windows
- Linux host or WSL2 for production RocksDB/20 containers (Windows dev uses `STORAGE_MODE=demo`)

## 6. Quick Start

### Frontend demo (no Kafka required)

The dashboard can run independently with simulated metrics. This is the fastest way to explore the topology, windowing, chaos, and metrics views:

```powershell
npm install
npm run dev
```

Open <http://localhost:3000/?demo>. The yellow DEMO banner confirms that the dashboard is using `simulationEngine.ts`, not a live Kafka cluster.

### Python env (Windows PowerShell)
```powershell
py -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
python -m pytest -q  # 16 passed
```

### Env
```powershell
Copy-Item .env.example .env  # edit KAFKA_BOOTSTRAP_SERVERS etc.
```

### Kafka (requires Docker)
```powershell
docker compose -f infra/kafka/docker-compose.yml up -d
python -m streamforge.infra.kafka_admin create
python -m streamforge.infra.kafka_admin describe  # verify 32 partitions + compacted changelog
```

### Workers (requires Docker)
```powershell
docker compose -f infra/docker-compose.workers.yml up --scale worker=20 -d
docker compose -f infra/docker-compose.workers.yml ps  # 20 containers, unique HOSTNAME as worker ID
```

### FastAPI + Frontend
```powershell
python -m streamforge.cli api  # :8000, /api/health /metrics WS /ws/metrics
npm run dev  # Vite :3000 proxy /api -> :8000, ?demo toggles simulation
```

Without `?demo`, the dashboard connects to FastAPI through the Vite proxy. Set `VITE_DEMO_MODE=true` when you want demo mode to be the default for local development.

### CLI
```powershell
python -m streamforge.cli live --workers 20
python -m streamforge.cli benchmark --events 100000
python -m streamforge.cli topics describe
python -m streamforge.cli test
```

## 7. Kafka Topics

- `fleet-telemetry` 32 partitions, `replication.factor=1` (single broker; production would be 3)
- `streamforge.truck_state.changelog` 32 partitions, `cleanup.policy=compact`
- Producer: `acks=all`, `enable.idempotence=true`, `compression.type=lz4`, `key=truck_id`
- Partition: `zlib.crc32(truck_id) % 32` deterministic (previous `hash()` fixed)

## 8. Event Pipeline

`TruckTelemetryEvent` (Pydantic) → Kafka → `StreamConsumer` (group `streamforge-workers`, `cooperative-sticky`, `auto.commit=false`) → `process_event` filters `temperature <=0` → `process_telemetry` checks `is_late` **before** watermark, `side_output` returns late signal without merging → `WindowAssigner` tumbling `[start, end)` `start = ts - ts%300_000` → `TemperatureAccumulator` (Welford `M2 += (x-old_mean)*(x-new_mean)`, `stddev` sample) → on watermark `W >= window_end` emit `WindowedAggregateResult`.

## 9. Watermark & Late Data

- `WatermarkGenerator: max_seen - MAX_LATENESS_MS (15s)`, monotonic `last_emitted`
- Late: `timestamp < last_watermark` → `is_late=True`, not merged, counted `streamforge_late_events_total`, routed side_output (or drop if `LATE_EVENT_POLICY=drop`)
- Window emits when `W >= end`; out-of-order within grace still merges

## 10. State & Changelog Protocol

See `docs/STATE_CHANGELOG_PROTOCOL.md`.

- State key: `"{truck_id}:{window_start}"`
- RocksDB value includes `seq = source_offset` (durable), `source_offset`
- Changelog key: `"{partition:02d}:{state_key}"` same partition as source for compaction
- Version invariant: `partition+state_key -> monotonic version = Kafka source offset`. Worker restart cannot reset; stale replay `seq <= existing.seq` skipped
- Recovery: `on_assign` opens `RocksDB(/data/rocksdb/pNN)` (ephemeral tmpfs) → `ChangelogManager.restore_partition_state` from `OFFSET_BEGINNING` (Kafka) or in-memory (demo) with idempotent check
- **Isolation:** No shared volume — each worker tmpfs, recovery via changelog, so concurrent mutation impossible during rebalance
- RPO/RTO: **not pre-claimed**; measure after chaos test via `docker stop worker` → time until `on_assign` completes and `/api/state` returns

## 11. Crash Consistency

- RocksDB fail → no commit
- Changelog publish/ack fail (`flush` false) → no commit
- Crash before commit → redelivery → idempotent seq check
- Commit only after `put + publish + flush ack`. Disable `auto.commit`.

## 12. Workers & Rebalancing

- Real `confluent-kafka` consumer group, `on_assign`/`on_revoke` lifecycle, per-partition RocksDB open/close
- 20 containers via `docker compose up --scale worker=20` (not Swarm templating; hostname is container HOSTNAME, unique)
- Docs say **20 scalable worker containers**, not 20 nodes

## 13. Prometheus & API

- `streamforge/metrics/exporter.py` real `Counter/Gauge/Histogram`, `generate_latest` on `/metrics`
- FastAPI `GET /api/health, /workers, /partitions, /telemetry, /windows/{id}, /state/{p}, /changelog, /metrics`, `POST /api/chaos/kill-worker/{id}`, `WS /ws/metrics` streaming counters/gauges
- Consumer lag is real via `get_watermark_offsets` vs `position` in `worker_process.py` (best effort; `-1` if unavailable)

## 14. React

- `src/lib/api.ts` `IS_DEMO = VITE_DEMO_MODE==="true" || ?demo`, `src/hooks/useLiveMetrics.ts` WS with poll fallback
- `src/App.tsx` banner DEMO (simulationEngine) vs LIVE (FastAPI), `TopologyView` labels `confluent-kafka + Custom Event-Time Engine`, `Effectively-Once*`
- Vite proxy `/api` → `:8000`

## 15. Tests

```powershell
python -m pytest -v  # 16 tests
# - config, producer affinity, Welford stddev+m2 persistence, filter T>0, watermark/late (on-time/within/beyond/out-of-order), changelog seq=source_offset, dup idempotent, stale newer overwrites, failure gating (no commit on changelog fail), crash recovery (partition 6)
```

Integration tests requiring Kafka/Docker are marked and skipped when broker unavailable.

## 16. Benchmark

See `docs/BENCHMARK.md`.

- In-memory 100k on Windows laptop (i7): **4,620 evt/s, p50 0.0005ms p95 1.24ms p99 1.66ms** (21.6s)
- 20k run: ~19,980 evt/s, p50 0.0003 p95 0.2 p99 0.4
- 100k target **not achieved** on this hardware; honest report. Kafka + 20 workers theoretical aggregate ~100k needs load test (see reproduce steps in docs).

## 17. Chaos & Limitations

- `POST /api/chaos/kill-worker` logs intent; real failure is `docker stop <worker>` → Kafka detects heartbeat miss → rebalance → `on_assign` replay
- **Limitations (honest):** Windows host has no Docker → 20 workers/Kafka/changelog not locally verified (compose files syntactically correct, worker image fails if rocksdict missing, architecture is correct); `consumer_lag` best-effort; RocksDB `sync_wal=false` trades durability; single broker replication=1 not HA; RPO/RTO not measured yet
- `simulationEngine.ts` remains DEMO only, never feeds LIVE

## 18. Exactly-Once Semantics

**Effectively-once** (at-least-once delivery + deterministic CRC32 partitioning + idempotent `seq=source_offset` + durable compacted changelog). Not exactly-once (would need transactions).

## 19. Security

- `.env` ignored, `.env.example` placeholders only, no secrets committed
- `infra/worker/Dockerfile` no longer masks rocksdict failure

## 20. License

Apache 2.0
