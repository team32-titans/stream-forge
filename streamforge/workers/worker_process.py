"""
Docker worker process — partition-aware RocksDB + changelog + consumer loop.
Each container runs this module: python -m streamforge.workers.worker_process
"""
from __future__ import annotations

import json
import logging
import os
import signal
import sys
import time
from typing import Dict, List

from streamforge.config import get_settings
from streamforge.core.interfaces import TruckTelemetryEvent
from streamforge.metrics.exporter import get_exporter
from streamforge.state.changelog_manager import ChangelogManager
from streamforge.state.rocksdb_store import RocksDBStateStore
from streamforge.windowing.engine import WindowedRollingAverageProcessor
from streamforge.workers.consumer import StreamConsumer

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
logger = logging.getLogger("streamforge.worker")

# Global state per worker
running = True
stores: Dict[int, RocksDBStateStore] = {}
processors: Dict[int, WindowedRollingAverageProcessor] = {}
changelog: ChangelogManager | None = None
exporter = get_exporter()
late_events = 0


def _handle_signal(signum, frame):
    global running
    logger.info(f"Signal {signum} received, shutting down")
    running = False


def on_assign(partitions):
    """Open RocksDB for newly assigned partitions, replay changelog."""
    s = get_settings()
    worker_id = s.worker_id
    for p in partitions:
        pid = p.partition
        if pid in stores:
            continue
        db_path = os.path.join(s.rocksdb_base_path, f"p{pid:02d}")
        try:
            store = RocksDBStateStore(db_path=db_path, partition_id=pid, storage_mode=s.storage_mode)
            stores[pid] = store
            proc = WindowedRollingAverageProcessor(
                worker_id=worker_id, window_size_ms=s.window_size_ms, max_lateness_ms=s.max_lateness_ms
            )
            processors[pid] = proc
            # Replay changelog
            assert changelog is not None
            changelog.restore_partition_state(pid, store, use_kafka_replay=(s.storage_mode == "production"))
            _restore_processor_state(proc, store)
            try:
                restored_active = proc.restore_from_store(store)
                logger.info(f"[{worker_id}] Active-window scan restored {restored_active} window(s) p={pid}")
            except Exception as e:
                logger.warning(f"restore_from_store failed p={pid}: {e}")
            exporter.gauges["streamforge_recovery_events_total"] = exporter.gauges.get("streamforge_recovery_events_total", 0) + 1
            logger.info(f"[{worker_id}] Recovery done for partition {pid}")
        except Exception as e:
            logger.error(f"Failed to init partition {pid}: {e}")


def on_revoke(partitions):
    """Flush and close RocksDB for revoked partitions."""
    for p in partitions:
        pid = p.partition
        store = stores.pop(pid, None)
        processors.pop(pid, None)
        if store:
            try:
                store.close()
                logger.info(f"Partition {pid} store closed on revoke")
            except Exception as e:
                logger.error(f"Close failed p={pid}: {e}")


def _get_real_lag(consumer, s) -> int:
    """Fetch real consumer lag via position vs high watermark (best effort).

    Returns total lag across assigned partitions, or -1 when the lag cannot
    be determined (broker unreachable / no assignment). Never fabricates 0.
    """
    try:
        # Use AdminClient to get high watermark is heavy; use consumer position if available
        # Fallback to 0 if not determinable
        from confluent_kafka import TopicPartition

        parts = consumer._consumer.assignment() if hasattr(consumer, "_consumer") else []
        if not parts:
            return -1
        total_lag = 0
        determined = False
        for tp in parts:
            try:
                low, high = consumer._consumer.get_watermark_offsets(tp, timeout=1)
                pos = consumer._consumer.position([tp])
                ppos = pos[0].offset if pos and pos[0].offset >= 0 else high
                total_lag += max(0, high - ppos)
                determined = True
            except Exception:
                continue
        return total_lag if determined else -1
    except Exception:
        return -1


def _persist_active_state(proc, store, changelog, worker_id: str, pid: int, source_offset: int) -> bool:
    """Durably persist active (unemitted) accumulators + watermark.

    Called after every accepted event so a replacement worker assigned this
    partition can restore mid-window state via on_assign replay. Returns
    False on any RocksDB/changelog failure so the caller blocks the commit.
    """
    try:
        snap = proc.snapshot_state()
    except Exception as e:
        logger.error(f"Active snapshot failed p={pid}: {e}")
        return False
    ts_ms = int(time.time() * 1000)
    try:
        for key, entry in snap.get("active_windows", {}).items():
            state_key = f"__active__:{key}"
            val = {
                "truck_id": entry["truck_id"],
                "window_start": entry["window_start"],
                "acc": entry["acc"],
                "active": True,
                "seq": source_offset,
                "source_offset": source_offset,
            }
            store.put(state_key, val)
            changelog.publish_state_change(
                partition=pid, key=state_key, value=val,
                worker_id=worker_id, timestamp=ts_ms, source_offset=source_offset,
            )
        wm = snap.get("watermark", {})
        wm_val = {
            "current_max_timestamp": wm.get("current_max_timestamp", 0),
            "last_emitted_watermark": wm.get("last_emitted_watermark", 0),
            "max_lateness_ms": wm.get("max_lateness_ms", 0),
            "seq": source_offset,
            "source_offset": source_offset,
        }
        store.put("__watermark__", wm_val)
        changelog.publish_state_change(
            partition=pid, key="__watermark__", value=wm_val,
            worker_id=worker_id, timestamp=ts_ms, source_offset=source_offset,
        )
        return True
    except Exception as e:
        logger.error(f"Active state persist failed p={pid}: {e}")
        return False


def _restore_processor_state(proc, store) -> None:
    """Rebuild in-memory accumulators + watermark from durable store keys."""
    try:
        wm = store.get("__watermark__")
    except Exception:
        wm = None
    active: dict = {}
    try:
        for k, v in store.scan(prefix="__active__:"):
            inner = k[len("__active__:"):]
            if isinstance(v, dict) and "acc" in v:
                active[inner] = {
                    "truck_id": v.get("truck_id", inner.split(":")[0] if ":" in inner else ""),
                    "window_start": v.get("window_start"),
                    "acc": v["acc"],
                }
    except Exception as e:
        logger.warning(f"Active scan failed during restore: {e}")
    snap = {"active_windows": active, "watermark": wm or {}}
    try:
        proc.restore_state(snap)
    except Exception as e:
        logger.error(f"Processor restore failed: {e}")


def main():
    global changelog, running
    s = get_settings()
    # Worker ID from hostname — Docker Compose generates unique container IDs;
    # HOSTNAME is unique per container, use it directly without relying on Swarm templating.
    hn = os.environ.get("HOSTNAME") or os.environ.get("HOST") or ""
    if s.worker_id == "worker-01" and hn and hn != "localhost":
        os.environ["WORKER_ID"] = hn
        from streamforge.config import reload_settings

        s = reload_settings()

    worker_id = s.worker_id
    logger.info(f"Starting worker {worker_id} storage_mode={s.storage_mode} bootstrap={s.kafka_bootstrap_servers}")
    exporter.gauges["streamforge_worker_up"] = 1
    changelog = ChangelogManager(
        changelog_topic=s.kafka_changelog_topic,
        bootstrap_servers=s.kafka_bootstrap_servers,
        storage_mode=s.storage_mode,
    )

    signal.signal(signal.SIGTERM, _handle_signal)
    signal.signal(signal.SIGINT, _handle_signal)

    consumer = StreamConsumer(worker_id=worker_id, on_assign=on_assign, on_revoke=on_revoke)
    consumer.subscribe()

    # Metrics: throughput tracking
    last_report = time.time()
    events_in_interval = 0

    try:
        while running:
            msg = consumer.poll(1.0)
            if msg is None:
                # Periodic throughput report
                now = time.time()
                if now - last_report >= 5:
                    elapsed = now - last_report
                    rate = events_in_interval / elapsed if elapsed > 0 else 0
                    exporter.set_throughput(rate)
                    logger.info(f"[{worker_id}] throughput {rate:.0f} evt/s, partitions={list(stores.keys())}")
                    events_in_interval = 0
                    last_report = now
                continue
            if msg.error():
                logger.error(f"Consumer error: {msg.error()}")
                exporter.counters["streamforge_events_failed_total"] += 1
                continue

            try:
                data = json.loads(msg.value().decode("utf-8"))
                evt = TruckTelemetryEvent(**data)
                # Ensure partition matches message partition if not set
                if evt.partition != msg.partition():
                    evt.partition = msg.partition()
            except Exception as e:
                logger.error(f"Deserialize failed: {e}")
                exporter.counters["streamforge_events_failed_total"] += 1
                continue

            pid = msg.partition()
            proc = processors.get(pid)
            store = stores.get(pid)
            if proc is None or store is None:
                # Not yet assigned? shouldn't happen — create on demand for demo
                if s.storage_mode != "production":
                    # Demo mode: create lazily
                    db_path = os.path.join(s.rocksdb_base_path, f"p{pid:02d}")
                    store = stores.setdefault(pid, RocksDBStateStore(db_path=db_path, partition_id=pid, storage_mode=s.storage_mode))
                    proc = processors.setdefault(
                        pid,
                        WindowedRollingAverageProcessor(worker_id=worker_id, window_size_ms=s.window_size_ms, max_lateness_ms=s.max_lateness_ms),
                    )
                else:
                    logger.warning(f"No processor for partition {pid}, skipping")
                    continue

            # Filter is inside proc.process_event (T>0)
            t0 = time.perf_counter()
            results = proc.process_event(evt)
            latency_ms = (time.perf_counter() - t0) * 1000
            exporter.observe_latency(latency_ms / 1000.0)

            # Filtered events (T<=0): no state, no watermark, no changelog —
            # but the offset can be committed (invalid data is skipped, not retried).
            if evt.temperature is not None and float(evt.temperature) <= 0:
                exporter.counters["streamforge_events_filtered_total"] = exporter.counters.get("streamforge_events_filtered_total", 0) + 1
                exporter.counters["streamforge_events_processed_total"] += 1
                try:
                    consumer.commit(msg)
                except Exception:
                    pass
                continue

            # If event was late, results empty — count late
            if evt.is_late:
                global late_events
                late_events += 1
                exporter.counters["streamforge_late_events_total"] += 1
                exporter.counters["streamforge_events_processed_total"] += 1
                # Commit offset even for late (side_output)
                try:
                    consumer.commit(msg)
                except Exception:
                    pass
                continue

            changelog_ok = True
            # Durably persist ACTIVE (unemitted) accumulator state every event
            # so a replacement worker can continue mid-window after a crash.
            if not _persist_active_state(proc, store, changelog, worker_id, pid, msg.offset()):
                exporter.counters["streamforge_events_failed_total"] += 1
                changelog_ok = False
            if changelog_ok and results:
                for res in results:
                    key = f"{res.truck_id}:{res.window_start}"
                    val = res.model_dump(mode="json")
                    # Durable version = Kafka source offset (see changelog_manager protocol)
                    val["seq"] = msg.offset()
                    val["source_offset"] = msg.offset()
                    try:
                        store.put(key, val)
                    except Exception as e:
                        logger.error(f"RocksDB put failed p={pid} key={key}: {e}")
                        exporter.counters["streamforge_events_failed_total"] += 1
                        changelog_ok = False
                        break
                    try:
                        changelog.publish_state_change(
                            partition=pid,
                            key=key,
                            value=val,
                            worker_id=worker_id,
                            timestamp=int(time.time() * 1000),
                            source_offset=msg.offset(),
                        )
                    except Exception as e:
                        logger.error(f"Changelog publish failed p={pid} key={key}: {e}")
                        exporter.counters["streamforge_changelog_failures_total"] = exporter.counters.get("streamforge_changelog_failures_total", 0) + 1
                        changelog_ok = False
                        break
                    exporter.counters["streamforge_window_updates_total"] += 1

            # Crash consistency: commit only if RocksDB + changelog ack succeeded.
            # In production the flush verifies delivery callbacks; in demo/test
            # the in-memory flush trivially succeeds.
            if changelog_ok:
                flushed = changelog.flush(timeout=5)
                if not flushed:
                    logger.error("Changelog flush failed — NOT committing source offset, will redeliver")
                    exporter.counters["streamforge_changelog_failures_total"] = exporter.counters.get("streamforge_changelog_failures_total", 0) + 1
                    changelog_ok = False
                if changelog_ok:
                    try:
                        consumer.commit(msg)
                    except Exception as e:
                        logger.error(f"Commit failed: {e}")
                else:
                    logger.warning(f"Skipping commit for offset {msg.offset()} p={pid} due to changelog failure")
            else:
                logger.warning(f"Skipping commit for offset {msg.offset()} p={pid} due to earlier failure")

            exporter.counters["streamforge_events_processed_total"] += 1
            exporter.counters["streamforge_partition_events_total"] = exporter.counters.get("streamforge_partition_events_total", 0) + 1
            events_in_interval += 1
            # Real lag every 5s (avoid per-message overhead); -1 = unavailable
            now_lag = time.time()
            if now_lag - last_report >= 5 or events_in_interval % 100 == 0:
                try:
                    lag = _get_real_lag(consumer, s)
                    exporter.gauges["streamforge_consumer_lag"] = lag
                    exporter.set_lag(int(lag))
                except Exception:
                    exporter.gauges["streamforge_consumer_lag"] = -1

            # Heartbeat throughput every 5s
            now = time.time()
            if now - last_report >= 5:
                elapsed = now - last_report
                rate = events_in_interval / elapsed if elapsed > 0 else 0
                exporter.set_throughput(rate)
                last_report = now
                events_in_interval = 0
    finally:
        logger.info("Shutting down worker")
        for store in list(stores.values()):
            try:
                store.close()
            except Exception:
                pass
        if changelog:
            changelog.flush()
        consumer.close()
        exporter.gauges["streamforge_worker_up"] = 0


if __name__ == "__main__":
    main()
