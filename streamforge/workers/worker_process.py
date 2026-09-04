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
    """Fetch real consumer lag via position vs high watermark (best effort)."""
    try:
        # Use AdminClient to get high watermark is heavy; use consumer position if available
        # Fallback to 0 if not determinable
        from confluent_kafka import TopicPartition

        parts = consumer._consumer.assignment() if hasattr(consumer, "_consumer") else []
        total_lag = 0
        for tp in parts:
            try:
                low, high = consumer._consumer.get_watermark_offsets(tp, timeout=1)
                pos = consumer._consumer.position([tp])
                ppos = pos[0].offset if pos and pos[0].offset >= 0 else high
                total_lag += max(0, high - ppos)
            except Exception:
                continue
        return total_lag
    except Exception:
        return 0


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
    changelog = ChangelogManager()

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

            # If event was late or filtered, results empty — count late
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
            if results:
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

            # Crash consistency: commit only if RocksDB + changelog ack succeeded
            if changelog_ok:
                if s.storage_mode == "production":
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
            # Real lag every 5s (avoid per-message overhead)
            now_lag = time.time()
            if now_lag - last_report >= 5 or events_in_interval % 100 == 0:
                try:
                    exporter.gauges["streamforge_consumer_lag"] = _get_real_lag(consumer, s)
                    exporter.set_lag(int(exporter.gauges["streamforge_consumer_lag"]))
                except Exception:
                    pass

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
