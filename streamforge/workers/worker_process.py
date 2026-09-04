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


def main():
    global changelog, running
    s = get_settings()
    # Worker ID from hostname if not set
    if s.worker_id == "worker-01" and os.environ.get("HOSTNAME"):
        # Docker hostname
        hn = os.environ["HOSTNAME"]
        # Use hostname as worker_id if looks like worker
        if "worker" in hn:
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

            if results:
                for res in results:
                    key = f"{res.truck_id}:{res.window_start}"
                    val = res.model_dump(mode="json")
                    val["seq"] = val.get("seq", 1)
                    # Enrich with seq/source_offset for protocol
                    store.put(key, val)
                    # Changelog produce — source_offset from msg.offset()
                    changelog.publish_state_change(
                        partition=pid,
                        key=key,
                        value=val,
                        worker_id=worker_id,
                        timestamp=int(time.time() * 1000),
                        source_offset=msg.offset(),
                    )
                    exporter.counters["streamforge_window_updates_total"] += 1

            # Commit only after state + changelog ack (flush batch)
            if s.storage_mode == "production":
                # Batch flush handled by manager; commit now
                changelog.flush(0.1)

            try:
                consumer.commit(msg)
            except Exception as e:
                logger.error(f"Commit failed: {e}")

            exporter.counters["streamforge_events_processed_total"] += 1
            exporter.counters["streamforge_partition_events_total"] = exporter.counters.get("streamforge_partition_events_total", 0) + 1
            events_in_interval += 1
            exporter.gauges["streamforge_consumer_lag"] = 0  # TODO: fetch real lag via AdminClient

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
