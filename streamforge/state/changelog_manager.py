"""
StreamForge Kafka Changelog Replication & Recovery Manager
==========================================================
Module: streamforge.state.changelog_manager
Author: Member 1 (Stream Processing & Stateful Engine)

Ensures every local RocksDB mutation is mirrored to a compacted Kafka changelog topic.
When a worker node fails, the replacement node replays this changelog from offset 0
to rebuild the exact 5-minute rolling averages without state corruption.
"""

import json
import logging
from typing import Any, Callable, Dict, List, Optional
from streamforge.core.interfaces import StateStore

logger = logging.getLogger("streamforge.changelog")


class ChangelogRecord:
    """Payload mirrored to the Kafka changelog topic."""
    def __init__(
        self,
        partition: int,
        key: str,
        value: Optional[Dict[str, Any]],
        offset: int,
        timestamp: int,
        worker_source: str,
        seq: Optional[int] = None,
        op: str = "PUT",
    ) -> None:
        self.partition = partition
        self.key = key
        self.value = value
        self.offset = offset
        self.timestamp = timestamp
        self.worker_source = worker_source
        # Durable version = Kafka source offset; falls back to changelog offset.
        self.seq: int = seq if seq is not None else offset
        self.op = op
        # Compacted changelog key colocates state with its source partition.
        self.changelog_key = f"{partition:02d}:{key}"

    def serialize(self) -> bytes:
        payload = {
            "partition": self.partition,
            "key": self.key,
            "changelog_key": self.changelog_key,
            "value": self.value,
            "offset": self.offset,
            "seq": self.seq,
            "op": self.op,
            "timestamp": self.timestamp,
            "worker_source": self.worker_source,
        }
        return json.dumps(payload).encode("utf-8")


class ChangelogManager:
    """
    Coordinates real-time replication from RocksDB to Kafka and state replay during recovery.

    Modes:
      - "production": REAL Kafka changelog topic. A confluent_kafka.Producer is
        constructed at init (fails loudly if the client lib is missing).
        publish_state_change produces to Kafka with a delivery callback;
        flush() verifies delivery callbacks + flush success. The source offset
        is committed only when this succeeds.
      - "demo"/"test": in-memory changelog (explicitly non-durable).
    """

    def __init__(
        self,
        changelog_topic: str = "streamforge.truck_state.changelog",
        bootstrap_servers: str = "localhost:9092",
        storage_mode: str = "demo",
        producer: Optional[Any] = None,
    ) -> None:
        self.changelog_topic = changelog_topic
        self.bootstrap_servers = bootstrap_servers
        self.storage_mode = storage_mode
        self._in_memory_changelog: Dict[int, List[ChangelogRecord]] = {}
        # Optional injected producer (dependency injection for tests/workers
        # that manage their own client). If not injected in production mode,
        # a real producer is constructed below (fails loudly if unavailable).
        self._producer = producer
        # Delivery-callback accounting (authoritative for commit gating).
        self._pending_deliveries: int = 0
        self._delivery_errors: List[str] = []
        self._produced_ok: int = 0
        if storage_mode == "production" and self._producer is None:
            try:
                from confluent_kafka import Producer
            except Exception as e:
                raise RuntimeError(
                    "STORAGE_MODE=production requires confluent-kafka installed. "
                    f"Original error: {e}"
                )
            conf = {
                "bootstrap.servers": self.bootstrap_servers,
                "acks": "all",
                "enable.idempotence": True,
                "compression.type": "lz4",
                "linger.ms": 10,
                "retries": 5,
                "retry.backoff.ms": 200,
            }
            try:
                self._producer = Producer(conf)
            except Exception as e:
                raise RuntimeError(f"Failed to create Kafka changelog producer: {e}")

    def _on_delivery(self, err, msg) -> None:
        self._pending_deliveries = max(0, self._pending_deliveries - 1)
        if err is not None:
            self._delivery_errors.append(str(err))
            logger.error(f"Changelog delivery failed: {err}")
        else:
            self._produced_ok += 1

    # Alias kept for compatibility with injected-producer call sites.
    def _delivery_callback(self, err, msg) -> None:
        self._on_delivery(err, msg)

    @property
    def delivery_errors(self) -> List[str]:
        """Public read access to delivery-error history (commit-gating signal)."""
        return self._delivery_errors

    def publish_state_change(
        self,
        partition: int,
        key: str,
        value: Dict[str, Any],
        worker_id: str,
        timestamp: int,
        source_offset: Optional[int] = None,
        op: str = "PUT",
    ) -> int:
        """
        Replicate a state update to the Kafka changelog topic.
        Returns the assigned changelog offset. seq = source_offset (durable version).
        Production: produces to Kafka with delivery-callback tracking; any
        produce/queue failure raises so the caller blocks the source commit.
        Demo/test: appends to the in-memory list.
        """
        if partition not in self._in_memory_changelog:
            self._in_memory_changelog[partition] = []

        offset = len(self._in_memory_changelog[partition])
        seq = source_offset if source_offset is not None else offset
        payload = dict(value) if isinstance(value, dict) else value
        if isinstance(payload, dict):
            payload.setdefault("seq", seq)
            payload.setdefault("source_offset", seq)
        record = ChangelogRecord(
            partition=partition,
            key=key,
            value=payload,
            offset=offset,
            timestamp=timestamp,
            worker_source=worker_id,
            seq=seq,
            op=op,
        )
        if self.storage_mode == "production":
            if self._producer is None:
                raise RuntimeError("Kafka changelog producer not initialized (production mode).")
            try:
                self._pending_deliveries += 1
                self._producer.produce(
                    topic=self.changelog_topic,
                    key=record.changelog_key.encode("utf-8"),
                    value=record.serialize(),
                    on_delivery=self._on_delivery,
                )
                self._producer.poll(0)  # serve delivery callbacks
            except Exception as e:
                self._pending_deliveries = max(0, self._pending_deliveries - 1)
                self._delivery_errors.append(str(e))
                logger.error(f"Changelog produce failed p={partition} key={key}: {e}")
                raise
            # Local mirror kept as read-your-writes cache only; Kafka is authoritative.
            self._in_memory_changelog[partition].append(record)
            return offset
        self._in_memory_changelog[partition].append(record)
        return offset

    def flush(self, timeout: float = 5) -> bool:
        """Flush pending changelog produces. Verifies delivery callbacks.

        Success requires: producer.flush() drains to zero AND no delivery
        callback reported an error since the last flush. Any failure returns
        False so the caller MUST NOT commit the source offset.
        """
        if self._delivery_errors:
            logger.error(f"Changelog delivery errors: {self._delivery_errors[-1]}")
            return False
        if self._pending_deliveries != 0:
            logger.error(f"Changelog {self._pending_deliveries} deliveries unacknowledged")
            return False
        try:
            if self._producer is not None:
                remaining = self._producer.flush(timeout)
                if remaining != 0:
                    logger.error(f"Changelog flush incomplete: {remaining} messages pending")
                    return False
                return True
        except Exception as e:
            logger.error(f"Changelog flush failed: {e}")
            return False
        return True

    def reset_delivery_errors(self) -> None:
        self._delivery_errors.clear()

    def _existing_seq(self, target_store: StateStore[str, Dict[str, Any]], key: str) -> Optional[int]:
        try:
            cur = target_store.get(key)
        except Exception:
            return None
        if not isinstance(cur, dict):
            return None
        seq = cur.get("seq", cur.get("source_offset"))
        return int(seq) if isinstance(seq, (int, float)) else None

    def restore_partition_state(
        self,
        partition: int,
        target_store: StateStore[str, Dict[str, Any]],
        on_progress: Optional[Callable[[int, int], None]] = None,
        use_kafka_replay: bool = False,
    ) -> int:
        """
        Replay all changelog records for a newly assigned partition into RocksDB.
        Idempotent: stale records (seq <= existing seq) are skipped.
        Production with use_kafka_replay=True consumes the compacted changelog
        topic from OFFSET_BEGINNING (read-only consumer group) and applies
        records for this partition; falls back to the local mirror only if the
        broker is unreachable (logged as a warning, mirror count returned).
        Returns count of newly applied records.
        """
        if use_kafka_replay and self.storage_mode == "production":
            try:
                replayed = self._replay_from_kafka(partition, target_store, on_progress)
                if replayed is not None:
                    return replayed
            except Exception as e:
                logger.warning(f"Kafka changelog replay unavailable p={partition}: {e}; using local mirror")
        records = self._in_memory_changelog.get(partition, [])
        total = len(records)
        logger.info(f"Restoring Partition {partition} state from changelog ({total} records)...")

        restored_count = 0
        for i, record in enumerate(records):
            existing_seq = self._existing_seq(target_store, record.key)
            if existing_seq is not None and record.seq <= existing_seq:
                if on_progress and (i % 100 == 0 or i == total - 1):
                    on_progress(i + 1, total)
                continue
            if record.value is None or (isinstance(record.value, dict) and record.value.get("__DELETED__")):
                target_store.delete(record.key)
            else:
                target_store.put(record.key, record.value)

            restored_count += 1
            if on_progress and (i % 100 == 0 or i == total - 1):
                on_progress(i + 1, total)

        logger.info(f"Partition {partition} state restored: {restored_count}/{total} applied.")
        return restored_count

    def _replay_from_kafka(
        self,
        partition: int,
        target_store: StateStore[str, Dict[str, Any]],
        on_progress: Optional[Callable[[int, int], None]] = None,
        max_messages: int = 100_000,
        poll_timeout: float = 2.0,
    ) -> Optional[int]:
        """Consume the compacted changelog topic from beginning (read-only group).

        Returns applied-record count, or None if the broker is unreachable so
        the caller can fall back to the local mirror (logged). Applies the same
        idempotent seq<=existing.seq skip rule as mirror replay.
        """
        from confluent_kafka import Consumer

        conf = {
            "bootstrap.servers": self.bootstrap_servers,
            "group.id": "streamforge-changelog-restore",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
        consumer = Consumer(conf)
        consumer.subscribe([self.changelog_topic])
        applied = 0
        seen = 0
        idle_polls = 0
        try:
            while seen < max_messages and idle_polls < 3:
                msg = consumer.poll(poll_timeout)
                if msg is None:
                    idle_polls += 1
                    continue
                idle_polls = 0
                if msg.error():
                    raise RuntimeError(str(msg.error()))
                try:
                    payload = json.loads(msg.value().decode("utf-8"))
                except Exception:
                    continue
                if int(payload.get("partition", -1)) != partition:
                    continue
                key = str(payload.get("key", ""))
                value = payload.get("value")
                seq = payload.get("seq", payload.get("source_offset"))
                try:
                    seq_int = int(seq) if seq is not None else None
                except Exception:
                    seq_int = None
                existing = self._existing_seq(target_store, key)
                if existing is not None and seq_int is not None and seq_int <= existing:
                    seen += 1
                    continue
                op = str(payload.get("op", "PUT"))
                if value is None or op == "DELETE" or (isinstance(value, dict) and value.get("__DELETED__")):
                    target_store.delete(key)
                else:
                    if isinstance(value, dict) and seq_int is not None:
                        value = dict(value)
                        value.setdefault("seq", seq_int)
                        value.setdefault("source_offset", seq_int)
                    target_store.put(key, value)
                applied += 1
                seen += 1
                if on_progress and (seen % 100 == 0):
                    on_progress(seen, seen)
            logger.info(f"Kafka changelog replay p={partition}: applied {applied}/{seen}")
            return applied
        finally:
            try:
                consumer.close()
            except Exception:
                pass
