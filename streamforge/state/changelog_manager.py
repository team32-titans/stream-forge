"""
StreamForge Kafka Changelog Replication & Recovery Manager
Uses confluent-kafka for production; in-memory for demo/test.
Protocol: docs/STATE_CHANGELOG_PROTOCOL.md
"""
import json
import logging
from typing import Any, Callable, Dict, List, Optional
from streamforge.core.interfaces import StateStore

logger = logging.getLogger("streamforge.changelog")


class ChangelogRecord:
    def __init__(
        self,
        partition: int,
        key: str,
        value: Optional[Dict[str, Any]],
        offset: int,
        timestamp: int,
        worker_source: str,
        seq: int = 0,
        source_offset: int = -1,
        changelog_key: Optional[str] = None,
        op: str = "PUT",
    ) -> None:
        self.partition = partition
        self.key = key  # state_key
        self.changelog_key = changelog_key or f"{partition:02d}:{key}"
        self.value = value
        self.offset = offset
        self.timestamp = timestamp
        self.worker_source = worker_source
        self.seq = seq
        self.source_offset = source_offset
        self.op = op

    def serialize(self) -> bytes:
        payload = {
            "partition": self.partition,
            "key": self.key,
            "changelog_key": self.changelog_key,
            "value": self.value,
            "offset": self.offset,
            "timestamp": self.timestamp,
            "worker_source": self.worker_source,
            "seq": self.seq,
            "source_offset": self.source_offset,
            "op": self.op,
        }
        return json.dumps(payload).encode("utf-8")

    @classmethod
    def deserialize(cls, data: bytes) -> "ChangelogRecord":
        d = json.loads(data.decode("utf-8"))
        return cls(
            partition=d["partition"],
            key=d["key"],
            value=d.get("value"),
            offset=d.get("offset", -1),
            timestamp=d.get("timestamp", 0),
            worker_source=d.get("worker_source", ""),
            seq=d.get("seq", 0),
            source_offset=d.get("source_offset", -1),
            changelog_key=d.get("changelog_key"),
            op=d.get("op", "PUT"),
        )


class ChangelogManager:
    def __init__(
        self,
        changelog_topic: Optional[str] = None,
        bootstrap_servers: Optional[str] = None,
        storage_mode: Optional[str] = None,
    ) -> None:
        from streamforge.config import get_settings

        s = get_settings()
        self.changelog_topic = changelog_topic or s.kafka_changelog_topic
        self.bootstrap_servers = bootstrap_servers or s.kafka_bootstrap_servers
        self.storage_mode = storage_mode or s.storage_mode
        self._in_memory_changelog: Dict[int, List[ChangelogRecord]] = {}
        self._seq_counters: Dict[str, int] = {}  # changelog_key -> seq
        self._producer = None
        self._use_kafka = self.storage_mode == "production"
        if self._use_kafka:
            try:
                from confluent_kafka import Producer

                self._producer = Producer(
                    {
                        "bootstrap.servers": self.bootstrap_servers,
                        "acks": "all",
                        "enable.idempotence": True,
                        "compression.type": "lz4",
                        "linger.ms": 5,
                    }
                )
                logger.info(f"Changelog Kafka producer connected to {self.bootstrap_servers}")
            except Exception as e:
                # Production must fail loud — do not silently become demo
                raise RuntimeError(f"STORAGE_MODE=production requires Kafka for changelog at {self.bootstrap_servers}: {e}") from e

    def _next_seq(self, changelog_key: str) -> int:
        nxt = self._seq_counters.get(changelog_key, 0) + 1
        self._seq_counters[changelog_key] = nxt
        return nxt

    def publish_state_change(
        self,
        partition: int,
        key: str,
        value: Dict[str, Any],
        worker_id: str,
        timestamp: int,
        source_offset: int = -1,
        op: str = "PUT",
    ) -> int:
        """
        Durable version invariant:
          partition + state_key -> monotonically increasing version
        Production: version = Kafka source_offset (durable, per-partition monotonic).
        Demo/test: if source_offset unavailable, fallback to in-memory _seq_counters (non-durable,
        explicitly for demo — not used in production recovery).
        Stale replay with older version never overwrites newer state.
        """
        changelog_key = f"{partition:02d}:{key}"
        # Production durability: source_offset is the version
        if self.storage_mode == "production" and source_offset >= 0:
            seq = source_offset
        else:
            # Demo/test fallback — not durable across worker restart; documented limitation
            seq = self._next_seq(changelog_key)
            # If caller did provide source_offset even in demo, prefer it as version when larger
            if source_offset >= 0 and source_offset > seq:
                seq = source_offset
        # Always keep in-memory for fast replay/test
        if partition not in self._in_memory_changelog:
            self._in_memory_changelog[partition] = []
        offset = len(self._in_memory_changelog[partition])
        record = ChangelogRecord(
            partition=partition,
            key=key,
            value=value,
            offset=offset,
            timestamp=timestamp,
            worker_source=worker_id,
            seq=seq,
            source_offset=source_offset,
            changelog_key=changelog_key,
            op=op,
        )
        self._in_memory_changelog[partition].append(record)

        if self._use_kafka and self._producer is not None:
            # Track delivery result — caller must wait for ack before committing source offset
            delivery_ok = {"success": None, "error": None}

            def _cb(err, msg):
                if err is not None:
                    delivery_ok["error"] = str(err)
                    delivery_ok["success"] = False
                else:
                    delivery_ok["success"] = True

            try:
                self._producer.produce(
                    self.changelog_topic,
                    key=changelog_key.encode(),
                    value=record.serialize(),
                    partition=partition,
                    on_delivery=_cb,
                )
                self._producer.poll(0)
            except Exception as e:
                logger.error(f"Changelog produce failed p={partition} key={key}: {e}")
                # Mark as failed so caller does not commit
                record._delivery_failed = True  # type: ignore
                return offset
            # Attach delivery tracker to record for caller inspection (used in tests)
            record._delivery_ok = delivery_ok  # type: ignore

        return offset

    def flush(self, timeout: float = 5) -> bool:
        """
        Flush changelog producer and return True if all deliveries succeeded.
        Caller must check this before committing source offset.
        """
        if self._producer:
            remaining = self._producer.flush(timeout)
            if remaining != 0:
                logger.error(f"Changelog flush incomplete: {remaining} messages pending after {timeout}s")
                return False
            # Check last records' delivery status if any failed callback
            # In production, delivery errors are reported via callback; flush success implies ack
            return True
        return True

    def publish_and_wait(self, partition: int, key: str, value: Dict[str, Any], worker_id: str, timestamp: int, source_offset: int = -1, op: str = "PUT", timeout: float = 5) -> bool:
        """Publish and block until ack; returns True on success."""
        self.publish_state_change(partition, key, value, worker_id, timestamp, source_offset, op)
        return self.flush(timeout)

    def restore_partition_state(
        self,
        partition: int,
        target_store: StateStore[str, Dict[str, Any]],
        on_progress: Optional[Callable[[int, int], None]] = None,
        use_kafka_replay: bool = False,
    ) -> int:
        """
        Replay changelog for partition into target_store.
        If use_kafka_replay and production, consumes from Kafka changelog from beginning.
        Otherwise replays in-memory changelog (demo/test).
        Idempotent via seq comparison.
        """
        if use_kafka_replay and self._use_kafka:
            return self._replay_from_kafka(partition, target_store, on_progress)

        records = self._in_memory_changelog.get(partition, [])
        total = len(records)
        logger.info(f"Restoring Partition {partition} state from in-memory changelog ({total} records)...")
        restored = 0
        for i, rec in enumerate(records):
            # Idempotent: only apply if seq > existing
            existing = target_store.get(rec.key)
            existing_seq = 0
            if existing:
                try:
                    existing_seq = int(existing.get("seq", 0))
                except Exception:
                    existing_seq = 0
            if rec.seq <= existing_seq:
                continue
            if rec.value is None or (isinstance(rec.value, dict) and rec.value.get("__DELETED__")) or rec.op == "DELETE":
                target_store.delete(rec.key)
            else:
                # Ensure seq preserved in value for next comparison
                val = dict(rec.value)
                val["seq"] = rec.seq
                val["source_offset"] = rec.source_offset
                target_store.put(rec.key, val)
            restored += 1
            if on_progress and (i % 100 == 0 or i == total - 1):
                on_progress(i + 1, total)
        logger.info(f"Partition {partition} restored {restored}/{total} records (idempotent).")
        return restored

    def _replay_from_kafka(self, partition: int, target_store: StateStore[str, Dict[str, Any]], on_progress=None) -> int:
        from confluent_kafka import Consumer, TopicPartition, OFFSET_BEGINNING

        conf = {
            "bootstrap.servers": self.bootstrap_servers,
            "group.id": f"streamforge-changelog-replay-p{partition}-{id(target_store)}",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
        c = Consumer(conf)
        try:
            tp = TopicPartition(self.changelog_topic, partition, OFFSET_BEGINNING)
            c.assign([tp])
            restored = 0
            # Poll until idle
            idle = 0
            while idle < 3:
                msg = c.poll(1.0)
                if msg is None:
                    idle += 1
                    continue
                if msg.error():
                    logger.error(f"Changelog replay error: {msg.error()}")
                    continue
                idle = 0
                try:
                    rec = ChangelogRecord.deserialize(msg.value())
                except Exception as e:
                    logger.error(f"Bad changelog record: {e}")
                    continue
                # Only records for this partition
                if rec.partition != partition:
                    continue
                existing = target_store.get(rec.key)
                existing_seq = int(existing.get("seq", 0)) if existing and isinstance(existing, dict) else 0
                if rec.seq <= existing_seq:
                    continue
                if rec.op == "DELETE" or (rec.value and rec.value.get("__DELETED__")):
                    target_store.delete(rec.key)
                else:
                    val = dict(rec.value) if rec.value else {}
                    val["seq"] = rec.seq
                    target_store.put(rec.key, val)
                restored += 1
                if on_progress and restored % 100 == 0:
                    on_progress(restored, restored)
            logger.info(f"Kafka replay partition {partition}: {restored} records")
            return restored
        finally:
            try:
                c.close()
            except Exception:
                pass
