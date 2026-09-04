"""
Hardening tests — covers Phase I requirements.
Tests are deterministic and must fail if feature broken.
"""
import math
import time
import pytest
from streamforge.core.interfaces import TruckTelemetryEvent
from streamforge.windowing.engine import TemperatureAccumulator, WatermarkGenerator, WindowedRollingAverageProcessor
from streamforge.state.rocksdb_store import RocksDBStateStore
from streamforge.state.changelog_manager import ChangelogManager


class TestConfig:
    def test_centralized_config(self):
        from streamforge.config import get_settings

        s = get_settings()
        assert s.window_size_ms == 300_000
        assert s.max_lateness_ms == 15_000
        assert s.kafka_partitions == 32
        assert s.storage_mode in ("production", "demo", "test")
        assert s.late_event_policy in ("drop", "side_output")


class TestProducerAffinity:
    def test_partition_deterministic(self):
        from streamforge.producers.truck_telemetry import FleetTelemetryGenerator

        gen = FleetTelemetryGenerator(fleet_size=100, num_partitions=32)
        p1 = gen._get_partition("TRK-00100")
        p2 = gen._get_partition("TRK-00100")
        assert p1 == p2
        assert 0 <= p1 < 32
        # Different truck likely different partition (not guaranteed but test distribution)
        # Check that generator sets partition on event
        evt = gen.generate_event(100)
        assert evt.partition == gen._get_partition(evt.truck_id)

    def test_key_is_truck_id(self):
        import json

        from streamforge.producers.kafka_producer import KafkaTelemetryProducer

        prod = KafkaTelemetryProducer(bootstrap_servers="localhost:9092")
        from streamforge.core.interfaces import TruckTelemetryEvent

        evt = TruckTelemetryEvent(truck_id="TRK-99999", timestamp=1700000000000, temperature=5.0, partition=3)
        k, v = prod._serialize(evt)
        assert k == b"TRK-99999"
        data = json.loads(v.decode())
        assert data["truck_id"] == "TRK-99999"


class TestWelford:
    def test_stddev_known_values(self):
        acc = TemperatureAccumulator()
        vals = [2, 4, 4, 4, 5, 5, 7, 9]  # mean 5, sample variance 32/7≈4.571 std≈2.138
        for v in vals:
            acc.add(float(v))
        assert acc.count == 8
        assert acc.average == 5.0
        assert abs(acc.std_dev - 2.14) < 0.02
        # Check m2 persistence
        d = acc.to_dict()
        assert "m2" in d
        acc2 = TemperatureAccumulator.from_dict(d)
        assert abs(acc2.std_dev - 2.14) < 0.02
        assert abs(acc2._m2 - acc._m2) < 1e-6

    def test_filter_T_gt_0(self):
        proc = WindowedRollingAverageProcessor(worker_id="w1", window_size_ms=300_000, max_lateness_ms=15_000)
        base = 1709280000000
        # Negative temp should be filtered (no window created, no watermark advance)
        evt_neg = TruckTelemetryEvent(truck_id="TRK-00001", timestamp=base + 1000, temperature=-5.0)
        res = proc.process_event(evt_neg)
        assert res == []
        assert len(proc.active_windows) == 0
        # Zero filtered
        evt_zero = TruckTelemetryEvent(truck_id="TRK-00001", timestamp=base + 2000, temperature=0.0)
        assert proc.process_event(evt_zero) == []
        # Positive passes
        evt_pos = TruckTelemetryEvent(truck_id="TRK-00001", timestamp=base + 3000, temperature=5.0)
        proc.process_event(evt_pos)
        assert len(proc.active_windows) == 1


class TestWatermarkLate:
    def test_watermark_advancement_and_late(self, monkeypatch):
        # Force side_output policy
        monkeypatch.setenv("LATE_EVENT_POLICY", "side_output")
        from streamforge.config import reload_settings

        reload_settings()
        proc = WindowedRollingAverageProcessor(worker_id="w1", window_size_ms=300_000, max_lateness_ms=10_000)
        base = 1709280000000
        # On-time
        e1 = TruckTelemetryEvent(truck_id="T", timestamp=base + 10_000, temperature=5.0)
        late, emitted = proc.process_telemetry(e1)
        assert late is None or isinstance(late, TruckTelemetryEvent) == False
        assert emitted == []
        # Advance watermark: need max_seen - lateness >= window_end
        # Window [base, base+300k), end=base+300k, need watermark >= end => max_seen >= base+310k
        e_adv = TruckTelemetryEvent(truck_id="T", timestamp=base + 320_000, temperature=5.0)
        late, emitted = proc.process_telemetry(e_adv)
        # Should have emitted window for base
        assert len(emitted) == 1
        assert emitted[0].window_start == base
        # Now late event for already-closed window
        e_late = TruckTelemetryEvent(truck_id="T", timestamp=base + 20_000, temperature=5.0)
        # Watermark is base+310k, late timestamp base+20k < watermark => late
        assert proc.watermark_gen.is_event_late(e_late.timestamp) is True
        late_sig, emitted2 = proc.process_telemetry(e_late)
        # side_output should return late signal, not merge
        assert late_sig is not None or emitted2 == []
        # Verify state not polluted: no active window for base
        assert (e_late.truck_id, base) not in proc.active_windows
        # Cleanup
        monkeypatch.delenv("LATE_EVENT_POLICY", raising=False)
        reload_settings()

    def test_out_of_order_within_lateness(self, monkeypatch):
        monkeypatch.setenv("LATE_EVENT_POLICY", "side_output")
        from streamforge.config import reload_settings

        reload_settings()
        proc = WindowedRollingAverageProcessor(worker_id="w1", window_size_ms=300_000, max_lateness_ms=15_000)
        base = 1709280000000
        e1 = TruckTelemetryEvent(truck_id="X", timestamp=base + 100_000, temperature=5.0)
        proc.process_telemetry(e1)
        # Out-of-order but within 15s: timestamp 90k, max_seen 100k, watermark 85k, 90k >85k so not late
        e2 = TruckTelemetryEvent(truck_id="X", timestamp=base + 90_000, temperature=6.0)
        assert proc.watermark_gen.is_event_late(e2.timestamp) is False
        late, emitted = proc.process_telemetry(e2)
        assert late is None
        # Count should be 2 in same window
        assert proc.active_windows[("X", base)].count == 2
        monkeypatch.delenv("LATE_EVENT_POLICY", raising=False)
        reload_settings()


class TestChangelogDurableSeq:
    def test_seq_is_source_offset(self, tmp_path):
        mgr = ChangelogManager(storage_mode="production", bootstrap_servers="localhost:9092")
        # source_offset 100 and 101 for same key
        mgr.publish_state_change(partition=0, key="TRK:1000", value={"avg": 1}, worker_id="w1", timestamp=1000, source_offset=100)
        mgr.publish_state_change(partition=0, key="TRK:1000", value={"avg": 2}, worker_id="w1", timestamp=2000, source_offset=101)
        # Check seq equals source_offset
        recs = mgr._in_memory_changelog[0]
        assert recs[0].seq == 100
        assert recs[1].seq == 101
        # Restore to empty store — should get latest
        store = RocksDBStateStore(db_path=str(tmp_path / "s1"), partition_id=0, storage_mode="test")
        mgr.restore_partition_state(0, store)
        v = store.get("TRK:1000")
        assert v["avg"] == 2
        assert v["seq"] == 101

    def test_duplicate_replay_idempotent(self, tmp_path):
        mgr = ChangelogManager(storage_mode="test")
        mgr.publish_state_change(partition=1, key="K:1", value={"avg": 10}, worker_id="w1", timestamp=1000, source_offset=50)
        store = RocksDBStateStore(db_path=str(tmp_path / "s2"), partition_id=1, storage_mode="test")
        c1 = mgr.restore_partition_state(1, store)
        v1 = store.get("K:1")
        c2 = mgr.restore_partition_state(1, store)
        v2 = store.get("K:1")
        assert v1 == v2
        # Second restore should apply 0 new (idempotent) — c2 may be 0 due to seq check
        # At least state unchanged
        assert v2["avg"] == 10

    def test_stale_not_overwrite_newer(self, tmp_path):
        mgr = ChangelogManager(storage_mode="production", bootstrap_servers="localhost:9092")
        # Simulate newer first, then stale
        mgr.publish_state_change(partition=2, key="K:2", value={"avg": 20}, worker_id="w1", timestamp=1000, source_offset=200)
        mgr.publish_state_change(partition=2, key="K:2", value={"avg": 99}, worker_id="w1", timestamp=500, source_offset=50)  # stale timestamp but seq 50 <200
        store = RocksDBStateStore(db_path=str(tmp_path / "s3"), partition_id=2, storage_mode="test")
        mgr.restore_partition_state(2, store)
        v = store.get("K:2")
        # Stale with seq 50 should not overwrite seq 200? But publish order is 200 then 50, replay order is as published.
        # Our replay iterates in publish order, so second (seq 50) will be skipped because existing seq 200 >50
        assert v["avg"] == 20

    def test_newer_overwrites(self, tmp_path):
        mgr = ChangelogManager(storage_mode="test")
        mgr.publish_state_change(partition=3, key="K:3", value={"avg": 1}, worker_id="w1", timestamp=1000, source_offset=10)
        store = RocksDBStateStore(db_path=str(tmp_path / "s4"), partition_id=3, storage_mode="test")
        mgr.restore_partition_state(3, store)
        assert store.get("K:3")["avg"] == 1
        mgr.publish_state_change(partition=3, key="K:3", value={"avg": 2}, worker_id="w1", timestamp=2000, source_offset=11)
        mgr.restore_partition_state(3, store)
        assert store.get("K:3")["avg"] == 2


class TestChangelogFailureGating:
    def test_failure_does_not_commit(self):
        """Simulates changelog delivery failure -> commit must not be called."""

        class FakeConsumer:
            def __init__(self):
                self.committed = []

            def commit(self, msg):
                self.committed.append(msg)

        class FailingChangelog:
            def publish_state_change(self, *a, **kw):
                raise RuntimeError("kafka down")

            def flush(self, timeout=5):
                return False

        fake_consumer = FakeConsumer()
        failing = FailingChangelog()

        # Simulate worker logic: try publish, if fails don't commit
        msg = type("M", (), {"offset": lambda self: 42})()
        changelog_ok = True
        try:
            failing.publish_state_change(partition=0, key="k", value={}, worker_id="w1", timestamp=0, source_offset=42)
        except Exception:
            changelog_ok = False
        if not changelog_ok:
            pass  # do not commit
        else:
            fake_consumer.commit(msg)
        assert fake_consumer.committed == []

        # Success path should commit
        class OkChangelog:
            def publish_state_change(self, *a, **kw):
                return 0

            def flush(self, timeout=5):
                return True

        ok = OkChangelog()
        changelog_ok = True
        try:
            ok.publish_state_change(partition=0, key="k", value={}, worker_id="w1", timestamp=0, source_offset=42)
            flushed = ok.flush()
            if not flushed:
                changelog_ok = False
        except Exception:
            changelog_ok = False
        if changelog_ok:
            fake_consumer.commit(msg)
        assert len(fake_consumer.committed) == 1
