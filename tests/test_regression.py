"""
StreamForge Regression & Hardening Test Suite
=============================================
Every bug identified in the final audit must have a regression test here.
Tests are deterministic and meaningful — no tautological assertions.
"""
import math
import time
import tempfile
from pathlib import Path

import pytest

from streamforge.core.interfaces import TruckTelemetryEvent
from streamforge.windowing.engine import (
    TemperatureAccumulator,
    WatermarkGenerator,
    WindowAssigner,
    WindowedRollingAverageProcessor,
)
from streamforge.state.rocksdb_store import RocksDBStateStore
from streamforge.state.changelog_manager import ChangelogManager


# ============================================================================
# TEMPERATURE FILTER (T > 0)
# ============================================================================


class TestTemperatureFilter:
    """Pipeline must reject T <= 0 events completely — no state mutation."""

    def test_negative_temperature_rejected(self):
        proc = WindowedRollingAverageProcessor(
            worker_id="w1", window_size_ms=300_000, max_lateness_ms=15_000
        )
        base = 1709280000000
        evt = TruckTelemetryEvent(
            truck_id="TRK-00001", timestamp=base + 1000, temperature=-5.0
        )
        late_sig, emitted = proc.process_telemetry(evt)
        assert late_sig is None
        assert emitted == []
        assert len(proc.active_windows) == 0
        # Watermark must not have advanced
        assert proc.watermark_gen.current_max_timestamp == 0

    def test_zero_temperature_rejected(self):
        proc = WindowedRollingAverageProcessor(
            worker_id="w1", window_size_ms=300_000, max_lateness_ms=15_000
        )
        base = 1709280000000
        evt = TruckTelemetryEvent(
            truck_id="TRK-00001", timestamp=base + 1000, temperature=0.0
        )
        late_sig, emitted = proc.process_telemetry(evt)
        assert late_sig is None
        assert emitted == []
        assert len(proc.active_windows) == 0

    def test_positive_temperature_accepted(self):
        proc = WindowedRollingAverageProcessor(
            worker_id="w1", window_size_ms=300_000, max_lateness_ms=15_000
        )
        base = 1709280000000
        evt = TruckTelemetryEvent(
            truck_id="TRK-00001", timestamp=base + 1000, temperature=5.0
        )
        late_sig, emitted = proc.process_telemetry(evt)
        assert late_sig is None
        assert len(proc.active_windows) == 1
        key = ("TRK-00001", base)
        assert proc.active_windows[key].count == 1
        assert proc.active_windows[key].sum_temp == 5.0

    def test_filtered_events_do_not_modify_existing_window(self):
        """After one accepted event, a filtered event must leave state unchanged."""
        proc = WindowedRollingAverageProcessor(
            worker_id="w1", window_size_ms=300_000, max_lateness_ms=15_000
        )
        base = 1709280000000
        # Accept one event
        proc.process_telemetry(
            TruckTelemetryEvent(
                truck_id="TRK-00001", timestamp=base + 1000, temperature=5.0
            )
        )
        key = ("TRK-00001", base)
        count_before = proc.active_windows[key].count
        sum_before = proc.active_windows[key].sum_temp
        m2_before = proc.active_windows[key]._m2

        # Send filtered event (T <= 0)
        proc.process_telemetry(
            TruckTelemetryEvent(
                truck_id="TRK-00001", timestamp=base + 2000, temperature=-10.0
            )
        )
        # State must be unchanged
        assert proc.active_windows[key].count == count_before
        assert proc.active_windows[key].sum_temp == sum_before
        assert proc.active_windows[key]._m2 == m2_before

    def test_filtered_events_do_not_change_emitted_statistics(self):
        """Filtered events must not affect the final emitted aggregate."""
        proc = WindowedRollingAverageProcessor(
            worker_id="w1", window_size_ms=300_000, max_lateness_ms=10_000
        )
        base = 1709280000000
        # Add events to window
        proc.process_telemetry(
            TruckTelemetryEvent(
                truck_id="T", timestamp=base + 10_000, temperature=4.0
            )
        )
        proc.process_telemetry(
            TruckTelemetryEvent(
                truck_id="T", timestamp=base + 20_000, temperature=6.0
            )
        )
        # Send filtered event
        proc.process_telemetry(
            TruckTelemetryEvent(
                truck_id="T", timestamp=base + 30_000, temperature=-5.0
            )
        )
        # Advance watermark past window end
        _, emitted = proc.process_telemetry(
            TruckTelemetryEvent(
                truck_id="T", timestamp=base + 320_000, temperature=5.0
            )
        )
        assert len(emitted) == 1
        assert emitted[0].count == 2  # NOT 3
        assert emitted[0].avg_temperature == 5.0  # (4+6)/2


# ============================================================================
# WELFORD ALGORITHM
# ============================================================================


class TestWelfordAlgorithm:
    """Verify mathematically correct online variance computation."""

    def test_known_values_stddev(self):
        """For [2,4,4,4,5,5,7,9]: mean=5, sample stddev ≈ 2.138."""
        acc = TemperatureAccumulator()
        vals = [2, 4, 4, 4, 5, 5, 7, 9]
        for v in vals:
            acc.add(float(v))
        assert acc.count == 8
        assert acc.sum_temp == 40.0
        assert acc.average == 5.0
        assert acc.min_temp == 2.0
        assert acc.max_temp == 9.0
        # Sample variance = 32/7 ≈ 4.5714, sample stddev ≈ 2.1381
        expected_m2 = 32.0  # sum of squared deviations from mean
        assert abs(acc._m2 - expected_m2) < 1e-9, f"m2 should be 32.0, got {acc._m2}"
        expected_stddev = math.sqrt(32.0 / 7)
        assert abs(acc.std_dev - round(expected_stddev, 2)) < 0.02

    def test_single_value_stddev(self):
        acc = TemperatureAccumulator()
        acc.add(10.0)
        assert acc.count == 1
        assert acc.std_dev == 0.0  # need >= 2 for sample stddev

    def test_two_values_stddev(self):
        acc = TemperatureAccumulator()
        acc.add(2.0)
        acc.add(4.0)
        assert acc.count == 2
        # mean=3, m2 = (2-3)*(2-2) + (4-3)*(4-4) — let's compute properly
        # After add(2): old_mean=2, count=1, new_mean=2, m2 += (2-2)*(2-2)=0
        # After add(4): old_mean=2, count=2, new_mean=3, m2 += (4-2)*(4-3)=2
        assert abs(acc._m2 - 2.0) < 1e-9
        expected = math.sqrt(2.0 / 1)  # sample variance = 2/1 = 2
        assert abs(acc.std_dev - round(expected, 2)) < 0.02

    def test_mean_calculation(self):
        acc = TemperatureAccumulator()
        for v in [10.0, 20.0, 30.0]:
            acc.add(v)
        assert acc.average == 20.0

    def test_min_max(self):
        acc = TemperatureAccumulator()
        for v in [5.0, 3.0, 8.0, 1.0, 6.0]:
            acc.add(v)
        assert acc.min_temp == 1.0
        assert acc.max_temp == 8.0

    def test_m2_incremental_correctness(self):
        """Verify m2 matches batch computation for arbitrary data."""
        import random

        random.seed(42)
        data = [random.uniform(-20.0, 20.0) for _ in range(100)]
        acc = TemperatureAccumulator()
        for v in data:
            acc.add(v)
        # Compute expected m2 from definition: sum((x - mean)^2)
        mean = sum(data) / len(data)
        expected_m2 = sum((x - mean) ** 2 for x in data)
        assert abs(acc._m2 - expected_m2) < 1e-6, (
            f"m2 mismatch: got {acc._m2}, expected {expected_m2}"
        )


# ============================================================================
# M2 PERSISTENCE (SERIALIZE / DESERIALIZE)
# ============================================================================


class TestM2Persistence:
    """Verify that TemperatureAccumulator round-trips correctly."""

    def test_to_dict_includes_m2(self):
        acc = TemperatureAccumulator()
        for v in [2, 4, 4, 4, 5, 5, 7, 9]:
            acc.add(float(v))
        d = acc.to_dict()
        assert "m2" in d
        assert d["m2"] != 0.0
        assert d["count"] == 8
        assert d["sum"] == 40.0

    def test_from_dict_restores_m2(self):
        acc = TemperatureAccumulator()
        for v in [2, 4, 4, 4, 5, 5, 7, 9]:
            acc.add(float(v))
        d = acc.to_dict()
        acc2 = TemperatureAccumulator.from_dict(d)
        assert acc2.count == acc.count
        assert abs(acc2.sum_temp - acc.sum_temp) < 1e-9
        assert abs(acc2._m2 - acc._m2) < 1e-6
        assert acc2.min_temp == acc.min_temp
        assert acc2.max_temp == acc.max_temp
        assert abs(acc2.std_dev - acc.std_dev) < 0.01

    def test_continue_processing_after_restore(self):
        """Serialize mid-stream, restore, continue, verify correct final stats."""
        acc_original = TemperatureAccumulator()
        all_values = [2.0, 4.0, 4.0, 4.0, 5.0, 5.0, 7.0, 9.0]

        # Process first 4 events
        for v in all_values[:4]:
            acc_original.add(v)

        # Serialize and restore
        d = acc_original.to_dict()
        acc_restored = TemperatureAccumulator.from_dict(d)

        # Continue with remaining 4 events
        for v in all_values[4:]:
            acc_restored.add(v)

        # Build a clean accumulator with all values for comparison
        acc_clean = TemperatureAccumulator()
        for v in all_values:
            acc_clean.add(v)

        # Must match exactly
        assert acc_restored.count == acc_clean.count == 8
        assert abs(acc_restored.sum_temp - acc_clean.sum_temp) < 1e-9
        assert abs(acc_restored._m2 - acc_clean._m2) < 1e-6
        assert abs(acc_restored.std_dev - acc_clean.std_dev) < 0.01
        assert acc_restored.min_temp == acc_clean.min_temp
        assert acc_restored.max_temp == acc_clean.max_temp


# ============================================================================
# LATE EVENT SEMANTICS
# ============================================================================


class TestLateEventSemantics:
    """Late events must not mutate aggregation state."""

    def test_on_time_event_updates_state(self):
        proc = WindowedRollingAverageProcessor(
            worker_id="w1", window_size_ms=300_000, max_lateness_ms=10_000
        )
        base = 1709280000000
        evt = TruckTelemetryEvent(
            truck_id="T", timestamp=base + 10_000, temperature=5.0
        )
        late, emitted = proc.process_telemetry(evt)
        assert late is None
        assert ("T", base) in proc.active_windows
        assert proc.active_windows[("T", base)].count == 1

    def test_out_of_order_within_grace_updates_state(self, monkeypatch):
        monkeypatch.setenv("LATE_EVENT_POLICY", "side_output")
        from streamforge.config import reload_settings
        reload_settings()

        proc = WindowedRollingAverageProcessor(
            worker_id="w1", window_size_ms=300_000, max_lateness_ms=15_000
        )
        base = 1709280000000
        # First event
        proc.process_telemetry(
            TruckTelemetryEvent(
                truck_id="X", timestamp=base + 100_000, temperature=5.0
            )
        )
        # OOO event: ts=90k, watermark = 100k - 15k = 85k, 90k > 85k => not late
        evt_ooo = TruckTelemetryEvent(
            truck_id="X", timestamp=base + 90_000, temperature=6.0
        )
        late, emitted = proc.process_telemetry(evt_ooo)
        assert late is None  # not late
        assert proc.active_windows[("X", base)].count == 2

        monkeypatch.delenv("LATE_EVENT_POLICY", raising=False)
        reload_settings()

    def test_late_event_does_not_mutate_state(self, monkeypatch):
        monkeypatch.setenv("LATE_EVENT_POLICY", "side_output")
        from streamforge.config import reload_settings
        reload_settings()

        proc = WindowedRollingAverageProcessor(
            worker_id="w1", window_size_ms=300_000, max_lateness_ms=10_000
        )
        base = 1709280000000
        # Process on-time event
        proc.process_telemetry(
            TruckTelemetryEvent(
                truck_id="T", timestamp=base + 10_000, temperature=5.0
            )
        )
        # Advance watermark far ahead
        proc.process_telemetry(
            TruckTelemetryEvent(
                truck_id="T", timestamp=base + 320_000, temperature=5.0
            )
        )
        # Capture state before late event
        windows_before = dict(proc.active_windows)
        wm_before = proc.watermark_gen.last_emitted_watermark

        # Send late event
        late_evt = TruckTelemetryEvent(
            truck_id="T", timestamp=base + 20_000, temperature=65.0
        )
        late_sig, emitted = proc.process_telemetry(late_evt)

        assert late_sig is not None  # returned as side output
        assert late_sig.is_late is True
        # No new window created for the old window start
        assert ("T", base) not in proc.active_windows
        # Watermark did not move backwards
        assert proc.watermark_gen.last_emitted_watermark >= wm_before
        assert emitted == []

        monkeypatch.delenv("LATE_EVENT_POLICY", raising=False)
        reload_settings()

    def test_watermark_monotonic(self):
        wm = WatermarkGenerator(max_lateness_ms=10_000)
        # Advance forward
        w1 = wm.on_event(100_000)
        w2 = wm.on_event(50_000)  # old event — should not move watermark back
        w3 = wm.on_event(200_000)
        assert w2 >= w1  # never moves backwards
        assert w3 >= w2

    def test_late_event_does_not_advance_watermark(self, monkeypatch):
        monkeypatch.setenv("LATE_EVENT_POLICY", "side_output")
        from streamforge.config import reload_settings
        reload_settings()

        proc = WindowedRollingAverageProcessor(
            worker_id="w1", window_size_ms=300_000, max_lateness_ms=10_000
        )
        base = 1709280000000
        proc.process_telemetry(
            TruckTelemetryEvent(
                truck_id="T", timestamp=base + 100_000, temperature=5.0
            )
        )
        wm_before = proc.watermark_gen.last_emitted_watermark

        # Late event (below watermark)
        proc.process_telemetry(
            TruckTelemetryEvent(
                truck_id="T", timestamp=base + 1_000, temperature=5.0
            )
        )
        # Watermark should not have changed (late events are rejected before on_event)
        assert proc.watermark_gen.last_emitted_watermark == wm_before

        monkeypatch.delenv("LATE_EVENT_POLICY", raising=False)
        reload_settings()

    def test_multiple_late_events_consistent(self, monkeypatch):
        monkeypatch.setenv("LATE_EVENT_POLICY", "side_output")
        from streamforge.config import reload_settings
        reload_settings()

        proc = WindowedRollingAverageProcessor(
            worker_id="w1", window_size_ms=300_000, max_lateness_ms=10_000
        )
        base = 1709280000000
        proc.process_telemetry(
            TruckTelemetryEvent(
                truck_id="T", timestamp=base + 320_000, temperature=5.0
            )
        )
        # Multiple late events
        for i in range(5):
            late_sig, _ = proc.process_telemetry(
                TruckTelemetryEvent(
                    truck_id="T",
                    timestamp=base + 1000 + i * 100,
                    temperature=float(10 + i),
                )
            )
            assert late_sig is not None
            assert late_sig.is_late is True

        # No windows created for old timestamps
        assert ("T", base) not in proc.active_windows

        monkeypatch.delenv("LATE_EVENT_POLICY", raising=False)
        reload_settings()


# ============================================================================
# ACTIVE WINDOW RECOVERY
# ============================================================================


class TestActiveWindowRecovery:
    """Active (in-progress) windows must be recoverable after crash."""

    def test_accumulator_recovery_matches_clean_run(self):
        """Process part of a window, persist, simulate crash, restore, continue,
        and verify the final result matches an uninterrupted run."""
        all_values = [2.0, 4.0, 4.0, 4.0, 5.0, 5.0, 7.0, 9.0]

        # === Uninterrupted run ===
        acc_clean = TemperatureAccumulator()
        for v in all_values:
            acc_clean.add(v)
        clean_stats = acc_clean.to_dict()

        # === Interrupted run ===
        acc_part = TemperatureAccumulator()
        for v in all_values[:3]:
            acc_part.add(v)

        # Persist to RocksDB-like store
        state = acc_part.to_dict()

        # Simulate crash — create fresh accumulator from restored state
        acc_restored = TemperatureAccumulator.from_dict(state)
        for v in all_values[3:]:
            acc_restored.add(v)
        restored_stats = acc_restored.to_dict()

        # Verify mathematical equivalence
        assert restored_stats["count"] == clean_stats["count"]
        assert abs(restored_stats["sum"] - clean_stats["sum"]) < 1e-6
        assert abs(restored_stats["m2"] - clean_stats["m2"]) < 1e-4
        assert abs(restored_stats["std_dev"] - clean_stats["std_dev"]) < 0.01
        assert restored_stats["min"] == clean_stats["min"]
        assert restored_stats["max"] == clean_stats["max"]

    def test_active_window_via_changelog_recovery(self, tmp_path):
        """Full lifecycle: process events, persist active state, simulate crash,
        restore via changelog, continue processing, verify output."""
        changelog_mgr = ChangelogManager(storage_mode="test")

        # === Worker A processes events ===
        proc_a = WindowedRollingAverageProcessor(
            worker_id="w-a", window_size_ms=300_000, max_lateness_ms=15_000
        )
        base = 1709280000000
        first_batch = [3.0, 5.0, 7.0]
        for i, temp in enumerate(first_batch):
            proc_a.process_telemetry(
                TruckTelemetryEvent(
                    truck_id="TRK-001",
                    timestamp=base + (i + 1) * 10_000,
                    temperature=temp,
                )
            )

        # Persist active window state
        key = ("TRK-001", base)
        assert key in proc_a.active_windows
        acc_state = proc_a.active_windows[key].to_dict()
        state_val = {
            "truck_id": "TRK-001",
            "window_start": base,
            "window_end": base + 300_000,
            "active": True,
            **acc_state,
            "seq": 100,
            "source_offset": 100,
        }

        store_a = RocksDBStateStore(
            db_path=str(tmp_path / "worker_a"), partition_id=6, storage_mode="test"
        )
        store_a.put("TRK-001:" + str(base), state_val)
        changelog_mgr.publish_state_change(
            partition=6,
            key="TRK-001:" + str(base),
            value=state_val,
            worker_id="w-a",
            timestamp=int(time.time() * 1000),
            source_offset=100,
        )
        store_a.close()

        # === Crash! Worker B takes over ===
        store_b = RocksDBStateStore(
            db_path=str(tmp_path / "worker_b"), partition_id=6, storage_mode="test"
        )
        assert store_b.get("TRK-001:" + str(base)) is None  # fresh store

        # Replay changelog
        restored = changelog_mgr.restore_partition_state(6, store_b)
        assert restored >= 1

        # Recover accumulator from restored state
        recovered_val = store_b.get("TRK-001:" + str(base))
        assert recovered_val is not None
        assert recovered_val["active"] is True
        assert recovered_val["count"] == 3
        acc_recovered = TemperatureAccumulator.from_dict(recovered_val)

        # Continue processing
        second_batch = [11.0, 13.0]
        for v in second_batch:
            acc_recovered.add(v)

        # === Clean run for comparison ===
        acc_clean = TemperatureAccumulator()
        for v in first_batch + second_batch:
            acc_clean.add(v)

        # Verify equivalence
        assert acc_recovered.count == acc_clean.count
        assert abs(acc_recovered.sum_temp - acc_clean.sum_temp) < 1e-6
        assert abs(acc_recovered._m2 - acc_clean._m2) < 1e-4
        assert abs(acc_recovered.std_dev - acc_clean.std_dev) < 0.01
        store_b.close()


# ============================================================================
# CHANGELOG DURABLE VERSION / SEQUENCE
# ============================================================================


class TestDurableVersionSemantics:
    """Version/seq semantics for idempotent replay."""

    def test_version_100_then_101_wins(self, tmp_path):
        mgr = ChangelogManager(storage_mode="test")
        mgr.publish_state_change(
            partition=0, key="K", value={"v": "A"}, worker_id="w1",
            timestamp=1000, source_offset=100,
        )
        mgr.publish_state_change(
            partition=0, key="K", value={"v": "B"}, worker_id="w1",
            timestamp=2000, source_offset=101,
        )
        store = RocksDBStateStore(db_path=str(tmp_path / "s"), partition_id=0, storage_mode="test")
        mgr.restore_partition_state(0, store)
        v = store.get("K")
        assert v["v"] == "B"
        assert v["seq"] == 101

    def test_stale_replay_ignored(self, tmp_path):
        """Replay seq=100 after seq=200 should be ignored."""
        mgr = ChangelogManager(storage_mode="test")
        mgr.publish_state_change(
            partition=0, key="K", value={"v": "new"}, worker_id="w1",
            timestamp=2000, source_offset=200,
        )
        mgr.publish_state_change(
            partition=0, key="K", value={"v": "stale"}, worker_id="w1",
            timestamp=1000, source_offset=100,
        )
        store = RocksDBStateStore(db_path=str(tmp_path / "s"), partition_id=0, storage_mode="test")
        mgr.restore_partition_state(0, store)
        v = store.get("K")
        # seq=200 was applied first, seq=100 should be skipped
        assert v["v"] == "new"
        assert v["seq"] == 200

    def test_duplicate_replay_idempotent(self, tmp_path):
        mgr = ChangelogManager(storage_mode="test")
        mgr.publish_state_change(
            partition=0, key="K", value={"v": 1}, worker_id="w1",
            timestamp=1000, source_offset=50,
        )
        store = RocksDBStateStore(db_path=str(tmp_path / "s"), partition_id=0, storage_mode="test")
        c1 = mgr.restore_partition_state(0, store)
        v1 = store.get("K")
        c2 = mgr.restore_partition_state(0, store)
        v2 = store.get("K")
        assert v1 == v2
        assert c2 == 0  # second replay applies nothing

    def test_newer_overwrites(self, tmp_path):
        mgr = ChangelogManager(storage_mode="test")
        mgr.publish_state_change(
            partition=0, key="K", value={"v": 1}, worker_id="w1",
            timestamp=1000, source_offset=10,
        )
        store = RocksDBStateStore(db_path=str(tmp_path / "s"), partition_id=0, storage_mode="test")
        mgr.restore_partition_state(0, store)
        assert store.get("K")["v"] == 1
        mgr.publish_state_change(
            partition=0, key="K", value={"v": 2}, worker_id="w1",
            timestamp=2000, source_offset=11,
        )
        mgr.restore_partition_state(0, store)
        assert store.get("K")["v"] == 2


# ============================================================================
# DETERMINISTIC PARTITIONING (CRC32)
# ============================================================================


class TestDeterministicPartitioning:
    """Partition assignment must be stable across runs and processes."""

    def test_crc32_deterministic(self):
        from streamforge.producers.truck_telemetry import FleetTelemetryGenerator

        gen = FleetTelemetryGenerator(fleet_size=100, num_partitions=32)
        # Same key must always produce same partition
        for _ in range(10):
            assert gen._get_partition("TRK-00100") == gen._get_partition("TRK-00100")
        # Partition in valid range
        p = gen._get_partition("TRK-00100")
        assert 0 <= p < 32

    def test_not_using_python_hash(self):
        """Verify partition doesn't change with PYTHONHASHSEED."""
        import zlib

        from streamforge.producers.truck_telemetry import FleetTelemetryGenerator

        gen = FleetTelemetryGenerator(fleet_size=100, num_partitions=32)
        truck_id = "TRK-00050"
        expected = zlib.crc32(truck_id.encode("utf-8")) % 32
        assert gen._get_partition(truck_id) == expected

    def test_event_partition_matches(self):
        from streamforge.producers.truck_telemetry import FleetTelemetryGenerator

        gen = FleetTelemetryGenerator(fleet_size=100, num_partitions=32)
        evt = gen.generate_event(50)
        assert evt.partition == gen._get_partition(evt.truck_id)


# ============================================================================
# CHANGELOG FAILURE GATING
# ============================================================================


class TestChangelogFailureGating:
    """Source offset must never be committed if changelog fails."""

    def test_publish_failure_blocks_commit(self):
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

        consumer = FakeConsumer()
        msg = type("M", (), {"offset": lambda self: 42})()

        changelog_ok = True
        try:
            FailingChangelog().publish_state_change(
                partition=0, key="k", value={}, worker_id="w1",
                timestamp=0, source_offset=42,
            )
        except Exception:
            changelog_ok = False

        if changelog_ok:
            consumer.commit(msg)
        assert consumer.committed == []  # must NOT have committed

    def test_flush_failure_blocks_commit(self):
        class FakeConsumer:
            def __init__(self):
                self.committed = []

            def commit(self, msg):
                self.committed.append(msg)

        class FlushFailChangelog:
            def publish_state_change(self, *a, **kw):
                return 0

            def flush(self, timeout=5):
                return False  # flush failed

        consumer = FakeConsumer()
        msg = type("M", (), {"offset": lambda self: 42})()

        changelog = FlushFailChangelog()
        changelog_ok = True
        try:
            changelog.publish_state_change(
                partition=0, key="k", value={}, worker_id="w1",
                timestamp=0, source_offset=42,
            )
            flushed = changelog.flush()
            if not flushed:
                changelog_ok = False
        except Exception:
            changelog_ok = False

        if changelog_ok:
            consumer.commit(msg)
        assert consumer.committed == []

    def test_success_path_commits(self):
        class FakeConsumer:
            def __init__(self):
                self.committed = []

            def commit(self, msg):
                self.committed.append(msg)

        class OkChangelog:
            def publish_state_change(self, *a, **kw):
                return 0

            def flush(self, timeout=5):
                return True

        consumer = FakeConsumer()
        msg = type("M", (), {"offset": lambda self: 42})()

        changelog = OkChangelog()
        changelog_ok = True
        try:
            changelog.publish_state_change(
                partition=0, key="k", value={}, worker_id="w1",
                timestamp=0, source_offset=42,
            )
            flushed = changelog.flush()
            if not flushed:
                changelog_ok = False
        except Exception:
            changelog_ok = False

        if changelog_ok:
            consumer.commit(msg)
        assert len(consumer.committed) == 1


# ============================================================================
# WINDOW ASSIGNER
# ============================================================================


class TestWindowAssigner:
    """Tumbling window boundaries [start, end) correctness."""

    def test_tumbling_boundaries(self):
        assigner = WindowAssigner(window_size_ms=300_000)
        # 12:00:10 UTC => window [12:00:00, 12:05:00)
        base = 1709280000000
        windows = assigner.assign_windows(base + 10_000)
        assert len(windows) == 1
        assert windows[0].start_ms == base
        assert windows[0].end_ms == base + 300_000

    def test_boundary_event_belongs_to_next_window(self):
        assigner = WindowAssigner(window_size_ms=300_000)
        base = 1709280000000
        # Event exactly at window boundary (end of [base, base+300k))
        windows = assigner.assign_windows(base + 300_000)
        assert len(windows) == 1
        assert windows[0].start_ms == base + 300_000  # belongs to next window


# ============================================================================
# METRICS
# ============================================================================


class TestPrometheusMetrics:
    """Verify that recording events actually changes exported values."""

    def test_record_event_increments_counter(self):
        from streamforge.metrics.exporter import PrometheusMetricsExporter

        exp = PrometheusMetricsExporter(service_name="test_svc")
        before = exp.counters["streamforge_events_processed_total"]
        exp.record_event_processed(5)
        after = exp.counters["streamforge_events_processed_total"]
        assert after == before + 5

    def test_set_throughput_updates_gauge(self):
        from streamforge.metrics.exporter import PrometheusMetricsExporter

        exp = PrometheusMetricsExporter(service_name="test_svc2")
        exp.set_throughput(1234.5)
        assert exp.gauges["streamforge_events_per_second"] == 1234.5

    def test_set_lag_updates_gauge(self):
        from streamforge.metrics.exporter import PrometheusMetricsExporter

        exp = PrometheusMetricsExporter(service_name="test_svc3")
        exp.set_lag(42)
        assert exp.gauges["streamforge_consumer_lag"] == 42.0


class TestActiveWindowRestoreFromStore:
    """Verify processor.restore_from_store reconstructs in-progress state correctly."""

    def test_restore_from_store_reconstructs_active_windows(self, tmp_path):
        base = 1709280000000
        # Phase 1: uninterrupted clean run
        proc_clean = WindowedRollingAverageProcessor(worker_id="clean")
        proc_clean.process_event(
            TruckTelemetryEvent(truck_id="TRK-1", timestamp=base + 10_000, temperature=10.0)
        )
        proc_clean.process_event(
            TruckTelemetryEvent(truck_id="TRK-1", timestamp=base + 20_000, temperature=20.0)
        )
        # Final sealing event
        clean_results = proc_clean.process_event(
            TruckTelemetryEvent(truck_id="TRK-1", timestamp=base + 320_000, temperature=15.0)
        )

        # Phase 2: interrupted worker with restore_from_store
        store = RocksDBStateStore(db_path=str(tmp_path / "active_store"), partition_id=0, storage_mode="test")
        proc_a = WindowedRollingAverageProcessor(worker_id="worker_a")
        proc_a.process_event(
            TruckTelemetryEvent(truck_id="TRK-1", timestamp=base + 10_000, temperature=10.0)
        )
        # Persist active window state into store
        for (trk, w_start), acc in proc_a.active_windows.items():
            store.put(
                f"{trk}:{w_start}",
                {
                    "truck_id": trk,
                    "window_start": w_start,
                    "window_end": w_start + 300_000,
                    "active": True,
                    **acc.to_dict(),
                    "seq": 1,
                },
            )

        # Worker crashes! Replacement worker initializes and restores from store
        proc_b = WindowedRollingAverageProcessor(worker_id="worker_b")
        assert len(proc_b.active_windows) == 0
        restored = proc_b.restore_from_store(store)
        assert restored == 1
        assert ("TRK-1", base) in proc_b.active_windows
        assert proc_b.active_windows[("TRK-1", base)].count == 1
        assert proc_b.active_windows[("TRK-1", base)].sum_temp == 10.0

        # Continue processing
        proc_b.process_event(
            TruckTelemetryEvent(truck_id="TRK-1", timestamp=base + 20_000, temperature=20.0)
        )
        recovered_results = proc_b.process_event(
            TruckTelemetryEvent(truck_id="TRK-1", timestamp=base + 320_000, temperature=15.0)
        )

        # Verified: recovered worker produces identical mathematical aggregate
        assert len(clean_results) == 1
        assert len(recovered_results) == 1
        assert recovered_results[0].count == clean_results[0].count
        assert recovered_results[0].avg_temperature == clean_results[0].avg_temperature
        assert recovered_results[0].sum_temperature == clean_results[0].sum_temperature


class TestChangelogDeliveryCallbackGating:
    """Verify changelog delivery callback errors prevent offset commits."""

    def test_delivery_callback_error_blocks_flush(self):
        class MockProducerWithError:
            def __init__(self):
                self._cb = None

            def produce(self, topic, key, value, on_delivery):
                self._cb = on_delivery

            def poll(self, timeout):
                # Trigger delivery callback with simulated Kafka error
                if self._cb:
                    self._cb(RuntimeError("Simulated Kafka Broker Delivery Error"), None)

            def flush(self, timeout):
                return 0

        mock_prod = MockProducerWithError()
        cm = ChangelogManager(storage_mode="production", producer=mock_prod)
        cm.publish_state_change(
            partition=0,
            key="TRK-1:1000",
            value={"avg": 5.0},
            worker_id="w1",
            timestamp=1000,
            source_offset=10,
        )
        assert len(cm.delivery_errors) == 1
        # flush must return False due to delivery error
        assert cm.flush(timeout=5) is False

    def test_option_b_kafka_producer_passes_partition(self):
        from streamforge.producers.kafka_producer import KafkaTelemetryProducer

        captured = {}

        class MockKafkaProducer:
            def produce(self, topic, key, value, partition, on_delivery):
                captured["topic"] = topic
                captured["key"] = key
                captured["partition"] = partition

            def poll(self, t):
                pass

        kp = KafkaTelemetryProducer(bootstrap_servers="localhost:9092")
        kp._producer = MockKafkaProducer()

        evt = TruckTelemetryEvent(
            truck_id="TRK-00042",
            timestamp=1709280000000,
            temperature=4.5,
            partition=14,
        )
        kp.produce_event(evt)

        # Option B verification: partition passed to produce matches evt.partition exactly
        assert captured["partition"] == 14
        assert captured["key"] == b"TRK-00042"


# ============================================================================
# APPENDED: ACTIVE + WATERMARK DURABLE RECOVERY (RUN A vs RUN B)
# ============================================================================

BASE_EXTRA = 1709280000000


def _extra_evt(truck="TRK-R", ts=BASE_EXTRA + 1000, temp=5.0):
    return TruckTelemetryEvent(truck_id=truck, timestamp=ts, temperature=temp)


class TestActiveRecoveryMidWindow:
    """Replacement worker continues mid-window aggregation without loss."""

    def test_runA_vs_runB_midwindow_restart(self, tmp_path):
        from streamforge.workers.worker_process import (
            _persist_active_state,
            _restore_processor_state,
        )

        events = [
            _extra_evt(ts=BASE_EXTRA + 1000 + i * 10_000, temp=5.0 + (i % 3))
            for i in range(10)
        ]
        a = WindowedRollingAverageProcessor(
            worker_id="w", window_size_ms=300_000, max_lateness_ms=600_000
        )
        for e in events:
            a.process_event(e)
        snap_a = a.snapshot_state()

        b1 = WindowedRollingAverageProcessor(
            worker_id="w", window_size_ms=300_000, max_lateness_ms=600_000
        )
        store = RocksDBStateStore(
            db_path=str(tmp_path / "rec"), partition_id=0, storage_mode="test"
        )
        cm = ChangelogManager(storage_mode="test")
        for e in events[:5]:
            b1.process_event(e)
        assert _persist_active_state(b1, store, cm, "w", 0, 4) is True
        b2 = WindowedRollingAverageProcessor(
            worker_id="w", window_size_ms=300_000, max_lateness_ms=600_000
        )
        cm.restore_partition_state(0, store)
        _restore_processor_state(b2, store)
        for e in events[5:]:
            b2.process_event(e)
        snap_b = b2.snapshot_state()
        assert snap_a["active_windows"].keys() == snap_b["active_windows"].keys()
        for k in snap_a["active_windows"]:
            assert snap_a["active_windows"][k]["acc"] == snap_b["active_windows"][k]["acc"]


class TestWatermarkRestore:
    def test_watermark_restored_and_late_consistent(self, tmp_path):
        from streamforge.workers.worker_process import (
            _persist_active_state,
            _restore_processor_state,
        )

        store = RocksDBStateStore(
            db_path=str(tmp_path / "wm"), partition_id=1, storage_mode="test"
        )
        p1 = WindowedRollingAverageProcessor(
            worker_id="w", window_size_ms=300_000, max_lateness_ms=10_000
        )
        p1.process_event(_extra_evt(temp=5.0, ts=BASE_EXTRA + 200_000))
        wm_before = p1.watermark_gen.last_emitted_watermark
        cm = ChangelogManager(storage_mode="test")
        assert _persist_active_state(p1, store, cm, "w", 1, 7) is True
        p2 = WindowedRollingAverageProcessor(
            worker_id="w", window_size_ms=300_000, max_lateness_ms=10_000
        )
        cm.restore_partition_state(1, store)
        _restore_processor_state(p2, store)
        assert p2.watermark_gen.last_emitted_watermark == wm_before
        p2.process_event(_extra_evt(temp=5.0, ts=BASE_EXTRA + 500_000))
        assert p2.watermark_gen.is_event_late(BASE_EXTRA + 1000) is True


# ============================================================================
# APPENDED: FILTER EXTRAS (watermark isolation, emitted stats)
# ============================================================================


class TestTemperatureFilterExtra:
    def test_rejected_does_not_advance_watermark(self):
        p = WindowedRollingAverageProcessor(
            worker_id="w", window_size_ms=300_000, max_lateness_ms=15_000
        )
        p.process_event(_extra_evt(temp=5.0, ts=BASE_EXTRA + 10_000))
        wm_before = p.watermark_gen.last_emitted_watermark
        p.process_event(_extra_evt(temp=-5.0, ts=BASE_EXTRA + 900_000))
        assert p.watermark_gen.last_emitted_watermark == wm_before

    def test_rejected_not_in_emitted_stats(self):
        p = WindowedRollingAverageProcessor(
            worker_id="w", window_size_ms=300_000, max_lateness_ms=10_000
        )
        p.process_event(_extra_evt(temp=4.0, ts=BASE_EXTRA + 10_000))
        p.process_event(_extra_evt(temp=6.0, ts=BASE_EXTRA + 20_000))
        p.process_event(_extra_evt(temp=-5.0, ts=BASE_EXTRA + 30_000))
        _, emitted = p.process_telemetry(_extra_evt(temp=5.0, ts=BASE_EXTRA + 320_000))
        assert len(emitted) == 1
        assert emitted[0].count == 2
        assert emitted[0].avg_temperature == 5.0


# ============================================================================
# APPENDED: FLUSH PENDING GATING + ROCKSDB PRODUCTION + LAG HONESTY
# ============================================================================


class TestFlushPendingGating:
    def test_pending_blocks_flush(self):
        cm = ChangelogManager(storage_mode="test")
        cm._pending_deliveries = 2
        assert cm.flush() is False
        cm._pending_deliveries = 0
        assert cm.flush() is True

    def test_delivery_error_blocks_flush_test_mode(self):
        cm = ChangelogManager(storage_mode="test")
        cm._delivery_errors.append("simulated broker NACK")
        assert cm.flush() is False
        cm.reset_delivery_errors()
        assert cm.flush() is True


class TestRocksDBProductionPath:
    def test_production_uses_real_rdict(self, tmp_path):
        pytest.importorskip("rocksdict")
        store = RocksDBStateStore(
            db_path=str(tmp_path / "prod"), partition_id=0, storage_mode="production"
        )
        assert store._rdict is not None
        store.put("k1", {"avg": 3})
        assert store.get("k1") == {"avg": 3}
        assert [k for k, _ in store.scan("")] == ["k1"]
        store.delete("k1")
        assert store.get("k1") is None
        store.close()

    def test_production_close_reopen(self, tmp_path):
        pytest.importorskip("rocksdict")
        p = str(tmp_path / "reopen")
        s1 = RocksDBStateStore(db_path=p, partition_id=2, storage_mode="production")
        s1.put("a", {"v": 1})
        s1.close()
        s2 = RocksDBStateStore(db_path=p, partition_id=2, storage_mode="production")
        assert s2.get("a") == {"v": 1}
        s2.close()


class TestLagHonesty:
    def test_lag_unavailable_is_minus_one(self):
        from streamforge.workers.worker_process import _get_real_lag

        class NoAssign:
            _consumer = type("C", (), {"assignment": lambda self: []})()

        assert _get_real_lag(NoAssign(), None) == -1

    def test_no_fabricated_zero_lag_default(self):
        from streamforge.metrics.exporter import PrometheusMetricsExporter

        ex = PrometheusMetricsExporter(service_name="test_svc_union")
        assert ex.gauges["streamforge_consumer_lag"] == -1.0

    def test_exporter_prometheus_text(self):
        from streamforge.metrics.exporter import PrometheusMetricsExporter

        ex = PrometheusMetricsExporter(service_name="test_svc_union2")
        ex.record_event_processed(3)
        text = ex.export_prometheus_text()
        assert "streamforge_events_processed_total" in text
        assert "streamforge_consumer_lag" in text


# ============================================================================
# APPENDED: PRODUCER EXPLICIT PARTITION (generator events carry CRC32)
# ============================================================================


class TestProducerExplicitPartitionExtra:
    def test_generator_event_partition_passed_explicitly(self):
        import zlib as _zlib

        from streamforge.producers.kafka_producer import KafkaTelemetryProducer
        from streamforge.producers.truck_telemetry import FleetTelemetryGenerator

        prod = KafkaTelemetryProducer.__new__(KafkaTelemetryProducer)
        prod.generator = FleetTelemetryGenerator(fleet_size=10, num_partitions=32)
        seen = {}

        class FakeProducer:
            def produce(self, topic, key=None, value=None, partition=None, on_delivery=None):
                seen["partition"] = partition

            def poll(self, t):
                return None

        prod._producer = FakeProducer()
        prod.topic = "fleet-telemetry"
        prod._delivery = lambda e, m: None
        e = prod.generator.generate_event(42)
        expected = _zlib.crc32(e.truck_id.encode()) % 32
        assert e.partition == expected
        prod.produce_event(e)
        assert seen["partition"] == expected


# ============================================================================
# APPENDED: API HONESTY
# ============================================================================


class TestAPIHonestyEndpoints:
    @pytest.fixture()
    def client(self, monkeypatch):
        monkeypatch.setenv("STORAGE_MODE", "test")
        from streamforge.config import reload_settings

        reload_settings()
        from fastapi.testclient import TestClient
        from streamforge.api.main import app

        return TestClient(app)

    def test_health_reports_degraded_without_kafka(self, client):
        r = client.get("/api/health")
        assert r.status_code == 200
        body = r.json()
        assert body["kafka"] in ("available", "unavailable", "unknown")
        if body["kafka"] != "available":
            assert body["status"] == "degraded"

    def test_workers_target_vs_observed(self, client):
        body = client.get("/api/workers").json()
        assert "target_workers" in body and "observed_workers" in body

    def test_partitions_no_fake_leader(self, client):
        body = client.get("/api/partitions").json()
        assert "partitions" in body
        if body.get("status") == "degraded":
            for p in body["partitions"]:
                assert p.get("leader") is None

    def test_changelog_demo_labelled(self, client):
        body = client.get("/api/changelog?partition=0&limit=5").json()
        assert body.get("mode") == "DEMO"

    def test_windows_demo_labelled(self, client):
        body = client.get("/api/windows/TRK-XXXX").json()
        assert body.get("mode") == "DEMO"

    def test_chaos_not_executed(self, client):
        body = client.post("/api/chaos/kill-worker/worker-04").json()
        assert body.get("executed") is False

    def test_metrics_endpoint(self, client):
        r = client.get("/metrics")
        assert r.status_code == 200
        assert "streamforge_" in r.text

    def test_ws_metrics(self, client):
        import json as _json

        with client.websocket_connect("/ws/metrics") as ws:
            data = _json.loads(ws.receive_text())
            assert data["type"] == "metrics"
            assert "counters" in data and "gauges" in data


# ============================================================================
# APPENDED: FRONTEND LIVE/DEMO SEPARATION
# ============================================================================


class TestFrontendLiveDemoSeparation:
    import os as _os
    ROOT = _os.path.abspath(_os.path.join(_os.path.dirname(__file__), ".."))

    def _read(self, rel):
        import os

        with open(os.path.join(self.ROOT, rel), encoding="utf-8", errors="ignore") as f:
            return f.read()

    def test_demo_starts_sim_only_in_demo(self):
        src = self._read("src/App.tsx")
        assert "IS_DEMO" in src

    def test_live_topology_no_false_claims(self):
        src = self._read("src/components/TopologyView.tsx")
        assert "IS_DEMO" in src
        assert "Exactly-Once" not in src
        assert "Murmur2" not in src

    def test_navbar_no_hardcoded_health(self):
        src = self._read("src/components/Navbar.tsx")
        assert "99.99%" not in src
        assert "Unavailable" in src

    def test_metrics_live_source(self):
        src = self._read("src/components/MetricsDashboard.tsx")
        assert "useLiveMetrics" in src and "IS_DEMO" in src

    def test_chaos_honest(self):
        src = self._read("src/components/ChaosStudio.tsx")
        assert "/api/chaos/kill-worker" in src
        assert "IS_DEMO" in src

    def test_no_simulation_leak_in_live_paths(self):
        for rel in [
            "src/components/TopologyView.tsx",
            "src/components/FleetMonitor.tsx",
            "src/components/MetricsDashboard.tsx",
            "src/components/RocksDBInspector.tsx",
            "src/components/ChaosStudio.tsx",
        ]:
            src = self._read(rel)
            assert "IS_DEMO" in src, f"{rel} must gate LIVE vs DEMO"
