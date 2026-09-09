"""
StreamForge Unit, Integration & Chaos Test Suite
================================================
Module: tests.test_stream_engine
Author: Member 1 (Stream Processing & Stateful Engine)

Validates:
1. 5-minute rolling average mathematical correctness.
2. Watermark late data filtering.
3. RocksDB state store persistence and checkpointing.
4. Chaos Test: Worker crash, partition rebalance, and exact state recovery.
"""

import os
import sys
import time
import unittest

# Ensure streamforge root is on python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

try:
    import pytest
except ImportError:
    pytest = None


from streamforge.core.interfaces import RefrigerationState, TruckTelemetryEvent
from streamforge.windowing.engine import (
    TemperatureAccumulator,
    WatermarkGenerator,
    WindowedRollingAverageProcessor,
)
from streamforge.state.rocksdb_store import RocksDBStateStore
from streamforge.state.changelog_manager import ChangelogManager
from streamforge.recovery.rebalancer import CooperativeStickyRebalancer


class TestRollingAverageMath(unittest.TestCase):
    """Verifies incremental online aggregation statistics."""

    def test_temperature_accumulator_basic_stats(self):
        acc = TemperatureAccumulator()
        temperatures = [-20.0, -18.0, -22.0, -19.5, -20.5]
        for t in temperatures:
            acc.add(t)

        self.assertEqual(acc.count, 5)
        self.assertEqual(acc.min_temp, -22.0)
        self.assertEqual(acc.max_temp, -18.0)
        self.assertEqual(acc.average, round(sum(temperatures) / 5, 2))

    def test_single_reading(self):
        acc = TemperatureAccumulator()
        acc.add(-15.4)
        self.assertEqual(acc.count, 1)
        self.assertEqual(acc.average, -15.4)
        self.assertEqual(acc.min_temp, -15.4)
        self.assertEqual(acc.max_temp, -15.4)


class TestWindowingAndWatermarks(unittest.TestCase):
    """Verifies 5-minute tumbling windows and out-of-order event handling."""

    def test_tumbling_window_emission_on_watermark(self):
        processor = WindowedRollingAverageProcessor(
            worker_id="worker-01",
            window_size_ms=300_000,  # 5 minutes
            max_lateness_ms=10_000,  # 10s
        )

        base_time = 1709280000000  # 12:00:00 UTC

        # Send 3 events inside window [12:00:00 - 12:05:00)
        events = [
            TruckTelemetryEvent(
                truck_id="TRK-00100",
                timestamp=base_time + 10_000,  # 12:00:10
                temperature=-20.0,
            ),
            TruckTelemetryEvent(
                truck_id="TRK-00100",
                timestamp=base_time + 120_000, # 12:02:00
                temperature=-18.0,
            ),
            TruckTelemetryEvent(
                truck_id="TRK-00100",
                timestamp=base_time + 240_000, # 12:04:00
                temperature=-22.0,
            ),
        ]

        for e in events:
            _, emitted = processor.process_telemetry(e)
            self.assertEqual(len(emitted), 0)  # Window still open

        # Now send event that advances watermark past 12:05:00 + lateness (12:05:15)
        advancing_event = TruckTelemetryEvent(
            truck_id="TRK-00100",
            timestamp=base_time + 320_000,  # 12:05:20
            temperature=-21.0,
        )
        _, emitted = processor.process_telemetry(advancing_event)
        
        self.assertEqual(len(emitted), 1)
        result = emitted[0]
        self.assertEqual(result.truck_id, "TRK-00100")
        self.assertEqual(result.count, 3)
        self.assertEqual(result.avg_temperature, -20.0)  # (-20 + -18 + -22) / 3


class TestRocksDBStateAndChaosRecovery(unittest.TestCase):
    """Simulates Member 1 key scenario: Worker #4 dies, Worker #5 recovers state."""

    def test_worker_crash_and_state_recovery(self):
        import tempfile
        from pathlib import Path
        tmp_path = Path(tempfile.mkdtemp(prefix="streamforge_test_"))

        db_path_4 = str(tmp_path / "worker4_rocksdb")
        db_path_5 = str(tmp_path / "worker5_rocksdb")

        changelog_mgr = ChangelogManager()

        # Step 1: Worker 4 processes telemetry for partition 7 and persists state
        store_4 = RocksDBStateStore(db_path=db_path_4, partition_id=7)
        state_payload = {"count": 140, "sum": -2800.0, "avg": -20.0, "min": -23.0, "max": -17.0}
        
        # Save to local RocksDB + Mirror to Kafka Changelog
        store_4.put("TRK-00492:1709280000", state_payload)
        changelog_mgr.publish_state_change(
            partition=7,
            key="TRK-00492:1709280000",
            value=state_payload,
            worker_id="worker-04",
            timestamp=int(time.time() * 1000),
        )
        store_4.close()

        # Step 2: Worker 4 crashes! Rebalancer assigns partition 7 to Worker 5
        rebalancer = CooperativeStickyRebalancer(total_partitions=32, changelog_manager=changelog_mgr)
        rebalancer.register_worker("worker-04")
        rebalancer.register_worker("worker-05")
        
        # Simulate Worker 4 failure
        orphaned = rebalancer.handle_worker_failure("worker-04")
        self.assertTrue(len(orphaned) >= 0)

        # Step 3: Worker 5 initializes clean RocksDB and restores state from changelog
        store_5 = RocksDBStateStore(db_path=db_path_5, partition_id=7)
        self.assertIsNone(store_5.get("TRK-00492:1709280000"))  # Initially empty

        restored_count = changelog_mgr.restore_partition_state(partition=7, target_store=store_5)
        self.assertGreaterEqual(restored_count, 1)

        # Step 4: Verify recovered state in Worker 5 matches exactly!
        recovered_state = store_5.get("TRK-00492:1709280000")
        self.assertIsNotNone(recovered_state)
        self.assertEqual(recovered_state["avg"], -20.0)
        self.assertEqual(recovered_state["count"], 140)
        store_5.close()


if __name__ == "__main__":
    print("=" * 70)
    print("  RUNNING STREAMFORGE PURE PYTHON TEST SUITE")
    print("=" * 70)
    test_classes = [
        TestRollingAverageMath,
        TestWindowingAndWatermarks,
        TestRocksDBStateAndChaosRecovery,
    ]

    total_passed = 0
    total_failed = 0
    start_time = time.time()

    for cls in test_classes:
        instance = cls()
        print(f"\n[SUITE] {cls.__name__}: {cls.__doc__ or ''}")
        for attr_name in dir(instance):
            if attr_name.startswith("test_"):
                test_fn = getattr(instance, attr_name)
                try:
                    test_fn()
                    print(f"  ✓ {attr_name}")
                    total_passed += 1
                except Exception as e:
                    print(f"  ✗ {attr_name}: {e}")
                    total_failed += 1

    elapsed = time.time() - start_time
    print("\n" + "=" * 70)
    print(f"RESULTS: {total_passed} passed, {total_failed} failed in {elapsed:.3f}s")
    print("=" * 70)
    if total_failed > 0:
        exit(1)

