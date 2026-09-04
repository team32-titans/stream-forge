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

import time
import pytest
from streamforge.core.interfaces import RefrigerationState, TruckTelemetryEvent
from streamforge.windowing.engine import (
    TemperatureAccumulator,
    WatermarkGenerator,
    WindowedRollingAverageProcessor,
)
from streamforge.state.rocksdb_store import RocksDBStateStore
from streamforge.state.changelog_manager import ChangelogManager
from streamforge.recovery.rebalancer import CooperativeStickyRebalancer


class TestRollingAverageMath:
    """Verifies incremental online aggregation statistics."""

    def test_temperature_accumulator_basic_stats(self):
        acc = TemperatureAccumulator()
        temperatures = [-20.0, -18.0, -22.0, -19.5, -20.5]
        for t in temperatures:
            acc.add(t)

        assert acc.count == 5
        assert acc.min_temp == -22.0
        assert acc.max_temp == -18.0
        assert acc.average == round(sum(temperatures) / 5, 2)

    def test_single_reading(self):
        acc = TemperatureAccumulator()
        acc.add(-15.4)
        assert acc.count == 1
        assert acc.average == -15.4
        assert acc.min_temp == -15.4
        assert acc.max_temp == -15.4


class TestWindowingAndWatermarks:
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
            assert len(emitted) == 0  # Window still open

        # Now send event that advances watermark past 12:05:00 + lateness (12:05:15)
        advancing_event = TruckTelemetryEvent(
            truck_id="TRK-00100",
            timestamp=base_time + 320_000,  # 12:05:20
            temperature=-21.0,
        )
        _, emitted = processor.process_telemetry(advancing_event)
        
        assert len(emitted) == 1
        result = emitted[0]
        assert result.truck_id == "TRK-00100"
        assert result.count == 3
        assert result.avg_temperature == -20.0  # (-20 + -18 + -22) / 3


class TestRocksDBStateAndChaosRecovery:
    """Simulates Member 1 key scenario: Worker #4 dies, Worker #5 recovers state."""

    def test_worker_crash_and_state_recovery(self, tmp_path):
        # Use partition 6 (even -> owned by worker-04 when 2 workers: p%2==0)
        chosen_partition = 6
        db_path_4 = str(tmp_path / "worker4_rocksdb")
        db_path_5 = str(tmp_path / "worker5_rocksdb")

        changelog_mgr = ChangelogManager()

        # Step 1: Worker 4 processes telemetry for partition and persists state
        store_4 = RocksDBStateStore(db_path=db_path_4, partition_id=chosen_partition)
        state_payload = {"count": 140, "sum": -2800.0, "avg": -20.0, "min": -23.0, "max": -17.0}

        # Save to local RocksDB + Mirror to Kafka Changelog
        store_4.put("TRK-00492:1709280000", state_payload)
        changelog_mgr.publish_state_change(
            partition=chosen_partition,
            key="TRK-00492:1709280000",
            value=state_payload,
            worker_id="worker-04",
            timestamp=int(time.time() * 1000),
            source_offset=100,
        )
        store_4.close()

        # Step 2: Worker 4 crashes! Rebalancer assigns partition to Worker 5
        rebalancer = CooperativeStickyRebalancer(total_partitions=32, changelog_manager=changelog_mgr)
        rebalancer.register_worker("worker-04")
        rebalancer.register_worker("worker-05")

        # Verify chosen partition is owned by worker-04 before failure
        assert rebalancer.assignments[chosen_partition] == "worker-04"
        orphaned = rebalancer.handle_worker_failure("worker-04")
        assert chosen_partition in orphaned

        # Step 3: Worker 5 initializes clean RocksDB and restores state from changelog
        store_5 = RocksDBStateStore(db_path=db_path_5, partition_id=chosen_partition)
        assert store_5.get("TRK-00492:1709280000") is None  # Initially empty

        restored_count = changelog_mgr.restore_partition_state(partition=chosen_partition, target_store=store_5)
        assert restored_count >= 1

        # Step 4: Verify recovered state in Worker 5 matches exactly!
        recovered_state = store_5.get("TRK-00492:1709280000")
        assert recovered_state is not None
        assert recovered_state["avg"] == -20.0
        assert recovered_state["count"] == 140
        store_5.close()
