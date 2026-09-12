"""
StreamForge - Real-Time Distributed Stateful Event Streaming Engine
===================================================================
A production-grade pure Python stream processing framework for high-throughput
IoT telemetry, featuring 5-minute tumbling/hopping windows, embedded RocksDB
state stores with Write-Ahead Logging (WAL), cooperative rebalancing, and
Prometheus metrics export.
"""

__version__ = "1.0.0"
__author__ = "StreamForge Core Team"

from streamforge.core.interfaces import (
    RefrigerationState,
    StateStore,
    TruckTelemetryEvent,
    WindowBounds,
    WindowedAggregateResult,
    WindowType,
)
from streamforge.windowing.engine import (
    TemperatureAccumulator,
    WatermarkGenerator,
    WindowedRollingAverageProcessor,
)
from streamforge.state.rocksdb_store import RocksDBOptions, RocksDBStateStore
from streamforge.state.changelog_manager import ChangelogManager
from streamforge.recovery.rebalancer import CooperativeStickyRebalancer
from streamforge.producers.truck_telemetry import SyntheticFleetProducer
from streamforge.metrics.exporter import PrometheusMetricsExporter

__all__ = [
    "RefrigerationState",
    "StateStore",
    "TruckTelemetryEvent",
    "WindowBounds",
    "WindowedAggregateResult",
    "WindowType",
    "TemperatureAccumulator",
    "WatermarkGenerator",
    "WindowedRollingAverageProcessor",
    "RocksDBOptions",
    "RocksDBStateStore",
    "ChangelogManager",
    "CooperativeStickyRebalancer",
    "SyntheticFleetProducer",
    "PrometheusMetricsExporter",
]
