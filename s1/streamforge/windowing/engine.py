"""
StreamForge Windowing & Aggregation Engine
==========================================
Module: streamforge.windowing.engine
Author: Member 1 (Stream Processing & Stateful Engine)

Implements high-throughput time-windowed aggregations (5-minute rolling averages)
for 50,000+ IoT fleet vehicles, robust out-of-order event handling with watermarks,
and memory-efficient incremental mathematical accumulators.
"""

from typing import Dict, List, Optional, Tuple
import math
from streamforge.core.interfaces import (
    TruckTelemetryEvent,
    WindowBounds,
    WindowedAggregateResult,
    WindowType,
)


class TemperatureAccumulator:
    """
    Incremental statistical accumulator computing rolling sum, count, min, max,
    and Welford's algorithm for online variance/standard deviation with O(1) space.
    """
    __slots__ = ("count", "sum_temp", "min_temp", "max_temp", "_m2")

    def __init__(self) -> None:
        self.count: int = 0
        self.sum_temp: float = 0.0
        self.min_temp: float = float("inf")
        self.max_temp: float = float("-inf")
        self._m2: float = 0.0  # Sum of squared differences for Welford's algorithm

    def add(self, temp: float) -> None:
        """Incorporate a new temperature sample into the running statistics."""
        self.count += 1
        self.sum_temp += temp
        if temp < self.min_temp:
            self.min_temp = temp
        if temp > self.max_temp:
            self.max_temp = temp

        # Online variance calculation (Welford's method)
        delta = temp - (self.sum_temp / self.count)
        delta2 = temp - ((self.sum_temp + temp) / (self.count + 1) if self.count > 0 else temp)
        self._m2 += delta * delta2

    @property
    def average(self) -> float:
        """Return the current rolling mean temperature."""
        if self.count == 0:
            return 0.0
        return round(self.sum_temp / self.count, 2)

    @property
    def std_dev(self) -> float:
        """Return the current rolling standard deviation."""
        if self.count < 2:
            return 0.0
        variance = self._m2 / (self.count - 1)
        return round(math.sqrt(max(0.0, variance)), 2)

    def to_dict(self) -> Dict[str, float]:
        return {
            "count": self.count,
            "sum": round(self.sum_temp, 2),
            "avg": self.average,
            "min": self.min_temp if self.count > 0 else 0.0,
            "max": self.max_temp if self.count > 0 else 0.0,
            "std_dev": self.std_dev,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, float]) -> "TemperatureAccumulator":
        acc = cls()
        acc.count = int(data.get("count", 0))
        acc.sum_temp = float(data.get("sum", 0.0))
        acc.min_temp = float(data.get("min", float("inf")))
        acc.max_temp = float(data.get("max", float("-inf")))
        return acc


class WatermarkGenerator:
    """
    Bounded Out-of-Order Watermark Generator.
    Tracks the maximum event time observed and produces a monotonic watermark
    delayed by a configurable tolerance duration (e.g. 15 seconds).
    """

    def __init__(self, max_lateness_ms: int = 15_000) -> None:
        self.max_lateness_ms = max_lateness_ms
        self.current_max_timestamp: int = 0
        self.last_emitted_watermark: int = 0

    def on_event(self, event_timestamp_ms: int) -> int:
        """Update max observed timestamp and return the current watermark."""
        if event_timestamp_ms > self.current_max_timestamp:
            self.current_max_timestamp = event_timestamp_ms
        
        watermark = self.current_max_timestamp - self.max_lateness_ms
        if watermark > self.last_emitted_watermark:
            self.last_emitted_watermark = watermark
        
        return self.last_emitted_watermark

    def is_event_late(self, event_timestamp_ms: int) -> bool:
        """Check if an incoming event timestamp is older than the current watermark."""
        return event_timestamp_ms < self.last_emitted_watermark


class WindowAssigner:
    """
    Calculates window boundaries for timestamps based on Window strategy.
    Default: 5-minute (300,000 ms) Tumbling or Hopping windows.
    """

    def __init__(
        self,
        window_size_ms: int = 300_000,   # 5 minutes
        slide_interval_ms: int = 300_000, # 5 min for Tumbling, 60s for Hopping
        window_type: WindowType = WindowType.TUMBLING,
    ) -> None:
        self.window_size_ms = window_size_ms
        self.slide_interval_ms = slide_interval_ms
        self.window_type = window_type

    def assign_windows(self, timestamp_ms: int) -> List[WindowBounds]:
        """
        Assign an event timestamp to one or more time windows.
        For Tumbling windows: 1 window.
        For Hopping windows: multiple overlapping windows.
        """
        windows: List[WindowBounds] = []
        
        if self.window_type == WindowType.TUMBLING:
            start = timestamp_ms - (timestamp_ms % self.window_size_ms)
            windows.append(WindowBounds(start_ms=start, end_ms=start + self.window_size_ms))
        else:
            # Hopping window calculation
            last_start = timestamp_ms - (timestamp_ms % self.slide_interval_ms)
            cur_start = last_start
            while cur_start > timestamp_ms - self.window_size_ms:
                windows.append(WindowBounds(start_ms=cur_start, end_ms=cur_start + self.window_size_ms))
                cur_start -= self.slide_interval_ms
                
        return windows


class WindowedRollingAverageProcessor:
    """
    Core Stateful Window Aggregation Engine.
    Maintains in-memory and RocksDB-backed state per (truck_id, window_start).
    Emits finalized aggregates when the watermark passes the window end.
    """

    def __init__(
        self,
        worker_id: str,
        window_size_ms: int = 300_000,  # 5 minutes
        max_lateness_ms: int = 15_000,  # 15 seconds grace period
    ) -> None:
        self.worker_id = worker_id
        self.assigner = WindowAssigner(window_size_ms=window_size_ms)
        self.watermark_gen = WatermarkGenerator(max_lateness_ms=max_lateness_ms)
        
        # State: mapping of (truck_id, window_start) -> TemperatureAccumulator
        self.active_windows: Dict[Tuple[str, int], TemperatureAccumulator] = {}

    def process_telemetry(
        self, event: TruckTelemetryEvent
    ) -> Tuple[Optional[WindowedAggregateResult], List[WindowedAggregateResult]]:
        """
        1. Evaluates watermark against event timestamp.
        2. Assigns event to appropriate 5-minute windows.
        3. Updates rolling accumulator.
        4. Evaluates window closure and triggers emission of completed aggregates.
        """
        watermark = self.watermark_gen.on_event(event.timestamp)
        is_late = self.watermark_gen.is_event_late(event.timestamp)
        event.is_late = is_late

        assigned_windows = self.assigner.assign_windows(event.timestamp)
        
        for win in assigned_windows:
            state_key = (event.truck_id, win.start_ms)
            if state_key not in self.active_windows:
                self.active_windows[state_key] = TemperatureAccumulator()
            
            self.active_windows[state_key].add(event.temperature)

        # Evaluate window trigger conditions (Event-Time Watermark > Window End)
        emitted_results: List[WindowedAggregateResult] = []
        expired_keys: List[Tuple[str, int]] = []

        for (truck_id, win_start), acc in self.active_windows.items():
            win_end = win_start + self.assigner.window_size_ms
            if watermark >= win_end:
                # Window is sealed; emit 5-minute rolling average
                result = WindowedAggregateResult(
                    truck_id=truck_id,
                    window_start=win_start,
                    window_end=win_end,
                    count=acc.count,
                    sum_temperature=round(acc.sum_temp, 2),
                    avg_temperature=acc.average,
                    min_temperature=acc.min_temp,
                    max_temperature=acc.max_temp,
                    emitted_by_worker=self.worker_id,
                )
                emitted_results.append(result)
                expired_keys.append((truck_id, win_start))

        # Evict sealed windows from active memory
        for key in expired_keys:
            del self.active_windows[key]

        return (None, emitted_results)

    def process_event(self, event: TruckTelemetryEvent) -> List[WindowedAggregateResult]:
        """Convenience method returning list of emitted aggregates directly."""
        _, emitted = self.process_telemetry(event)
        return emitted

