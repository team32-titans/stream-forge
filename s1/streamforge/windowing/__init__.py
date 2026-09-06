"""Windowing and online stream aggregation algorithms."""
from streamforge.windowing.engine import (
    TemperatureAccumulator,
    WatermarkGenerator,
    WindowedRollingAverageProcessor,
)

__all__ = [
    "TemperatureAccumulator",
    "WatermarkGenerator",
    "WindowedRollingAverageProcessor",
]
