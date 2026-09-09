"""Core interfaces, protocols, and data models for StreamForge."""
from streamforge.core.interfaces import (
    RefrigerationState,
    StateStore,
    TruckTelemetryEvent,
    WindowBounds,
    WindowedAggregateResult,
    WindowType,
)

__all__ = [
    "RefrigerationState",
    "StateStore",
    "TruckTelemetryEvent",
    "WindowBounds",
    "WindowedAggregateResult",
    "WindowType",
]
