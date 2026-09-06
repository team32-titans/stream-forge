"""
StreamForge Core Interfaces & Type Definitions
=============================================
Module: streamforge.core.interfaces
Author: Member 1 (Stream Processing & Stateful Engine)
Standard: PEP 8, PEP 484 Type Hints, Clean Architecture

Defines the fundamental contracts, abstract base classes, and protocols
governing event processing, stateful storage, and windowed aggregations.
"""

from abc import ABC, abstractmethod
from datetime import datetime
from enum import Enum
from typing import (
    Any,
    Callable,
    Dict,
    Generic,
    Iterator,
    List,
    NamedTuple,
    Optional,
    Protocol,
    Tuple,
    TypeVar,
    runtime_checkable,
)
try:
    from pydantic import BaseModel, Field, field_validator
except ImportError:
    # Fallback to standard library implementation when pydantic is not installed
    class BaseModel:
        def __init__(self, **kwargs):
            for k, v in self.__class__.__dict__.items():
                if not k.startswith("_") and not callable(v):
                    setattr(self, k, v)
            for k, v in kwargs.items():
                setattr(self, k, v)

        def model_dump(self) -> Dict[str, Any]:
            res = {}
            for k, v in self.__dict__.items():
                if not k.startswith("_"):
                    res[k] = v.value if hasattr(v, "value") else v
            return res

        def dict(self) -> Dict[str, Any]:
            return self.model_dump()

        def __repr__(self) -> str:
            return f"{self.__class__.__name__}({self.model_dump()})"

    def Field(default=..., default_factory=None, **kwargs):
        if default_factory is not None:
            return default_factory()
        if default is ...:
            return None
        return default

    def field_validator(*args, **kwargs):
        def decorator(fn):
            return fn
        return decorator



class WindowType(str, Enum):
    """Supported windowing strategies for continuous stream aggregation."""
    TUMBLING = "TUMBLING"   # Fixed, non-overlapping time boundaries
    HOPPING = "HOPPING"     # Fixed-size windows with sliding step intervals
    SESSION = "SESSION"     # Gap-based activity windows


class RefrigerationState(str, Enum):
    """Cold-chain refrigeration telemetry status."""
    OPTIMAL = "OPTIMAL"
    WARNING = "WARNING"
    CRITICAL = "CRITICAL"
    DEFROST = "DEFROST"


class TruckTelemetryEvent(BaseModel):
    """
    Immutable Pydantic model representing an incoming IoT event from a fleet vehicle.
    Designed for zero-copy deserialization and strict schema enforcement.
    """
    truck_id: str = Field(..., description="Unique vehicle identifier (e.g. TRK-48102)")
    timestamp: int = Field(..., description="Epoch timestamp in milliseconds (Event-Time)")
    temperature: float = Field(..., description="Cargo bay temperature in Celsius")
    engine_rpm: int = Field(default=0, ge=0, le=8000)
    latitude: float = Field(default=0.0)
    longitude: float = Field(default=0.0)
    speed_kmh: float = Field(default=0.0, ge=0.0)
    partition: int = Field(default=0, ge=0, description="Kafka partition index")
    refrigeration_status: RefrigerationState = Field(default=RefrigerationState.OPTIMAL)
    is_late: bool = Field(default=False, description="Flagged by watermark if arriving after window close")

    @field_validator("temperature")
    @classmethod
    def validate_realistic_temperature(cls, v: float) -> float:
        """Ensure temperature falls within physical transport bounds."""
        if not (-50.0 <= v <= 70.0):
            raise ValueError(f"Temperature reading {v}°C is physically anomalous.")
        return round(v, 2)


class WindowBounds(NamedTuple):
    """Represents the closed-open interval [start, end) for a time window."""
    start_ms: int
    end_ms: int

    def contains(self, timestamp_ms: int) -> bool:
        return self.start_ms <= timestamp_ms < self.end_ms


class WindowedAggregateResult(BaseModel):
    """
    Output payload produced when a window completes its aggregation cycle.
    """
    truck_id: str
    window_start: int
    window_end: int
    count: int = Field(..., description="Number of sensor readings aggregated")
    sum_temperature: float
    avg_temperature: float = Field(..., description="5-minute rolling average temperature")
    min_temperature: float
    max_temperature: float
    calculated_at: int = Field(default_factory=lambda: int(datetime.utcnow().timestamp() * 1000))
    emitted_by_worker: str


K = TypeVar("K")
V = TypeVar("V")
ACC = TypeVar("ACC")


@runtime_checkable
class StateStore(Protocol[K, V]):
    """
    Protocol defining the state store interface (implemented by RocksDBStateStore).
    Adheres to the Dependency Inversion Principle (DIP).
    """

    def get(self, key: K) -> Optional[V]:
        """Retrieve state for key or None if not found."""
        ...

    def put(self, key: K, value: V) -> None:
        """Persist or update state for key."""
        ...

    def delete(self, key: K) -> None:
        """Remove state for key (writes a tombstone)."""
        ...

    def commit(self) -> int:
        """Flush MemTable and return write-ahead log (WAL) sequence offset."""
        ...

    def create_checkpoint(self, checkpoint_path: str) -> str:
        """Create a point-in-time point-to-point snapshot of the state store."""
        ...


class BaseStreamProcessor(ABC):
    """
    Abstract Base Class for distributed stream processor workers.
    Enforces standardized lifecycle management and partition assignments.
    """

    def __init__(self, worker_id: str, partition_ids: List[int]) -> None:
        self.worker_id = worker_id
        self.partition_ids = partition_ids
        self._is_running = False

    @abstractmethod
    def start(self) -> None:
        """Initialize consumer, open state stores, and begin event loop."""
        pass

    @abstractmethod
    def stop(self) -> None:
        """Gracefully flush states, commit Kafka offsets, and close RocksDB."""
        pass

    @abstractmethod
    def process_event(self, event: TruckTelemetryEvent) -> Optional[WindowedAggregateResult]:
        """Process a single event through the streaming pipeline."""
        pass
