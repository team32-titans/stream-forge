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
    # Robust standard library fallback implementation when pydantic is not installed
    class _FieldInfo:
        def __init__(self, default=..., default_factory=None, description=None, **kwargs):
            self.default = default
            self.default_factory = default_factory
            self.description = description
            self.extra = kwargs

    def Field(default=..., default_factory=None, description=None, **kwargs):
        return _FieldInfo(default=default, default_factory=default_factory, description=description, **kwargs)

    def field_validator(*field_names, **kwargs):
        def decorator(fn):
            target = getattr(fn, "__func__", fn)
            try:
                target.__field_validator_fields__ = field_names
            except AttributeError:
                pass
            try:
                fn.__field_validator_fields__ = field_names
            except AttributeError:
                pass
            return fn
        return decorator

    class BaseModel:
        def __init__(self, **kwargs):
            cls = self.__class__
            annotations = getattr(cls, "__annotations__", {})

            for field_name in annotations:
                if field_name.startswith("_"):
                    continue
                class_attr = cls.__dict__.get(field_name, ...)
                if field_name in kwargs:
                    val = kwargs[field_name]
                elif isinstance(class_attr, _FieldInfo):
                    if class_attr.default_factory is not None:
                        val = class_attr.default_factory()
                    elif class_attr.default is not ...:
                        val = class_attr.default
                    else:
                        raise ValueError(f"Field '{field_name}' is required for {cls.__name__}")
                elif class_attr is not ...:
                    val = class_attr
                else:
                    raise ValueError(f"Field '{field_name}' is required for {cls.__name__}")
                setattr(self, field_name, val)

            for k, v in kwargs.items():
                if k not in annotations and not k.startswith("_"):
                    setattr(self, k, v)

            for attr_name, class_val in cls.__dict__.items():
                raw_func = getattr(class_val, "__func__", class_val)
                validator_fields = (
                    getattr(class_val, "__field_validator_fields__", None)
                    or getattr(raw_func, "__field_validator_fields__", None)
                )
                if validator_fields:
                    for target_field in validator_fields:
                        if hasattr(self, target_field):
                            curr_val = getattr(self, target_field)
                            if isinstance(class_val, classmethod) or (
                                hasattr(raw_func, "__code__")
                                and raw_func.__code__.co_varnames
                                and raw_func.__code__.co_varnames[0] == "cls"
                            ):
                                validated = raw_func(cls, curr_val)
                            else:
                                validated = raw_func(self, curr_val)
                            setattr(self, target_field, validated)

        def model_dump(self) -> Dict[str, Any]:
            res = {}
            for k, v in self.__dict__.items():
                if k.startswith("_") or callable(v) or isinstance(v, (classmethod, staticmethod)):
                    continue
                if hasattr(v, "value"):
                    res[k] = v.value
                elif hasattr(v, "model_dump"):
                    res[k] = v.model_dump()
                elif hasattr(v, "dict"):
                    res[k] = v.dict()
                else:
                    res[k] = v
            return res

        def dict(self) -> Dict[str, Any]:
            return self.model_dump()

        def __repr__(self) -> str:
            fields_str = ", ".join(f"{k}={v!r}" for k, v in self.model_dump().items())
            return f"{self.__class__.__name__}({fields_str})"



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
