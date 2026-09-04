"""
Real Prometheus exporter using prometheus_client.
Keeps dict facade for backwards compat + real registry.
"""
from typing import Dict
import time

try:
    from prometheus_client import Counter, Gauge, Histogram, REGISTRY, generate_latest, CONTENT_TYPE_LATEST

    _PROM_AVAILABLE = True
except Exception:
    Counter = Gauge = Histogram = None  # type: ignore
    _PROM_AVAILABLE = False
    generate_latest = None  # type: ignore
    CONTENT_TYPE_LATEST = "text/plain"


class PrometheusMetricsExporter:
    def __init__(self, service_name: str = "streamforge_engine") -> None:
        self.service_name = service_name
        # Dict facade for existing code
        self.counters: Dict[str, float] = {
            "streamforge_events_processed_total": 0.0,
            "streamforge_windows_emitted_total": 0.0,
            "streamforge_late_events_total": 0.0,
            "streamforge_rebalances_total": 0.0,
            "streamforge_events_failed_total": 0.0,
            "streamforge_partition_events_total": 0.0,
            "streamforge_window_updates_total": 0.0,
            "streamforge_recovery_events_total": 0.0,
        }
        self.gauges: Dict[str, float] = {
            "streamforge_throughput_events_per_sec": 0.0,
            "streamforge_events_per_second": 0.0,
            "streamforge_active_workers": 0.0,
            "streamforge_healthy_workers": 0.0,
            "streamforge_total_partition_lag": 0.0,
            "streamforge_consumer_lag": 0.0,
            "streamforge_rocksdb_memtable_mb": 0.0,
            "streamforge_p99_latency_ms": 0.0,
            "streamforge_worker_up": 0.0,
            "streamforge_processing_latency_seconds": 0.0,
        }
        self._hist = None
        if _PROM_AVAILABLE:
            try:
                # Reuse or create
                self._c_processed = Counter("streamforge_events_processed_total", "Total events processed", ["service"])
                self._c_failed = Counter("streamforge_events_failed_total", "Failed events", ["service"])
                self._g_throughput = Gauge("streamforge_events_per_second", "Throughput", ["service"])
                self._g_lag = Gauge("streamforge_consumer_lag", "Consumer lag", ["service"])
                self._g_worker_up = Gauge("streamforge_worker_up", "Worker up", ["service"])
                self._g_p99 = Gauge("streamforge_p99_latency_ms", "p99 latency ms", ["service"])
                self._hist = Histogram("streamforge_processing_latency_seconds", "Processing latency", ["service"], buckets=[0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1])
            except Exception:
                # Already registered
                pass

    def record_event_processed(self, count: int = 1) -> None:
        self.counters["streamforge_events_processed_total"] += count
        if _PROM_AVAILABLE and hasattr(self, "_c_processed"):
            try:
                self._c_processed.labels(service=self.service_name).inc(count)
            except Exception:
                pass

    def set_throughput(self, events_per_sec: float) -> None:
        self.gauges["streamforge_throughput_events_per_sec"] = events_per_sec
        self.gauges["streamforge_events_per_second"] = events_per_sec
        if _PROM_AVAILABLE and hasattr(self, "_g_throughput"):
            try:
                self._g_throughput.labels(service=self.service_name).set(events_per_sec)
            except Exception:
                pass

    def set_lag(self, total_lag: int) -> None:
        self.gauges["streamforge_total_partition_lag"] = float(total_lag)
        self.gauges["streamforge_consumer_lag"] = float(total_lag)
        if _PROM_AVAILABLE and hasattr(self, "_g_lag"):
            try:
                self._g_lag.labels(service=self.service_name).set(float(total_lag))
            except Exception:
                pass

    def observe_latency(self, seconds: float) -> None:
        self.gauges["streamforge_processing_latency_seconds"] = seconds
        if self._hist is not None:
            try:
                self._hist.labels(service=self.service_name).observe(seconds)
            except Exception:
                pass

    def export_prometheus_text(self) -> str:
        if _PROM_AVAILABLE and generate_latest:
            try:
                return generate_latest().decode("utf-8")
            except Exception:
                pass
        lines = []
        for metric, val in self.counters.items():
            lines.append(f"# TYPE {metric} counter")
            lines.append(f'{metric}{{service="{self.service_name}"}} {val}')
        for metric, val in self.gauges.items():
            lines.append(f"# TYPE {metric} gauge")
            lines.append(f'{metric}{{service="{self.service_name}"}} {val}')
        return "\n".join(lines)


# Singleton for workers/api
_singleton: PrometheusMetricsExporter | None = None


def get_exporter(service_name: str = "streamforge_engine") -> PrometheusMetricsExporter:
    global _singleton
    if _singleton is None:
        _singleton = PrometheusMetricsExporter(service_name=service_name)
    return _singleton
