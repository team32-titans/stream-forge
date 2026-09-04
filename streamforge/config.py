"""
StreamForge Centralized Configuration
====================================
Single source of truth for all environment-driven settings.
Uses pydantic-settings with python-dotenv.
All modules must import get_settings() instead of hardcoding values.

Storage Mode:
  - "production": real RocksDB (rocksdict) inside Docker/Linux worker. Raises if unavailable.
  - "demo": in-memory dict fallback for UI demo / Windows without Docker.
  - "test": in-memory for pytest.

Watermark semantics: WATERMARK = max_event_time - MAX_LATENESS_MS, monotonic.
Late events: timestamp < watermark -> flagged is_late, routed per LATE_EVENT_POLICY.
"""
from __future__ import annotations

import os
from functools import lru_cache
from typing import Literal

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    # --- Kafka ---
    kafka_bootstrap_servers: str = Field(default="localhost:9092", alias="KAFKA_BOOTSTRAP_SERVERS")
    kafka_topic: str = Field(default="fleet-telemetry", alias="KAFKA_TOPIC")
    kafka_changelog_topic: str = Field(default="streamforge.truck_state.changelog", alias="KAFKA_CHANGELOG_TOPIC")
    kafka_consumer_group: str = Field(default="streamforge-workers", alias="KAFKA_CONSUMER_GROUP")
    kafka_partitions: int = Field(default=32, alias="KAFKA_PARTITIONS")
    kafka_replication_factor: int = Field(default=1, alias="KAFKA_REPLICATION_FACTOR")

    # --- Windowing ---
    window_size_ms: int = Field(default=300_000, alias="WINDOW_SIZE_MS")  # 5 minutes tumbling
    max_lateness_ms: int = Field(default=15_000, alias="MAX_LATENESS_MS")  # watermark grace
    watermark_idle_timeout_ms: int = Field(default=30_000, alias="WATERMARK_IDLE_TIMEOUT_MS")
    late_event_policy: Literal["drop", "side_output"] = Field(default="side_output", alias="LATE_EVENT_POLICY")

    # --- Workers / State ---
    storage_mode: Literal["production", "demo", "test"] = Field(default="demo", alias="STORAGE_MODE")
    worker_id: str = Field(default="worker-01", alias="WORKER_ID")
    rocksdb_base_path: str = Field(default="/tmp/streamforge_rocksdb", alias="ROCKSDB_BASE_PATH")
    # Alias for compact env: WORKERS
    target_workers: int = Field(default=20, alias="WORKERS")

    # --- Fleet ---
    fleet_size: int = Field(default=50_000, alias="FLEET_SIZE")

    # --- API / Metrics ---
    api_host: str = Field(default="0.0.0.0", alias="API_HOST")
    api_port: int = Field(default=8000, alias="API_PORT")
    metrics_port: int = Field(default=9102, alias="METRICS_PORT")
    log_level: str = Field(default="INFO", alias="LOG_LEVEL")

    # --- Frontend ---
    vite_api_url: str = Field(default="http://localhost:8000", alias="VITE_API_URL")

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    @property
    def is_production(self) -> bool:
        return self.storage_mode == "production"

    @property
    def is_demo(self) -> bool:
        return self.storage_mode == "demo"


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()


def reload_settings() -> Settings:
    get_settings.cache_clear()
    return get_settings()
