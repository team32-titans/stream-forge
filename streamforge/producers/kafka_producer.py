"""
Real Kafka producer for fleet telemetry — confluent-kafka only.
Uses FleetTelemetryGenerator for physics, adds serialization + idempotent produce.
"""
from __future__ import annotations

import json
import logging
from typing import Callable, Optional

from streamforge.config import get_settings
from streamforge.core.interfaces import TruckTelemetryEvent
from streamforge.producers.truck_telemetry import FleetTelemetryGenerator

logger = logging.getLogger("streamforge.producer")


class KafkaTelemetryProducer:
    """
    Wraps confluent_kafka.Producer with:
      - idempotent + acks=all
      - lz4 compression
      - batching (linger.ms, batch.size)
      - delivery reports
    Key = truck_id (partition affinity), value = JSON of TruckTelemetryEvent.
    """

    def __init__(
        self,
        bootstrap_servers: str | None = None,
        fleet_size: int | None = None,
        num_partitions: int | None = None,
        on_delivery: Optional[Callable] = None,
    ) -> None:
        s = get_settings()
        self.bootstrap_servers = bootstrap_servers or s.kafka_bootstrap_servers
        self.topic = s.kafka_topic
        self.generator = FleetTelemetryGenerator(
            fleet_size=fleet_size or s.fleet_size,
            num_partitions=num_partitions or s.kafka_partitions,
        )
        self._on_delivery = on_delivery
        self._produced = 0
        self._failed = 0

        try:
            from confluent_kafka import Producer

            conf = {
                "bootstrap.servers": self.bootstrap_servers,
                "acks": "all",
                "enable.idempotence": True,
                "compression.type": "lz4",
                "linger.ms": 10,
                "batch.size": 65536,
                "retries": 5,
                "retry.backoff.ms": 200,
            }
            self._producer: Optional[Producer] = Producer(conf)
        except Exception as e:
            logger.warning("confluent-kafka Producer init failed: %s (is broker up?)", e)
            self._producer = None

    def _delivery(self, err, msg) -> None:
        if err is not None:
            self._failed += 1
            logger.error("delivery failed: %s", err)
        else:
            self._produced += 1
        if self._on_delivery:
            try:
                self._on_delivery(err, msg)
            except Exception:
                pass

    def _serialize(self, evt: TruckTelemetryEvent) -> tuple[bytes, bytes]:
        key = evt.truck_id.encode("utf-8")
        value = json.dumps(evt.model_dump(mode="json")).encode("utf-8")
        return key, value

    def produce_event(self, evt: TruckTelemetryEvent) -> None:
        if self._producer is None:
            raise RuntimeError("Kafka producer not initialized — check bootstrap.servers and confluent-kafka install")
        key, value = self._serialize(evt)
        # Use truck_id as key for partition affinity (also respects hash partitioning in generator)
        self._producer.produce(self.topic, key=key, value=value, on_delivery=self._delivery)
        # Serve delivery callbacks
        self._producer.poll(0)

    def produce_batch(self, batch_size: int = 5000, inject_anomaly_rate: float = 0.02) -> int:
        """Generate + produce batch_size events; returns count queued."""
        batch = self.generator.stream_batch(batch_size=batch_size)
        for evt in batch:
            # Respect inject rate already applied in generator
            self.produce_event(evt)
        if self._producer:
            self._producer.flush(10)
        return batch_size

    def flush(self, timeout: float = 10) -> int:
        if self._producer is None:
            return 0
        return self._producer.flush(timeout)

    def close(self) -> None:
        try:
            if self._producer:
                self._producer.flush(5)
        finally:
            self._producer = None

    @property
    def stats(self) -> dict:
        return {"produced": self._produced, "failed": self._failed, "topic": self.topic}
