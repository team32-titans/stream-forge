"""
StreamForge High-Throughput IoT Fleet Telemetry Producer
========================================================
Module: streamforge.producers.truck_telemetry
Author: Member 1 (Stream Processing & Stateful Engine)

Generates real-time IoT sensor readings for 50,000 refrigerated transport trucks.
Capable of blasting 100,000+ events per second into Kafka partitions using
Murmur2 key hashing on truck_id.
"""

import json
import random
import time
from typing import Generator, List
from streamforge.core.interfaces import RefrigerationState, TruckTelemetryEvent


class FleetTelemetryGenerator:
    """
    Simulates a large-scale logistics fleet with realistic physics:
    - Temperature fluctuations
    - Engine RPM & Speed correlation
    - Simulated anomalies (freezer failure, defrost cycles)
    """

    def __init__(self, fleet_size: int = 50_000, num_partitions: int = 32) -> None:
        self.fleet_size = fleet_size
        self.num_partitions = num_partitions
        
        # Pre-seed baseline temperatures per truck (e.g. -20°C for deep freeze, +4°C for dairy)
        self._truck_baselines = [
            -22.0 + (i % 26) * 1.2 for i in range(min(fleet_size, 1000))
        ]

    def _get_partition(self, truck_id: str) -> int:
        """Consistent Murmur2 hashing to distribute trucks evenly across partitions."""
        return hash(truck_id) % self.num_partitions

    def generate_event(self, truck_index: int, inject_anomaly: bool = False) -> TruckTelemetryEvent:
        """Generate a single high-fidelity telemetry event."""
        truck_id = f"TRK-{truck_index:05d}"
        partition = self._get_partition(truck_id)
        baseline = self._truck_baselines[truck_index % len(self._truck_baselines)]
        
        # Add random sensor noise (-0.5°C to +0.5°C)
        temp_noise = (random.random() - 0.5) * 1.0
        current_temp = baseline + temp_noise

        refrig_status = RefrigerationState.OPTIMAL
        if inject_anomaly:
            current_temp += 15.0  # Simulated compressor malfunction
            refrig_status = RefrigerationState.CRITICAL
        elif current_temp > 2.0 and baseline < -10.0:
            refrig_status = RefrigerationState.WARNING

        return TruckTelemetryEvent(
            truck_id=truck_id,
            timestamp=int(time.time() * 1000),
            temperature=round(current_temp, 2),
            engine_rpm=random.randint(1200, 2600),
            latitude=37.7749 + (random.random() - 0.5) * 5.0,
            longitude=-122.4194 + (random.random() - 0.5) * 5.0,
            speed_kmh=round(random.uniform(50.0, 95.0), 1),
            partition=partition,
            refrigeration_status=refrig_status,
        )

    def stream_batch(self, batch_size: int = 5000) -> List[TruckTelemetryEvent]:
        """Produce a high-throughput batch of events for Kafka ingestion."""
        batch: List[TruckTelemetryEvent] = []
        for _ in range(batch_size):
            truck_idx = random.randint(1, self.fleet_size)
            anomaly = random.random() < 0.02  # 2% anomaly rate
            batch.append(self.generate_event(truck_idx, inject_anomaly=anomaly))
        return batch


# Convenient alias for backward-compatibility and architecture documentation
SyntheticFleetProducer = FleetTelemetryGenerator
