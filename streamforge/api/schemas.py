"""Pydantic response schemas for control plane."""
from pydantic import BaseModel
from typing import List, Optional


class HealthResponse(BaseModel):
    status: str
    service: str
    version: str
    kafka_bootstrap: str
    storage_mode: str
    partitions: int


class PartitionInfo(BaseModel):
    partitionId: int
    leader: Optional[int] = None
    replicas: Optional[List[int]] = None
    isrs: Optional[List[int]] = None


class WorkersResponse(BaseModel):
    target_workers: int
    storage_mode: str
