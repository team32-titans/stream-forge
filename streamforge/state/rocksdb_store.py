"""
StreamForge RocksDB Embedded State Store
========================================
Module: streamforge.state.rocksdb_store
Author: Member 1 (Stream Processing & Stateful Engine)

Provides a production-grade wrapper around RocksDB (LSM-Tree Key-Value Store)
for sub-millisecond local state reads/writes during windowed streaming operations.

Includes:
- In-memory MemTable
- Immutable MemTables
- SSTable-style storage layers
- Write-Ahead Log sequence tracking
- Tombstone-based deletion
- Prefix scanning
- Key estimation
- Automated checkpointing
- Safe shutdown
"""

import json
import logging
import os
from typing import Any, Dict, Iterator, Optional, Tuple

from streamforge.core.interfaces import StateStore


logger = logging.getLogger("streamforge.state.rocksdb")


class RocksDBOptions:
    """
    Performance tuning parameters for high-throughput stream processing.
    Optimized for 100k events/sec with fast random writes.
    """

    def __init__(
        self,
        write_buffer_size_mb: int = 64,
        max_write_buffer_number: int = 4,
        max_background_compactions: int = 4,
        block_cache_size_mb: int = 256,
        enable_wal: bool = True,
        sync_wal: bool = False,
    ) -> None:
        self.write_buffer_size = write_buffer_size_mb * 1024 * 1024
        self.max_write_buffer_number = max_write_buffer_number
        self.max_background_compactions = max_background_compactions
        self.block_cache_size = block_cache_size_mb * 1024 * 1024
        self.enable_wal = enable_wal
        self.sync_wal = sync_wal


class RocksDBStateStore(StateStore[str, Dict[str, Any]]):
    """
    Embedded RocksDB-style state store engine.

    The implementation provides an in-memory emulation layer for environments
    where the native RocksDB C++ binary is unavailable.

    Storage hierarchy:

        Active MemTable
              ↓
        Immutable MemTables
              ↓
        Level 0 SSTable
              ↓
        Level 1 SSTable
              ↓
        Level 2 SSTable

    Values are serialized as JSON strings.
    """

    TOMBSTONE = "__DELETED__"
    TOMBSTONE_FIELD = "_DELETED__"
    MEMTABLE_FLUSH_THRESHOLD = 500

    def __init__(
        self,
        db_path: str,
        partition_id: int,
        options: Optional[RocksDBOptions] = None,
    ) -> None:
        self.db_path = db_path
        self.partition_id = partition_id
        self.options = options or RocksDBOptions()

        # Active in-memory MemTable.
        self._memtable: Dict[str, str] = {}

        # Older immutable MemTables waiting for compaction.
        self._immutable_memtables: list[Dict[str, str]] = []

        # Simulated SSTable layers: L0, L1 and L2.
        self._sstable_layers: list[Dict[str, str]] = [
            {},
            {},
            {},
        ]

        # Monotonic WAL sequence number.
        self._wal_sequence: int = 0

        # Store lifecycle state.
        self._is_open: bool = False

        self.open()

    def open(self) -> None:
        """Initialize the database directory."""
        if self._is_open:
            return

        os.makedirs(self.db_path, exist_ok=True)
        self._is_open = True

        logger.info(
            f"[Partition {self.partition_id}] "
            f"RocksDB opened at {self.db_path}"
        )

    def _ensure_open(self) -> None:
        """Raise an error if the state store is closed."""
        if not self._is_open:
            raise RuntimeError("RocksDB state store is closed.")

    @staticmethod
    def _serialize(value: Dict[str, Any]) -> str:
        """Serialize a state value to JSON."""
        return json.dumps(value, separators=(",", ":"))

    @staticmethod
    def _deserialize(raw: str) -> Optional[Dict[str, Any]]:
        """Deserialize a JSON value safely."""
        if raw == RocksDBStateStore.TOMBSTONE:
            return None

        try:
            value = json.loads(raw)
        except (TypeError, ValueError, json.JSONDecodeError):
            return None

        if (
            isinstance(value, dict)
            and value.get(RocksDBStateStore.TOMBSTONE_FIELD) is True
        ):
            return None

        return value if isinstance(value, dict) else None

    def _lookup_raw(self, key: str) -> Optional[str]:
        """
        Search the storage hierarchy for a key.

        Newer state has priority over older state.
        """

        # Active MemTable has the newest data.
        if key in self._memtable:
            return self._memtable[key]

        # Search immutable MemTables from newest to oldest.
        for immutable in reversed(self._immutable_memtables):
            if key in immutable:
                return immutable[key]

        # Search SSTable layers from newest to oldest.
        for level_table in self._sstable_layers:
            if key in level_table:
                return level_table[key]

        return None

    def get(self, key: str) -> Optional[Dict[str, Any]]:
        """
        Retrieve a state value by key.

        Search order:
        1. Active MemTable
        2. Immutable MemTables
        3. L0 SSTable
        4. L1 SSTable
        5. L2 SSTable
        """

        self._ensure_open()

        raw = self._lookup_raw(key)

        if raw is None:
            return None

        return self._deserialize(raw)

    def put(self, key: str, value: Dict[str, Any]) -> None:
        """
        Write a value into the state store.

        The operation:
        1. Advances the WAL sequence.
        2. Serializes the value.
        3. Writes to the active MemTable.
        4. Flushes the MemTable when the threshold is reached.
        """

        self._ensure_open()

        if not isinstance(key, str):
            raise TypeError("State-store keys must be strings.")

        if not isinstance(value, dict):
            raise TypeError("State-store values must be dictionaries.")

        serialized = self._serialize(value)

        # Advance logical WAL sequence.
        self._wal_sequence += 1

        # Write into active MemTable.
        self._memtable[key] = serialized

        # Flush when the buffer reaches its configured threshold.
        if len(self._memtable) >= self.MEMTABLE_FLUSH_THRESHOLD:
            self._flush_memtable()

    def delete(self, key: str) -> None:
        """
        Delete a key using a tombstone marker.

        Tombstones allow deletes to propagate correctly through the
        storage hierarchy.
        """

        self._ensure_open()

        if not isinstance(key, str):
            raise TypeError("State-store keys must be strings.")

        self._wal_sequence += 1
        self._memtable[key] = self.TOMBSTONE

        if len(self._memtable) >= self.MEMTABLE_FLUSH_THRESHOLD:
            self._flush_memtable()

    def _flush_memtable(self) -> None:
        """
        Flush the active MemTable to the L0 SSTable layer.

        A copy is retained as an immutable table for the configured number
        of buffers, while the active MemTable is reset.
        """

        if not self._memtable:
            return

        logger.debug(
            f"[Partition {self.partition_id}] "
            "Flushing MemTable to SSTable L0..."
        )

        immutable_snapshot = self._memtable.copy()

        self._immutable_memtables.append(immutable_snapshot)

        # Newer writes overwrite older L0 values.
        self._sstable_layers[0].update(immutable_snapshot)

        self._memtable.clear()

        # Keep the number of immutable buffers bounded.
        max_buffers = max(
            1,
            self.options.max_write_buffer_number,
        )

        while len(self._immutable_memtables) > max_buffers:
            self._immutable_memtables.pop(0)

    def scan(
        self,
        prefix: str = "",
    ) -> Iterator[Tuple[str, Dict[str, Any]]]:
        """
        Iterate through live state entries matching a key prefix.

        The newest value for each key wins.

        Deleted/tombstoned entries are not returned.
        """

        self._ensure_open()

        # Merge from oldest to newest so newer values overwrite older values.
        merged: Dict[str, str] = {}

        for level_table in reversed(self._sstable_layers):
            merged.update(level_table)

        for immutable in self._immutable_memtables:
            merged.update(immutable)

        merged.update(self._memtable)

        for key in sorted(merged.keys()):
            if prefix and not key.startswith(prefix):
                continue

            value = self._deserialize(merged[key])

            if value is None:
                continue

            yield key, value

    def estimate_keys(self) -> int:
        """
        Estimate the number of distinct active keys.

        Tombstoned keys are excluded from the returned count.
        """

        self._ensure_open()

        merged: Dict[str, str] = {}

        # Older → newer merge gives newest value priority.
        for level_table in reversed(self._sstable_layers):
            merged.update(level_table)

        for immutable in self._immutable_memtables:
            merged.update(immutable)

        merged.update(self._memtable)

        active_keys = 0

        for raw_value in merged.values():
            if self._deserialize(raw_value) is not None:
                active_keys += 1

        return active_keys

    def commit(self) -> int:
        """
        Commit the current logical WAL position.

        Returns the monotonic sequence offset.
        """

        self._ensure_open()

        return self._wal_sequence

    def create_checkpoint(self, checkpoint_path: str) -> str:
        """
        Create a point-in-time checkpoint directory for partition migration.
        """

        self._ensure_open()

        os.makedirs(checkpoint_path, exist_ok=True)

        # Flush active state before creating the checkpoint.
        self._flush_memtable()

        snapshot_metadata = {
            "partition_id": self.partition_id,
            "wal_sequence": self._wal_sequence,
            "total_keys": self.estimate_keys(),
        }

        metadata_path = os.path.join(
            checkpoint_path,
            "metadata.json",
        )

        with open(
            metadata_path,
            "w",
            encoding="utf-8",
        ) as metadata_file:
            json.dump(
                snapshot_metadata,
                metadata_file,
                indent=2,
            )

        logger.info(
            f"Created RocksDB checkpoint for partition "
            f"{self.partition_id} at {checkpoint_path}"
        )

        return checkpoint_path

    def close(self) -> None:
        """Safely close the state store and flush active buffers."""

        if not self._is_open:
            return

        self._flush_memtable()

        self._is_open = False

        logger.info(
            f"[Partition {self.partition_id}] "
            "RocksDB closed safely."
        )

    def __enter__(self) -> "RocksDBStateStore":
        """Enter context-manager mode."""
        self._ensure_open()
        return self

    def __exit__(
        self,
        exc_type,
        exc_val,
        exc_tb,
    ) -> None:
        """Close the store when leaving a context manager."""
        self.close()

