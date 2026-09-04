"""
Partition-aware Kafka consumer with on_assign/on_revoke lifecycle.
Uses confluent-kafka; opens RocksDB only for assigned partitions.
"""
from __future__ import annotations

import json
import logging
import time
from typing import Dict, List

from streamforge.config import get_settings

logger = logging.getLogger("streamforge.worker.consumer")


class StreamConsumer:
    def __init__(self, worker_id: str, on_assign=None, on_revoke=None):
        s = get_settings()
        self.worker_id = worker_id
        self.topic = s.kafka_topic
        self.group_id = s.kafka_consumer_group
        self.bootstrap = s.kafka_bootstrap_servers
        self.on_assign = on_assign
        self.on_revoke = on_revoke
        self._consumer = None
        self._init_consumer()

    def _init_consumer(self):
        from confluent_kafka import Consumer

        s = get_settings()
        conf = {
            "bootstrap.servers": self.bootstrap,
            "group.id": self.group_id,
            "client.id": self.worker_id,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
            "partition.assignment.strategy": "cooperative-sticky",
            "enable.auto.offset.store": False,
        }
        self._consumer = Consumer(conf)
        logger.info(f"[{self.worker_id}] Consumer created group={self.group_id}")

    def subscribe(self, topics=None):
        topics = topics or [self.topic]

        def _on_assign(consumer, partitions):
            logger.info(f"[{self.worker_id}] on_assign {partitions}")
            if self.on_assign:
                try:
                    self.on_assign(partitions)
                except Exception as e:
                    logger.error(f"on_assign error: {e}")
            # incremental assign handled by confluent_kafka

        def _on_revoke(consumer, partitions):
            logger.info(f"[{self.worker_id}] on_revoke {partitions}")
            if self.on_revoke:
                try:
                    self.on_revoke(partitions)
                except Exception as e:
                    logger.error(f"on_revoke error: {e}")

        self._consumer.subscribe(topics, on_assign=_on_assign, on_revoke=_on_revoke)

    def poll(self, timeout: float = 1.0):
        return self._consumer.poll(timeout)

    def commit(self, msg=None):
        try:
            if msg is not None:
                self._consumer.commit(msg, asynchronous=False)
            else:
                self._consumer.commit(asynchronous=False)
        except Exception as e:
            logger.error(f"commit failed: {e}")

    def store_offset(self, msg):
        try:
            self._consumer.store_offsets(msg)
        except Exception as e:
            logger.error(f"store_offset failed: {e}")

    def close(self):
        try:
            self._consumer.close()
        except Exception:
            pass
