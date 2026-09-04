"""
Kafka topic administration via confluent-kafka AdminClient.
"""
from __future__ import annotations

import sys
import time

from streamforge.config import get_settings


def _admin():
    from confluent_kafka.admin import AdminClient

    s = get_settings()
    return AdminClient({"bootstrap.servers": s.kafka_bootstrap_servers}), s


def create_topics() -> None:
    from confluent_kafka.admin import NewTopic

    admin, s = _admin()
    topics = [
        NewTopic(
            s.kafka_topic,
            num_partitions=s.kafka_partitions,
            replication_factor=s.kafka_replication_factor,
        ),
        NewTopic(
            s.kafka_changelog_topic,
            num_partitions=s.kafka_partitions,
            replication_factor=s.kafka_replication_factor,
            config={"cleanup.policy": "compact", "min.compaction.lag.ms": "0"},
        ),
    ]
    fs = admin.create_topics(topics, request_timeout=15)
    for name, f in fs.items():
        try:
            f.result()
            print(f"[kafka] created topic {name}")
        except Exception as e:
            # TopicExists is okay
            if "already exists" in str(e).lower() or "TopicExists" in str(type(e).__name__):
                print(f"[kafka] topic {name} already exists")
            else:
                print(f"[kafka] create {name} failed: {e}", file=sys.stderr)


def describe_topics() -> None:
    admin, s = _admin()
    md = admin.list_topics(timeout=10)
    for name in [s.kafka_topic, s.kafka_changelog_topic]:
        t = md.topics.get(name)
        if t is None:
            print(f"[kafka] topic {name}: NOT FOUND")
        else:
            parts = len(t.partitions)
            print(f"[kafka] topic {name}: {parts} partitions, error={t.error}")


def delete_topics() -> None:
    admin, s = _admin()
    fs = admin.delete_topics([s.kafka_topic, s.kafka_changelog_topic], request_timeout=15)
    for name, f in fs.items():
        try:
            f.result()
            print(f"[kafka] deleted {name}")
        except Exception as e:
            print(f"[kafka] delete {name}: {e}", file=sys.stderr)


def main(action: str = "describe") -> None:
    # Wait for broker briefly
    for i in range(10):
        try:
            if action == "create":
                create_topics()
            elif action == "describe":
                describe_topics()
            elif action == "delete":
                delete_topics()
            else:
                print(f"unknown action {action}", file=sys.stderr)
                sys.exit(1)
            return
        except Exception as e:
            if i == 9:
                raise
            print(f"[kafka] broker not ready ({e}), retry {i+1}/10...")
            time.sleep(2)


if __name__ == "__main__":
    main(sys.argv[1] if len(sys.argv) > 1 else "describe")
