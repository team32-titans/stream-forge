"""
FastAPI Control Plane — StreamForge
"""
from __future__ import annotations

import asyncio
import json
import time
from typing import Any, Dict, List

from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import PlainTextResponse, JSONResponse

from streamforge.config import get_settings
from streamforge.metrics.exporter import get_exporter, _PROM_AVAILABLE

try:
    from prometheus_client import generate_latest, CONTENT_TYPE_LATEST

    _has_prom = True
except Exception:
    _has_prom = False

app = FastAPI(title="StreamForge Control Plane", version="1.0.0")
exporter = get_exporter()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/api/health")
def health():
    s = get_settings()
    kafka_status = "unknown"
    try:
        from confluent_kafka.admin import AdminClient

        admin = AdminClient({"bootstrap.servers": s.kafka_bootstrap_servers, "socket.timeout.ms": 2000})
        md = admin.list_topics(timeout=2)
        kafka_status = "available" if (md.brokers and len(md.brokers) > 0) else "unavailable"
    except Exception:
        kafka_status = "unavailable"
    import os as _os

    storage_status = "available" if _os.path.isdir(s.rocksdb_base_path) else "no_local_state"
    degraded = kafka_status != "available"
    return {
        "status": "degraded" if degraded else "healthy",
        "service": "streamforge_api",
        "version": "1.0.0",
        "kafka_bootstrap": s.kafka_bootstrap_servers,
        "kafka": kafka_status,
        "storage": storage_status,
        "storage_mode": s.storage_mode,
        "partitions": s.kafka_partitions,
        "note": "API alive. Kafka/state report real availability; degraded means dependents unavailable."
        if degraded
        else "API alive with Kafka reachable.",
    }


@app.get("/health")
def health_alt():
    return health()


@app.get("/metrics", response_class=PlainTextResponse)
def metrics():
    if _has_prom:
        try:
            data = generate_latest()
            return PlainTextResponse(data.decode("utf-8"), media_type=CONTENT_TYPE_LATEST)
        except Exception:
            pass
    return PlainTextResponse(exporter.export_prometheus_text(), media_type="text/plain; version=0.0.4")


@app.get("/api/metrics")
def api_metrics():
    return {"counters": exporter.counters, "gauges": exporter.gauges}


@app.get("/api/workers")
def workers():
    # Separate configured target from actually observed workers.
    # Observed membership requires Kafka consumer-group query; without a
    # broker we return explicit unknown — never fabricate "20 running".
    s = get_settings()
    brokers = 0
    observed: Any = "unknown"
    assignments: Any = "unknown"
    kafka_status = "unavailable"
    try:
        from confluent_kafka.admin import AdminClient

        admin = AdminClient({"bootstrap.servers": s.kafka_bootstrap_servers, "socket.timeout.ms": 2000})
        md = admin.list_topics(timeout=2)
        brokers = len(md.brokers) if md.brokers else 0
        if brokers > 0:
            kafka_status = "available"
            try:
                groups = admin.list_consumer_groups(timeout=3)
                result = groups.result() if hasattr(groups, "result") else groups
                valid = getattr(result, "valid", []) or []
                for g in valid:
                    if getattr(g, "group_id", "") == s.kafka_consumer_group:
                        try:
                            desc = admin.describe_consumer_groups([s.kafka_consumer_group], request_timeout=3)
                            fut = desc.get(s.kafka_consumer_group)
                            gd = fut.result() if fut is not None and hasattr(fut, "result") else None
                            members = getattr(gd, "members", None)
                            observed = len(members) if members is not None else "available-see-group"
                        except Exception:
                            observed = "available-see-group"
                        break
            except Exception:
                observed = "unknown"
        else:
            observed = "unknown"
    except Exception:
        brokers = 0
        observed = "unknown"
    return {
        "target_workers": s.target_workers,
        "observed_workers": observed,
        "assignments": assignments,
        "kafka": kafka_status,
        "storage_mode": s.storage_mode,
        "kafka_brokers": brokers,
        "exporter": {"counters": exporter.counters, "gauges": exporter.gauges},
        "note": "target_workers is configured scale; observed_workers requires a reachable Kafka group query. 'unknown' means unavailable, not zero.",
    }


@app.get("/api/partitions")
def partitions():
    s = get_settings()
    try:
        from confluent_kafka.admin import AdminClient

        admin = AdminClient({"bootstrap.servers": s.kafka_bootstrap_servers})
        md = admin.list_topics(timeout=5)
        t = md.topics.get(s.kafka_topic)
        if t is None:
            raise HTTPException(404, f"topic {s.kafka_topic} not found")
        parts = []
        for pid, p in t.partitions.items():
            parts.append({"partitionId": pid, "leader": p.leader, "replicas": p.replicas, "isrs": p.isrs, "error": str(p.error) if p.error else None})
        return {"topic": s.kafka_topic, "partitions": sorted(parts, key=lambda x: x["partitionId"])}
    except HTTPException:
        raise
    except Exception as e:
        # Honest degraded state: no invented leaders.
        return {
            "topic": s.kafka_topic,
            "status": "degraded",
            "kafka": "unavailable",
            "partitions": [{"partitionId": i, "leader": None, "status": "unknown", "error": f"broker unavailable: {e}"} for i in range(s.kafka_partitions)],
            "warning": str(e),
        }


@app.get("/api/telemetry")
def telemetry(limit: int = 20):
    # Best-effort: short-lived consumer per request (no pooling; closed in finally).
    s = get_settings()
    limit = max(1, min(100, limit))
    events: List[Dict[str, Any]] = []
    c = None
    try:
        from confluent_kafka import Consumer

        conf = {"bootstrap.servers": s.kafka_bootstrap_servers, "group.id": "api-telemetry-reader", "auto.offset.reset": "latest", "enable.auto.commit": False}
        c = Consumer(conf)
        c.subscribe([s.kafka_topic])
        for _ in range(limit):
            msg = c.poll(0.5)
            if msg is None or msg.error():
                continue
            try:
                events.append(json.loads(msg.value().decode()))
            except Exception:
                continue
            if len(events) >= limit:
                break
    except Exception as e:
        return {"events": [], "warning": str(e), "limit": limit}
    finally:
        try:
            if c is not None:
                c.close()
        except Exception:
            pass
    return {"events": events, "count": len(events)}


@app.get("/api/windows/{truck_id}")
def windows(truck_id: str):
    # Distributed read path: worker state lives in per-partition RocksDB on
    # workers + the Kafka changelog. The API host can only see local copies;
    # reconstruct from the changelog topic in production, scan local RocksDB
    # in demo. Always label the source honestly.
    s = get_settings()
    if s.storage_mode == "production":
        try:
            from confluent_kafka import Consumer

            conf = {
                "bootstrap.servers": s.kafka_bootstrap_servers,
                "group.id": "api-windows-reader",
                "auto.offset.reset": "earliest",
                "enable.auto.commit": False,
            }
            c = Consumer(conf)
            c.subscribe([s.kafka_changelog_topic])
            results = []
            idle = 0
            while len(results) < 200 and idle < 4:
                msg = c.poll(0.5)
                if msg is None:
                    idle += 1
                    continue
                if msg.error():
                    continue
                try:
                    payload = json.loads(msg.value().decode("utf-8"))
                except Exception:
                    continue
                key = str(payload.get("key", ""))
                if not key.startswith(f"{truck_id}:") and f"{truck_id}:" not in key:
                    continue
                results.append({"partition": payload.get("partition"), "key": key, "value": payload.get("value")})
            try:
                c.close()
            except Exception:
                pass
            return {"mode": "LIVE", "truck_id": truck_id, "windows": results, "count": len(results), "source": "kafka_changelog_topic"}
        except Exception as e:
            return {"mode": "LIVE", "status": "degraded", "truck_id": truck_id, "windows": [], "warning": f"changelog unavailable: {e}"}
    results = []
    import os

    base = s.rocksdb_base_path
    if not os.path.isdir(base):
        return {"mode": "DEMO", "truck_id": truck_id, "windows": [], "note": "no RocksDB data on API host (workers hold state)"}
    from streamforge.state.rocksdb_store import RocksDBStateStore

    limit_total = 200
    for pid in range(s.kafka_partitions):
        if len(results) >= limit_total:
            break
        db_path = os.path.join(base, f"p{pid:02d}")
        if not os.path.isdir(db_path):
            continue
        try:
            store = RocksDBStateStore(db_path=db_path, partition_id=pid, storage_mode=s.storage_mode)
            for k, v in store.scan(prefix=f"{truck_id}:"):
                results.append({"partition": pid, "key": k, "value": v})
                if len(results) >= limit_total:
                    break
            store.close()
        except Exception:
            continue
    return {"mode": "DEMO", "truck_id": truck_id, "windows": results, "count": len(results), "limit": limit_total, "source": "api_host_local_rocksdb_demo_only"}


@app.get("/api/state/{partition}")
def state_partition(partition: int, prefix: str = "", limit: int = 50):
    s = get_settings()
    if partition < 0 or partition >= s.kafka_partitions:
        raise HTTPException(400, "invalid partition")
    import os

    db_path = os.path.join(s.rocksdb_base_path, f"p{partition:02d}")
    if not os.path.isdir(db_path):
        return {"partition": partition, "entries": [], "note": "no RocksDB data on API host"}
    from streamforge.state.rocksdb_store import RocksDBStateStore

    try:
        store = RocksDBStateStore(db_path=db_path, partition_id=partition, storage_mode=s.storage_mode)
        entries = []
        for k, v in store.scan(prefix=prefix):
            entries.append({"key": k, "value": v})
            if len(entries) >= limit:
                break
        store.close()
        return {"partition": partition, "entries": entries, "count": len(entries)}
    except Exception as e:
        raise HTTPException(500, str(e))


@app.get("/api/changelog")
def changelog(partition: int = 0, limit: int = 50):
    """Production reads the Kafka changelog topic (read-only consumer).

    Demo/test modes return the API-process in-memory mirror explicitly
    labelled as demo. Never present API-process memory as Kafka data.
    """
    from streamforge.config import get_settings as _gs

    s = _gs()
    limit = max(1, min(200, limit))
    if s.storage_mode == "production":
        try:
            from confluent_kafka import Consumer

            conf = {
                "bootstrap.servers": s.kafka_bootstrap_servers,
                "group.id": "api-changelog-reader",
                "auto.offset.reset": "earliest",
                "enable.auto.commit": False,
            }
            c = Consumer(conf)
            c.subscribe([s.kafka_changelog_topic])
            recs: List[Dict[str, Any]] = []
            idle = 0
            import json as _json

            while len(recs) < limit and idle < 4:
                msg = c.poll(0.5)
                if msg is None:
                    idle += 1
                    continue
                if msg.error():
                    continue
                try:
                    payload = _json.loads(msg.value().decode("utf-8"))
                except Exception:
                    continue
                if int(payload.get("partition", -1)) != partition:
                    continue
                recs.append(
                    {
                        "key": payload.get("key"),
                        "changelog_key": payload.get("changelog_key"),
                        "seq": payload.get("seq"),
                        "offset": payload.get("offset"),
                        "op": payload.get("op"),
                        "worker": payload.get("worker_source"),
                        "timestamp": payload.get("timestamp"),
                    }
                )
            try:
                c.close()
            except Exception:
                pass
            return {
                "mode": "LIVE",
                "partition": partition,
                "changelog_topic": s.kafka_changelog_topic,
                "count": len(recs),
                "records": recs[-limit:],
            }
        except Exception as e:
            return {
                "mode": "LIVE",
                "status": "degraded",
                "partition": partition,
                "changelog_topic": s.kafka_changelog_topic,
                "count": 0,
                "records": [],
                "warning": f"Kafka changelog unavailable: {e}",
            }
    from streamforge.state.changelog_manager import ChangelogManager

    cm = ChangelogManager()
    recs = cm._in_memory_changelog.get(partition, [])
    # Return last N
    tail = recs[-limit:] if len(recs) > limit else recs
    return {
        "mode": "DEMO",
        "partition": partition,
        "changelog_topic": cm.changelog_topic,
        "count": len(recs),
        "records": [
            {"key": r.key, "changelog_key": r.changelog_key, "seq": r.seq, "offset": r.offset, "op": r.op, "worker": r.worker_source, "timestamp": r.timestamp}
            for r in tail
        ],
        "note": "DEMO mode: API-process in-memory mirror, not Kafka data.",
    }


@app.post("/api/chaos/kill-worker/{worker_id}")
def kill_worker(worker_id: str):
    # Honest chaos endpoint: the API cannot physically kill a worker without
    # an external orchestrator (docker kill / k8s delete). This records the
    # failure-signal request; real termination must be performed externally.
    import logging

    logging.getLogger("streamforge.api.chaos").warning(f"Chaos kill requested for {worker_id}")
    return {
        "status": "requested",
        "worker_id": worker_id,
        "executed": False,
        "note": "Failure signal recorded only — NOT executed. Terminate the worker externally (e.g. `docker stop <worker>`) and observe Kafka rebalance + changelog replay via /api/workers.",
    }


# WebSocket broadcast
connected: List[WebSocket] = []


@app.websocket("/ws/metrics")
async def ws_metrics(ws: WebSocket):
    await ws.accept()
    connected.append(ws)
    try:
        while True:
            payload = {
                "type": "metrics",
                "timestamp": int(time.time() * 1000),
                "counters": exporter.counters,
                "gauges": exporter.gauges,
            }
            # Backpressure guard: drop this tick for slow clients instead of
            # queueing unbounded messages in memory.
            try:
                await asyncio.wait_for(ws.send_text(json.dumps(payload)), timeout=2)
            except (asyncio.TimeoutError, RuntimeError):
                break
            await asyncio.sleep(1)
            # Also wait for client ping
    except WebSocketDisconnect:
        pass
    except Exception:
        pass
    finally:
        if ws in connected:
            connected.remove(ws)


@app.websocket("/ws/telemetry")
async def ws_telemetry(ws: WebSocket):
    await ws.accept()
    try:
        while True:
            await asyncio.sleep(1)
            await ws.send_text(json.dumps({"type": "heartbeat", "ts": int(time.time() * 1000)}))
    except WebSocketDisconnect:
        pass
