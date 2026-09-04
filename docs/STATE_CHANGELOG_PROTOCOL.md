# StreamForge State / Changelog / Recovery Protocol

**Status: Design-First (Phase 6) — must be reviewed before changelog implementation.**

## 1. Goal
Guarantee partition-aware, idempotent state recovery with no silent loss, without falsely claiming exactly-once.

## 2. State Key
`state_key = f"{truck_id}:{window_start_ms}"` where `window_start_ms = timestamp - (timestamp % WINDOW_SIZE_MS)` (tumbling 5min). Example `TRK-00492:1709280000000`.

## 3. RocksDB Value Schema (versioned)
```json
{
  "schema_version": 1,
  "truck_id": "TRK-00492",
  "window_start": 1709280000000,
  "window_end": 1709280300000,
  "count": 140,
  "sum_temperature": -2800.0,
  "avg_temperature": -20.0,
  "min_temperature": -23.0,
  "max_temperature": -17.0,
  "seq": 42,
  "source_offset": 12345,
  "updated_at_ms": 1709280100000,
  "worker_id": "worker-04"
}
```
`seq` = durable version = **Kafka source offset** per `(partition, state_key)` (monotonic per partition). Invariant: `partition+state_key -> seq` never moves backwards; worker restart/reassignment cannot reset it; stale replay with smaller seq never overwrites newer. In production, `seq == source_offset`; in demo/test where offset unavailable, fallback counter is non-durable and explicitly documented.

## 4. Changelog Topic
*Name* `streamforge.truck_state.changelog`, partitions=32 aligned, `cleanup.policy=compact`.
*Key* `changelog_key = f"{partition:02d}:{state_key}"` e.g. `07:TRK-00492:1709280000000` — routes changelog record to same partition number as source.
*Value* JSON with `partition, key, changelog_key, value (above), seq, source_offset, timestamp_ms, worker_id, op=PUT|DELETE`.
Headers: `seq`, `partition`.

## 5. Ordering
Per `(partition, state_key)` total order via Kafka partition order. Global order not needed. Single writer enforced by cooperative-sticky assignor.

## 6. Produce Order (critical)
```
RocksDB put
  -> changelog produce (ack via on_delivery + flush)
    -> commit source offset (only if flush ack succeeded)
```
Crash semantics: RocksDB fail -> no commit; changelog fail -> no commit; crash before commit -> redelivery (idempotent via seq); success path -> durable. RPO/RTO not pre-claimed — measured after chaos test.

## 7. Replay & Isolation
On `on_partitions_assigned([p])`:
1. open `RocksDBStateStore(/data/rocksdb/p{p}, p)` on **ephemeral tmpfs per worker** (no shared volume). Recovery is via changelog, not shared files, so two workers cannot concurrently mutate same partition during rebalance.

2. assign `TopicPartition(changelog_topic, p, OFFSET_BEGINNING)` to changelog consumer
3. for each record in order: if `rec.seq > local.get(key).seq` then `put`, else skip; DELETE tombstone -> `delete`
4. commit is not needed for changelog consumer (internal)

## 8. Duplicate Replay
Idempotent: `seq` comparison ensures replaying twice yields same final state. Test: replay twice -> assert equal.

## 9. Crash Scenarios
* after RocksDB put, before changelog ack -> lost update (1 window) — documented
* after changelog ack, before offset commit -> new worker replays update + reprocesses event -> dedup via seq (effectively-once)
* during replay -> restart from beginning, idempotent
* concurrent owner race -> old worker closes DB on revoke, new worker wins

## 10. Offset Semantics
`enable.auto.commit=False`. Source offsets committed only after changelog ack. Changelog offsets not committed as source.

## 11. Semantics Claim
at-least-once transport + idempotent state = effectively-once aggregation. Exactly-once (transactions) not claimed in v1.

## 12. Watermark / Lateness
Watermark `W = max_event_time - MAX_LATENESS_MS`, monotonic. Window `[start,end)` emits when `W >= end`. Late `timestamp < W` -> side_output or drop per `LATE_EVENT_POLICY`. `W` not advanced by late events. Gauge `streamforge_watermark_lag_ms`.
