# Benchmark Observations — StrataDB v0.1

**Date**: 2026-02-01
**Hardware**: AMD Ryzen 7 7800X3D (8C/16T, 96MB V-Cache), 64GB DDR5, NVMe SSD
**OS**: Linux 6.14.0-37-generic (x86_64)
**Benchmark framework**: Criterion 0.5 with custom latency percentile reporting

## Executive Summary

20 operations across 6 primitives were benchmarked at 3 durability levels (NoDurability, Buffered, Strict). The in-memory path is fast (sub-microsecond for reads, low-microsecond for writes). However, the benchmarks exposed **three engine-level bugs** and **two performance anomalies** that need investigation.

---

## Results Matrix (p50 Latency)

### KV Primitive

| Operation | NoDurability | Buffered | Strict |
|-----------|-------------|----------|--------|
| put (128B) | 922 ns | 6.12 ms | 6.54 ms |
| put (1KB) | 1.02 us | 6.12 ms | 6.57 ms |
| put (8KB) | 3.93 us | 6.18 ms | 6.63 ms |
| get (128B) | 411 ns | 483 ns | 501 ns |
| get (1KB) | 431 ns | 484 ns | 488 ns |
| get (8KB) | 527 ns | 655 ns | 653 ns |
| delete | 1.75 us | 6.14 ms | 6.54 ms |
| list_prefix | 934 us | 983 us | 946 us |

### State Primitive

| Operation | NoDurability | Buffered | Strict |
|-----------|-------------|----------|--------|
| set | 2.21 us | 6.13 ms | 6.52 ms |
| read | 1.10 us | 6.12 ms | 6.47 ms |
| cas | 2.04 us | 6.11 ms | 6.50 ms |

### Event Primitive

| Operation | NoDurability | Buffered | Strict |
|-----------|-------------|----------|--------|
| append | 267 us | 6.53 ms | 6.53 ms |
| read | 4.24 us | 13.77 ms | 15.49 ms |
| read_by_type | 2.89 ms | 12.12 ms | 12.45 ms |

### JSON Primitive

| Operation | NoDurability | Buffered | Strict |
|-----------|-------------|----------|--------|
| set_root | 6.04 us | 24.32 ms | 27.58 ms |
| set_path | 4.31 us | 24.27 ms | 31.04 ms |
| get | 3.89 us | 14.00 ms | 16.14 ms |
| list | 891 us | 15.62 ms | 14.95 ms |

### Vector Primitive

| Operation | NoDurability | Buffered | Strict |
|-----------|-------------|----------|--------|
| upsert | 2.52 us | 13.89 ms | 16.25 ms |
| search (10K, top-10) | 68.96 ms | 70.19 ms | 68.05 ms |
| get | 1.20 us | 1.11 us | 1.21 us |

### Branch Primitive

| Operation | NoDurability | Buffered | Strict |
|-----------|-------------|----------|--------|
| create | 2.69 us | 13.86 ms | 15.56 ms |
| switch | 451 ns | 13.91 ms | 15.55 ms |
| delete | 6.34 us | 92.53 ms (p50) | 98.20 ms (p50) |

*Note: Results above use the sequential (uncontested) run for disk-backed modes, which gives the most accurate single-threaded numbers.*

---

## Bug: Buffered Mode Not Batching (BUG-1)

**Severity**: High
**Affected**: All write operations across all primitives

Buffered durability mode (`DurabilityMode::Batched`) shows nearly identical latency to Strict mode. For every write operation measured, Buffered is within 5-10% of Strict:

| Operation | Buffered p50 | Strict p50 | Ratio |
|-----------|-------------|-----------|-------|
| kv/put (1KB) | 6.12 ms | 6.57 ms | 0.93x |
| state/set | 6.13 ms | 6.52 ms | 0.94x |
| state/cas | 6.11 ms | 6.50 ms | 0.94x |
| event/append | 6.53 ms | 6.53 ms | 1.00x |
| branch/create | 13.86 ms | 15.56 ms | 0.89x |

**Expected behavior**: Buffered mode should batch multiple writes before issuing a single fsync. With batching, individual write latency should be significantly lower than Strict (which fsyncs every write). A well-implemented batched mode typically achieves 10-100x better write throughput than per-write fsync.

**Hypothesis**: The buffered durability backend is either:
1. Falling through to per-write fsync (same codepath as Strict), or
2. The batch window is too small / batch size is 1, effectively degenerating to Strict behavior

**Investigation path**: Trace the `DurabilityMode::Batched` codepath in `crates/durability/` to verify batch accumulation and deferred fsync logic.

---

## Bug: Reads Trigger Fsync in Some Primitives (BUG-2)

**Severity**: High
**Affected**: state/read, event/read, event/read_by_type, json/get, json/list, branch/switch

Pure read operations should never trigger fsync — they should serve data from the in-memory layer regardless of durability configuration. However, several primitives show read latency equal to write latency in durable modes:

| Operation | NoDurability | Buffered | Expected Buffered | Actual/Expected |
|-----------|-------------|----------|-------------------|-----------------|
| state/read | 1.10 us | 6.12 ms | ~1 us | 5,563x slower |
| event/read | 4.24 us | 13.77 ms | ~4 us | 3,248x slower |
| json/get | 3.89 us | 14.00 ms | ~4 us | 3,599x slower |
| branch/switch | 451 ns | 13.91 ms | ~450 ns | 30,844x slower |

**Correctly-behaved reads** (durability-agnostic):
| Operation | NoDurability | Buffered | Strict | Ratio |
|-----------|-------------|----------|--------|-------|
| kv/get (1KB) | 431 ns | 484 ns | 488 ns | ~1.0x |
| vector/get | 1.20 us | 1.11 us | 1.21 us | ~1.0x |
| vector/search | 68.96 ms | 70.19 ms | 68.05 ms | ~1.0x |
| kv/list_prefix | 934 us | 983 us | 946 us | ~1.0x |

**Pattern**: KV and Vector reads are correctly fast across all modes. State, Event, JSON, and Branch reads are broken — they appear to be routing through the write/sync path.

**Hypothesis**: The read path for state, event, json, and branch primitives is incorrectly calling into the durability layer (triggering a sync) rather than reading directly from the in-memory store. The KV and Vector primitives likely have a separate, correctly-implemented read path that bypasses durability.

---

## Bug: Branch Switch Triggers Write in Durable Modes (BUG-3)

**Severity**: Medium
**Affected**: branch/switch (set_branch)

Branch switch is a metadata operation that changes which branch is active. It should be an in-memory pointer swap taking ~450ns (as shown in NoDurability mode). Instead, in durable modes it takes 13-15ms — the cost of a full fsync.

This suggests `set_branch()` is persisting the active branch selection to disk on every call. If the intent is to restore the active branch after crash recovery, this should be deferred or batched, not synchronous.

---

## Anomaly: Event Append Is 100x Slower Than KV Put in Memory (PERF-1)

**Severity**: Medium
**Affected**: event/append in NoDurability mode

| Operation | NoDurability p50 |
|-----------|-----------------|
| kv/put (1KB) | 1.02 us |
| state/set | 2.21 us |
| event/append | 267 us |

Event append in NoDurability mode takes 267us — two orders of magnitude slower than comparable write operations. Since there's no fsync involved, this overhead must come from the event log's internal data structures (sequence number allocation, type indexing, append-only log management).

**Investigation path**: Profile `event_append` to identify whether the cost is in serialization, index maintenance, or log structure overhead.

---

## Anomaly: JSON Operations Require Multiple Fsyncs (PERF-2)

**Severity**: Low
**Affected**: json/set_root, json/set_path, json/get

JSON write operations in durable modes take ~24-31ms — roughly 4x the single-fsync baseline of ~6ms. JSON get takes ~14ms (2x baseline).

| Operation | Buffered p50 | Implied fsyncs |
|-----------|-------------|----------------|
| json/set_root | 24.32 ms | ~4 |
| json/set_path | 24.27 ms | ~4 |
| json/get | 14.00 ms | ~2 |

This suggests JSON operations decompose into multiple internal storage operations, each triggering its own fsync. A `json_set` likely does: (1) read existing document, (2) deserialize, (3) modify, (4) serialize, (5) write back — with fsyncs at steps 1 and 5 (or more if the implementation uses multiple KV operations internally).

**Investigation path**: Verify whether JSON operations can be wrapped in a single transaction to consolidate fsyncs.

---

## Anomaly: Branch Delete Is Extremely Expensive (PERF-3)

**Severity**: Medium
**Affected**: branch/delete in durable modes

Branch delete takes 93-233ms in durable modes — 15-35x more expensive than branch create (~14ms). The bimodal distribution (p50=92ms, p95=210ms in buffered) suggests some deletions trigger heavy cleanup.

**Investigation path**: Profile branch deletion to understand what cleanup operations are triggered (metadata removal, data compaction, index cleanup) and whether they can be deferred.

---

## Positive Findings

1. **In-memory performance is excellent**: Sub-microsecond reads (kv/get at 411ns, branch/switch at 451ns), low-microsecond writes across most primitives.

2. **Cache hierarchy effects visible on 7800X3D**: Value-size sweep shows clear L1/L2/L3 transitions — 128B put at 922ns, 1KB at 1.02us, 8KB at 3.93us. The large V-Cache (96MB L3) keeps the working set in cache.

3. **Vector operations are correctly durability-agnostic for reads**: Search (68ms) and get (1.2us) show zero overhead from durability configuration, confirming the vector index is served purely from memory.

4. **KV read path is correct**: Gets are 400-500ns regardless of durability mode — the read path correctly bypasses the durability layer.

---

## Recommended Fix Priority

| Priority | Issue | Impact |
|----------|-------|--------|
| P0 | BUG-2: Reads trigger fsync | 3,000-30,000x read latency regression in durable modes |
| P0 | BUG-1: Buffered mode not batching | Eliminates benefit of batched durability mode entirely |
| P1 | BUG-3: Branch switch writes to disk | 30,000x latency regression for branch switching |
| P1 | PERF-1: Event append overhead | 100x slower than expected for in-memory writes |
| P2 | PERF-2: JSON multi-fsync | 4x expected latency for JSON writes |
| P2 | PERF-3: Branch delete cost | 15-35x more expensive than create |
