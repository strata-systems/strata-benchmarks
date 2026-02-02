//! KV primitive benchmarks: put, get, delete, list_prefix
//!
//! put and get include a value-size sweep (128B, 1KB, 8KB) to expose
//! cache-hierarchy effects. All benchmarks report latency percentiles.

#[allow(unused)]
#[path = "harness/mod.rs"]
mod harness;

use std::sync::atomic::{AtomicU64, Ordering};

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use harness::{
    create_db, kv_key, kv_key_with_prefix, kv_value, kv_value_sized, measure_with_counters,
    report_counters, report_percentiles, DurabilityConfig, ValueSize, PERCENTILE_SAMPLES,
    WARMUP_COUNT,
};

// =============================================================================
// PUT — value-size sweep × durability
// =============================================================================

fn kv_put(c: &mut Criterion) {
    let mut group = c.benchmark_group("kv/put");
    group.throughput(Throughput::Elements(1));

    eprintln!("\n--- Latency Percentiles: kv/put ---");
    for size in ValueSize::ALL {
        for mode in DurabilityConfig::ALL {
            let bench_db = create_db(mode);
            let counter = AtomicU64::new(0);
            let id = format!("{}/{}", size.label(), mode.label());

            group.bench_function(BenchmarkId::new("durability", &id), |b| {
                b.iter(|| {
                    let i = counter.fetch_add(1, Ordering::Relaxed);
                    bench_db
                        .db
                        .kv_put(&kv_key(i), kv_value_sized(size))
                        .unwrap();
                });
            });

            // Percentile pass
            let pct_counter = AtomicU64::new(u64::MAX / 2); // offset to avoid key collisions
            let label = format!("kv/put/{}/{}", size.label(), mode.label());
            let (p, counters) = measure_with_counters(&bench_db, PERCENTILE_SAMPLES, || {
                let i = pct_counter.fetch_add(1, Ordering::Relaxed);
                bench_db
                    .db
                    .kv_put(&kv_key(i), kv_value_sized(size))
                    .unwrap();
            });
            report_percentiles(&label, &p);
            report_counters(&label, &counters, PERCENTILE_SAMPLES as u64);
        }
    }
    group.finish();
}

// =============================================================================
// GET — value-size sweep × durability
// =============================================================================

fn kv_get(c: &mut Criterion) {
    let mut group = c.benchmark_group("kv/get");
    group.throughput(Throughput::Elements(1));

    eprintln!("\n--- Latency Percentiles: kv/get ---");
    for size in ValueSize::ALL {
        for mode in DurabilityConfig::ALL {
            let bench_db = create_db(mode);
            // Pre-populate with this value size
            for i in 0..WARMUP_COUNT {
                bench_db
                    .db
                    .kv_put(&kv_key(i), kv_value_sized(size))
                    .unwrap();
            }
            let counter = AtomicU64::new(0);
            let id = format!("{}/{}", size.label(), mode.label());

            group.bench_function(BenchmarkId::new("durability", &id), |b| {
                b.iter(|| {
                    let i = counter.fetch_add(1, Ordering::Relaxed) % WARMUP_COUNT;
                    bench_db.db.kv_get(&kv_key(i)).unwrap();
                });
            });

            // Percentile pass
            let pct_counter = AtomicU64::new(0);
            let label = format!("kv/get/{}/{}", size.label(), mode.label());
            let (p, counters) = measure_with_counters(&bench_db, PERCENTILE_SAMPLES, || {
                let i = pct_counter.fetch_add(1, Ordering::Relaxed) % WARMUP_COUNT;
                bench_db.db.kv_get(&kv_key(i)).unwrap();
            });
            report_percentiles(&label, &p);
            report_counters(&label, &counters, PERCENTILE_SAMPLES as u64);
        }
    }
    group.finish();
}

// =============================================================================
// DELETE — 1KB default, all durability modes
// =============================================================================

fn kv_delete(c: &mut Criterion) {
    let mut group = c.benchmark_group("kv/delete");
    group.throughput(Throughput::Elements(1));

    eprintln!("\n--- Latency Percentiles: kv/delete ---");
    for mode in DurabilityConfig::ALL {
        let bench_db = create_db(mode);
        for i in 0..WARMUP_COUNT {
            bench_db.db.kv_put(&kv_key(i), kv_value()).unwrap();
        }
        let counter = AtomicU64::new(0);
        group.bench_function(BenchmarkId::new("durability", mode.label()), |b| {
            b.iter(|| {
                let i = counter.fetch_add(1, Ordering::Relaxed) % WARMUP_COUNT;
                let key = kv_key(i);
                bench_db.db.kv_delete(&key).unwrap();
                bench_db.db.kv_put(&key, kv_value()).unwrap();
            });
        });

        let pct_counter = AtomicU64::new(0);
        let label = format!("kv/delete/{}", mode.label());
        let (p, counters) = measure_with_counters(&bench_db, PERCENTILE_SAMPLES, || {
            let i = pct_counter.fetch_add(1, Ordering::Relaxed) % WARMUP_COUNT;
            let key = kv_key(i);
            bench_db.db.kv_delete(&key).unwrap();
            bench_db.db.kv_put(&key, kv_value()).unwrap();
        });
        report_percentiles(&label, &p);
        report_counters(&label, &counters, PERCENTILE_SAMPLES as u64);
    }
    group.finish();
}

// =============================================================================
// LIST PREFIX — 1KB default, all durability modes
// =============================================================================

fn kv_list_prefix(c: &mut Criterion) {
    let mut group = c.benchmark_group("kv/list_prefix");
    group.throughput(Throughput::Elements(1));

    eprintln!("\n--- Latency Percentiles: kv/list_prefix ---");
    for mode in DurabilityConfig::ALL {
        let bench_db = create_db(mode);
        for i in 0..1000u64 {
            bench_db
                .db
                .kv_put(&kv_key_with_prefix("alpha:", i), kv_value())
                .unwrap();
            bench_db
                .db
                .kv_put(&kv_key_with_prefix("beta:", i), kv_value())
                .unwrap();
        }
        group.bench_function(BenchmarkId::new("durability", mode.label()), |b| {
            b.iter(|| {
                bench_db.db.kv_list(Some("alpha:")).unwrap();
            });
        });

        let label = format!("kv/list_prefix/{}", mode.label());
        let (p, counters) = measure_with_counters(&bench_db, PERCENTILE_SAMPLES, || {
            bench_db.db.kv_list(Some("alpha:")).unwrap();
        });
        report_percentiles(&label, &p);
        report_counters(&label, &counters, PERCENTILE_SAMPLES as u64);
    }
    group.finish();
}

criterion_group!(benches, kv_put, kv_get, kv_delete, kv_list_prefix);
criterion_main!(benches);
