use std::sync::{
    atomic::{AtomicU32, Ordering},
    Arc,
};

use arrow::{
    array::{
        Array, StringArray, StringBuilder, StructArray, StructBuilder, UInt32Array, UInt32Builder,
    },
    datatypes::{DataType, Field, Fields, SchemaRef},
    record_batch::RecordBatch,
};
use criterion::{criterion_group, criterion_main, Criterion};
use elsm::{
    oracle::LocalOracle,
    record::RecordType,
    schema::{Builder, Schema},
    serdes::{Decode, Encode},
    wal::provider::{fs::Fs, in_mem::InMemProvider},
    Db, DbOption,
};
use elsm_marco::elsm_schema;
use lazy_static::lazy_static;
#[cfg(unix)]
use pprof::criterion::{Output, PProfProfiler};
use rand::{distributions::Alphanumeric, rngs::StdRng, Rng, SeedableRng};
use tokio::{
    io,
    io::{AsyncRead, AsyncReadExt, AsyncWrite},
};

fn counter() -> usize {
    use std::sync::atomic::{AtomicUsize, Ordering::Relaxed};

    static C: AtomicUsize = AtomicUsize::new(0);

    C.fetch_add(1, Relaxed)
}

#[derive(Debug, Eq, PartialEq)]
#[elsm_schema]
pub(crate) struct TestString {
    #[primary_key]
    pub(crate) string_0: String,
    pub(crate) string_1: String,
}

#[derive(Debug, Eq, PartialEq)]
#[elsm_schema]
pub(crate) struct User {
    #[primary_key]
    pub(crate) id: u32,
    pub(crate) i_number_0: u32,
}

fn random(n: u32) -> u32 {
    use std::{cell::Cell, num::Wrapping};

    thread_local! {
        static RNG: Cell<Wrapping<u32>> = Cell::new(Wrapping(1406868647));
    }

    RNG.with(|rng| {
        let mut x = rng.get();
        x ^= x << 13;
        x ^= x >> 17;
        x ^= x << 5;
        rng.set(x);

        ((x.0 as u64).wrapping_mul(n as u64) >> 32) as u32
    })
}

fn elsm_bulk_load(c: &mut Criterion) {
    let count = AtomicU32::new(0_u32);
    let bytes = |len| -> String {
        let r = StdRng::seed_from_u64(count.fetch_add(1, Ordering::Relaxed) as u64);

        r.sample_iter(&Alphanumeric)
            .take(len)
            .map(char::from)
            .collect()
    };

    let mut bench = |key_len, val_len| {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(8)
            .enable_all()
            .build()
            .unwrap();
        let path = format!("bulk load key/value lengths {}/{}", key_len, val_len);
        let db = rt.block_on(async {
            Db::new(
                LocalOracle::default(),
                Fs::new(&path).unwrap(),
                DbOption::new(&path),
            )
            .await
            .unwrap()
        });

        c.bench_function(
            &format!("bulk load key/value lengths {}/{}", key_len, val_len),
            |b| {
                b.to_async(&rt).iter(|| async {
                    db.write(
                        RecordType::Full,
                        0,
                        TestStringInner::new(bytes(key_len), bytes(val_len)),
                    )
                    .await
                    .unwrap();
                })
            },
        );
    };

    for key_len in &[10_usize, 128, 256, 512] {
        for val_len in &[0_usize, 10, 128, 256, 512, 1024, 2048, 4096, 8192] {
            bench(*key_len, *val_len)
        }
    }
}

fn elsm_monotonic_crud(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(8)
        .enable_all()
        .build()
        .unwrap();
    let path = format!("monotonic_crud/{}/", counter());
    let db = rt.block_on(async {
        Db::new(
            LocalOracle::default(),
            Fs::new(&path).unwrap(),
            DbOption::new(path),
        )
        .await
        .unwrap()
    });

    c.bench_function("monotonic inserts", |b| {
        let count = AtomicU32::new(0_u32);
        b.to_async(&rt).iter(|| async {
            let count = count.fetch_add(1, Ordering::Relaxed);
            db.write(RecordType::Full, 0, UserInner::new(count, count))
                .await
                .unwrap();
        })
    });

    c.bench_function("monotonic gets", |b| {
        let count = AtomicU32::new(0_u32);
        b.to_async(&rt).iter(|| async {
            let count = count.fetch_add(1, Ordering::Relaxed);
            let _ = db.get(&count, &0).await;
        })
    });

    c.bench_function("monotonic removals", |b| {
        let count = AtomicU32::new(0_u32);
        b.to_async(&rt).iter(|| async {
            let count = count.fetch_add(1, Ordering::Relaxed);
            db.remove(RecordType::Full, 0, count).await.unwrap();
        })
    });
}

fn elsm_random_crud(c: &mut Criterion) {
    const SIZE: u32 = 65536;
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(8)
        .enable_all()
        .build()
        .unwrap();
    let path = format!("random_crud/{}/", counter());
    let db = rt.block_on(async {
        Db::new(
            LocalOracle::default(),
            Fs::new(&path).unwrap(),
            DbOption::new(&path),
        )
        .await
        .unwrap()
    });

    c.bench_function("random inserts", |b| {
        b.to_async(&rt).iter(|| async {
            db.write(
                RecordType::Full,
                0,
                UserInner::new(random(SIZE), random(SIZE)),
            )
            .await
            .unwrap();
        })
    });

    c.bench_function("random gets", |b| {
        b.to_async(&rt).iter(|| async {
            let _ = db.get(&random(SIZE), &0).await;
        })
    });

    c.bench_function("random removals", |b| {
        b.to_async(&rt).iter(|| async {
            db.remove(RecordType::Full, 0, random(SIZE)).await.unwrap();
        })
    });
}

fn elsm_empty_opens(c: &mut Criterion) {
    let _ = std::fs::remove_dir_all("empty_opens");
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(8)
        .enable_all()
        .build()
        .unwrap();
    let path = format!("empty_opens/{}/", counter());

    c.bench_function("empty opens", |b| {
        b.to_async(&rt).iter(|| async {
            Db::<UserInner, LocalOracle<<UserInner as Schema>::PrimaryKey>, Fs>::new(
                LocalOracle::default(),
                Fs::new(&path).unwrap(),
                DbOption::new(&path),
            )
            .await
            .unwrap()
        })
    });
    let _ = std::fs::remove_dir_all("empty_opens");
}
#[cfg(unix)]
criterion_group!(
    name = benches;
    config = Criterion::default().sample_size(100000).with_profiler(PProfProfiler::new(100, Output::Flamegraph(None)));
    targets = elsm_bulk_load, elsm_monotonic_crud, elsm_random_crud, elsm_empty_opens
);
#[cfg(windows)]
criterion_group!(
    name = benches;
    config = Criterion::default().sample_size(100000);
    targets = elsm_bulk_load, elsm_monotonic_crud, elsm_random_crud, elsm_empty_opens
);
criterion_main!(benches);
