use std::{
    mem,
    sync::{
        atomic::{AtomicU32, Ordering},
        Arc,
    },
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
    wal::provider::fs::Fs,
    Db, DbOption,
};
use elsm_marco::elsm_schema;
use futures::future;
use lazy_static::lazy_static;
#[cfg(unix)]
use pprof::criterion::{Output, PProfProfiler};
use rand::{distributions::Alphanumeric, rngs::StdRng, Rng, SeedableRng};
use tokio::{
    io,
    io::{AsyncRead, AsyncWrite},
    task,
};

const THREAD_NUM: usize = 8;

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
    fn bytes(len: usize, count: &AtomicU32) -> String {
        let r = StdRng::seed_from_u64(count.fetch_add(1, Ordering::Relaxed) as u64);

        r.sample_iter(&Alphanumeric)
            .take(len)
            .map(char::from)
            .collect()
    }

    async fn test_fn(
        db: &Db<TestStringInner, LocalOracle<String>, Fs>,
        count: &AtomicU32,
        key_len: usize,
        val_len: usize,
    ) {
        db.write(
            RecordType::Full,
            0,
            TestStringInner::new(bytes(key_len, count), bytes(val_len, count)),
        )
        .await
        .unwrap();
    }

    let mut bench = |count, key_len, val_len| {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(8)
            .enable_all()
            .build()
            .unwrap();
        let path = format!("bulk_load_key_value_lengths_{}_{}", key_len, val_len);
        let db: Db<TestStringInner, LocalOracle<String>, Fs> = rt.block_on(async {
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
                // Safety: read-only would not break data.
                b.to_async(&rt).iter(|| {
                    test_fn(
                        unsafe { mem::transmute::<_, &'static Db<_, _, _>>(&db) },
                        count,
                        key_len,
                        val_len,
                    )
                })
            },
        );
        c.bench_function(
            &format!(
                "multi thread bulk load key/value lengths {}/{}",
                key_len, val_len
            ),
            |b| {
                b.to_async(&rt).iter(|| async {
                    let mut tasks = vec![];

                    for _ in 0..THREAD_NUM {
                        // Safety: read-only would not break data.
                        tasks.push(task::spawn(test_fn(
                            unsafe { mem::transmute::<_, &'static Db<_, _, _>>(&db) },
                            count,
                            key_len,
                            val_len,
                        )));
                    }
                    future::join_all(tasks).await;
                })
            },
        );
    };

    let count = AtomicU32::new(0_u32);

    for key_len in &[10_usize, 128, 256, 512] {
        for val_len in &[0_usize, 10, 128, 256, 512, 1024, 2048, 4096, 8192] {
            bench(
                unsafe { mem::transmute::<_, &'static AtomicU32>(&count) },
                *key_len,
                *val_len,
            )
        }
    }
}

fn elsm_monotonic_crud(c: &mut Criterion) {
    async fn test_fn_write(db: &Db<UserInner, LocalOracle<u32>, Fs>, count: &AtomicU32) {
        let count = count.fetch_add(1, Ordering::Relaxed);
        db.write(RecordType::Full, 0, UserInner::new(count, count))
            .await
            .unwrap();
    }
    async fn test_fn_get(db: &Db<UserInner, LocalOracle<u32>, Fs>, count: &AtomicU32) {
        let count = count.fetch_add(1, Ordering::Relaxed);
        let _ = db.get(&count, &0).await;
    }
    async fn test_fn_remove(db: &Db<UserInner, LocalOracle<u32>, Fs>, count: &AtomicU32) {
        let count = count.fetch_add(1, Ordering::Relaxed);
        db.remove(RecordType::Full, 0, count).await.unwrap();
    }

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
        b.to_async(&rt).iter(|| test_fn_write(&db, &count))
    });
    c.bench_function("multi thread monotonic inserts", |b| {
        let count = AtomicU32::new(0_u32);

        b.to_async(&rt).iter(|| async {
            let (db, count) = unsafe {
                (
                    mem::transmute::<_, &'static Db<_, _, _>>(&db),
                    mem::transmute::<_, &'static AtomicU32>(&count),
                )
            };
            let mut tasks = vec![];

            for _ in 0..THREAD_NUM {
                // Safety: read-only would not break data.
                tasks.push(task::spawn(test_fn_write(db, count)));
            }
            future::join_all(tasks).await;
        })
    });

    c.bench_function("monotonic gets", |b| {
        let count = AtomicU32::new(0_u32);
        b.to_async(&rt).iter(|| test_fn_get(&db, &count))
    });
    c.bench_function("multi thread monotonic gets", |b| {
        let count = AtomicU32::new(0_u32);

        b.to_async(&rt).iter(|| async {
            let (db, count) = unsafe {
                (
                    mem::transmute::<_, &'static Db<_, _, _>>(&db),
                    mem::transmute::<_, &'static AtomicU32>(&count),
                )
            };
            let mut tasks = vec![];

            for _ in 0..THREAD_NUM {
                // Safety: read-only would not break data.
                tasks.push(task::spawn(test_fn_get(&db, &count)));
            }
            future::join_all(tasks).await;
        })
    });

    c.bench_function("monotonic removals", |b| {
        let count = AtomicU32::new(0_u32);
        b.to_async(&rt).iter(|| test_fn_remove(&db, &count))
    });
    c.bench_function("multi thread monotonic removals", |b| {
        let count = AtomicU32::new(0_u32);

        b.to_async(&rt).iter(|| async {
            let (db, count) = unsafe {
                (
                    mem::transmute::<_, &'static Db<_, _, _>>(&db),
                    mem::transmute::<_, &'static AtomicU32>(&count),
                )
            };
            let mut tasks = vec![];

            for _ in 0..THREAD_NUM {
                // Safety: read-only would not break data.
                tasks.push(task::spawn(test_fn_remove(&db, &count)));
            }
            future::join_all(tasks).await;
        })
    });
}

fn elsm_random_crud(c: &mut Criterion) {
    async fn test_fn_write(db: &Db<UserInner, LocalOracle<u32>, Fs>) {
        db.write(
            RecordType::Full,
            0,
            UserInner::new(random(SIZE), random(SIZE)),
        )
        .await
        .unwrap();
    }
    async fn test_fn_get(db: &Db<UserInner, LocalOracle<u32>, Fs>) {
        let _ = db.get(&random(SIZE), &0).await;
    }
    async fn test_fn_remove(db: &Db<UserInner, LocalOracle<u32>, Fs>) {
        db.remove(RecordType::Full, 0, random(SIZE)).await.unwrap();
    }

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
        b.to_async(&rt).iter(|| test_fn_write(&db))
    });
    c.bench_function("multi thread random inserts", |b| {
        b.to_async(&rt).iter(|| async {
            let mut tasks = vec![];

            for _ in 0..THREAD_NUM {
                // Safety: read-only would not break data.
                tasks.push(task::spawn(test_fn_write(unsafe {
                    mem::transmute::<_, &'static Db<_, _, _>>(&db)
                })));
            }
            future::join_all(tasks).await;
        })
    });

    c.bench_function("random gets", |b| b.to_async(&rt).iter(|| test_fn_get(&db)));
    c.bench_function("multi thread random gets", |b| {
        b.to_async(&rt).iter(|| async {
            let mut tasks = vec![];

            for _ in 0..THREAD_NUM {
                // Safety: read-only would not break data.
                tasks.push(task::spawn(test_fn_get(unsafe {
                    mem::transmute::<_, &'static Db<_, _, _>>(&db)
                })));
            }
            future::join_all(tasks).await;
        })
    });

    c.bench_function("random removals", |b| {
        b.to_async(&rt).iter(|| test_fn_remove(&db))
    });
    c.bench_function("multi thread random removals", |b| {
        b.to_async(&rt).iter(|| async {
            let mut tasks = vec![];

            for _ in 0..THREAD_NUM {
                // Safety: read-only would not break data.
                tasks.push(task::spawn(test_fn_get(unsafe {
                    mem::transmute::<_, &'static Db<_, _, _>>(&db)
                })));
            }
            future::join_all(tasks).await;
        })
    });
}

fn elsm_empty_opens(c: &mut Criterion) {
    async fn test_fn_open(path: &String) -> Db<UserInner, LocalOracle<u32>, Fs> {
        Db::new(
            LocalOracle::default(),
            Fs::new(&path).unwrap(),
            DbOption::new(&path),
        )
        .await
        .unwrap()
    }

    let _ = std::fs::remove_dir_all("empty_opens");
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(8)
        .enable_all()
        .build()
        .unwrap();
    let path = format!("empty_opens/{}/", counter());

    c.bench_function("empty opens", |b| {
        b.to_async(&rt).iter(|| test_fn_open(&path))
    });
    // Kould: is it necessary to test multi-threading?
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
