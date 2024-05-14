mod consistent_hash;
pub(crate) mod mem_table;
pub(crate) mod oracle;
pub(crate) mod record;
pub mod serdes;
pub mod transaction;
pub(crate) mod utils;
pub mod wal;

use arrow::array::{GenericBinaryBuilder, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::fmt::Debug;
use std::ops::Bound;
use std::{error, future::Future, hash::Hash, io, mem, sync::Arc};

use crate::record::EncodeError;
use crate::utils::CmpKeyItem;
use consistent_hash::jump_consistent_hash;
use crossbeam_queue::ArrayQueue;
use executor::shard::Shard;
use futures::io::Cursor;
use futures::{executor::block_on, AsyncWrite};
use lazy_static::lazy_static;
use mem_table::MemTable;
use oracle::Oracle;
use record::{Record, RecordType};
use serdes::Encode;
use transaction::Transaction;
use wal::{provider::WalProvider, WalFile, WalManager, WalWrite, WriteError};

lazy_static! {
    pub static ref ELSM_SCHEMA: SchemaRef = {
        Arc::new(Schema::new(vec![
            Field::new("key", DataType::LargeBinary, false),
            Field::new("value", DataType::LargeBinary, true),
        ]))
    };
}

pub type Offset = i64;

#[derive(Debug)]
pub struct DbOption {
    pub max_wal_size: usize,
    pub immutable_chunk_num: usize,
}

#[derive(Debug)]
struct MutableShard<K, V, T, W>
where
    K: Ord,
    T: Ord,
{
    mutable: MemTable<K, V, T>,
    wal: WalFile<W, Arc<K>, V, T>,
}

#[derive(Debug)]
pub struct Db<K, V, O, WP>
where
    K: Ord,
    O: Oracle<K>,
    WP: WalProvider,
{
    option: DbOption,
    pub(crate) oracle: O,
    wal_manager: Arc<WalManager<WP>>,
    pub(crate) mutable_shards:
        Shard<unsend::lock::RwLock<MutableShard<K, V, O::Timestamp, WP::File>>>,
    pub(crate) immutable: ArrayQueue<RecordBatch>,
}

impl<K, V, O, WP> Db<K, V, O, WP>
where
    K: Ord + Send + Sync,
    V: Send,
    O: Oracle<K>,
    O::Timestamp: Send,
    WP: WalProvider + Send,
{
    pub fn new(oracle: O, wal_provider: WP, option: DbOption) -> io::Result<Self> {
        let wal_manager = Arc::new(WalManager::new(wal_provider, option.max_wal_size));
        let mutable_shards = Shard::new(|| {
            unsend::lock::RwLock::new(crate::MutableShard {
                mutable: MemTable::default(),
                wal: block_on(wal_manager.create_wal_file()).unwrap(),
            })
        });
        Ok(Db {
            option,
            oracle,
            wal_manager,
            mutable_shards,
            immutable: ArrayQueue::new(1),
        })
    }
}

impl<K, V, O, WP> Db<K, V, O, WP>
where
    K: Encode + Ord + Hash + Send + Sync + 'static,
    V: Encode + Send + 'static,
    O: Oracle<K>,
    O::Timestamp: Encode + Copy + Send + Sync + 'static,
    WP: WalProvider,
    WP::File: AsyncWrite,
{
    pub fn new_txn(self: &Arc<Self>) -> Transaction<K, V, Self> {
        Transaction::new(self.clone())
    }

    async fn write(
        &self,
        record_type: RecordType,
        key: Arc<K>,
        ts: O::Timestamp,
        value: Option<V>,
    ) -> Result<(), WriteError<<Record<Arc<K>, V, O::Timestamp> as Encode>::Error>> {
        let consistent_hash =
            jump_consistent_hash(fxhash::hash64(&key), executor::worker_num()) as usize;
        let wal_manager = self.wal_manager.clone();
        let freeze = self
            .mutable_shards
            .with(consistent_hash, move |local| async move {
                let mut local = local.write().await;
                let result = local
                    .wal
                    .write(Record::new(record_type, &key, &ts, value.as_ref()))
                    .await;
                match result {
                    Ok(_) => {
                        local.mutable.insert(key, ts, value);
                        Ok(None)
                    }
                    Err(e) => {
                        if let WriteError::MaxSizeExceeded = e {
                            let mut wal_file = wal_manager
                                .create_wal_file()
                                .await
                                .map_err(WriteError::Io)?;
                            let mut mem_table = MemTable::default();
                            mem::swap(&mut local.wal, &mut wal_file);
                            mem::swap(&mut local.mutable, &mut mem_table);

                            wal_file.close().await.map_err(WriteError::Io)?;
                            Ok(Some(mem_table))
                        } else {
                            Err(e)
                        }
                    }
                }
            })
            .await?;
        if let Some(mem_table) = freeze {
            if let Err(mem_table) = self.immutable.push(Self::freeze(mem_table).await?) {
                let _ = mem_table;
            }
        }
        Ok(())
    }

    async fn get<G>(
        &self,
        key: &Arc<K>,
        ts: &O::Timestamp,
        f: impl FnOnce(&V) -> G + Send + 'static,
    ) -> Option<G>
    where
        G: Send + 'static,
        O::Timestamp: Sync,
    {
        let consistent_hash =
            jump_consistent_hash(fxhash::hash64(key), executor::worker_num()) as usize;

        // Safety: read-only would not break data.
        let (key, ts) = unsafe {
            (
                mem::transmute::<_, &Arc<K>>(key),
                mem::transmute::<_, &O::Timestamp>(ts),
            )
        };

        self.mutable_shards
            .with(consistent_hash, move |local| async move {
                local.read().await.mutable.get(key, ts).map(f)
            })
            .await
    }

    async fn range_scan<G>(
        &self,
        lower: Bound<Arc<K>>,
        upper: Bound<Arc<K>>,
        ts: &O::Timestamp,
        f: impl FnOnce(&V) -> G + Send + Copy + 'static,
    ) -> Vec<G>
    where
        G: Send + Sync + 'static,
        O::Timestamp: Sync,
    {
        fn merge_sort<K, G>(mut arrays: Vec<Vec<CmpKeyItem<Arc<K>, Option<G>>>>) -> Vec<G>
        where
            K: Ord,
        {
            let mut heap = BinaryHeap::new();

            for (i, array) in arrays.iter_mut().enumerate() {
                if array.is_empty() {
                    continue;
                }
                let val = array.remove(0);
                heap.push(Reverse((val, i)));
            }

            let mut result = Vec::new();

            while let Some(Reverse((CmpKeyItem { _value: value, .. }, idx))) = heap.pop() {
                if let Some(value) = value {
                    result.push(value);
                }
                if arrays[idx].is_empty() {
                    continue;
                }
                let next_val = arrays[idx].remove(0);
                heap.push(Reverse((next_val, idx)));
            }

            result
        }
        let mut arrarys = Vec::with_capacity(executor::worker_num());

        for i in 0..executor::worker_num() {
            let lower = lower.clone();
            let upper = upper.clone();
            let ts = *ts;

            let items: Vec<_> = self
                .mutable_shards
                .with(i, move |local| async move {
                    let guard = local.read().await;
                    // TODO: MergeIterator
                    guard
                        .mutable
                        .range(lower.as_ref(), upper.as_ref(), &ts)
                        .map(|(k, v)| CmpKeyItem {
                            key: k.clone(),
                            _value: v.as_ref().map(f),
                        })
                        .collect()
                })
                .await;
            arrarys.push(items);
        }

        merge_sort(arrarys)
    }

    async fn write_batch(
        &self,
        mut kvs: impl ExactSizeIterator<Item = (Arc<K>, O::Timestamp, Option<V>)>,
    ) -> Result<(), WriteError<<Record<Arc<K>, V, O::Timestamp> as Encode>::Error>> {
        match kvs.len() {
            0 => Ok(()),
            1 => {
                let (key, ts, value) = kvs.next().unwrap();
                self.write(RecordType::Full, key, ts, value).await
            }
            len => {
                let (key, ts, value) = kvs.next().unwrap();
                self.write(RecordType::First, key, ts, value).await?;

                for (key, ts, value) in (&mut kvs).take(len - 2) {
                    self.write(RecordType::Middle, key, ts, value).await?;
                }

                let (key, ts, value) = kvs.next().unwrap();
                self.write(RecordType::Last, key, ts, value).await
            }
        }
    }

    async fn freeze(
        mem_table: MemTable<K, V, <O as Oracle<K>>::Timestamp>,
    ) -> Result<RecordBatch, WriteError<<Record<Arc<K>, V, O::Timestamp> as Encode>::Error>> {
        fn clear(buf: &mut Cursor<Vec<u8>>) {
            buf.get_mut().clear();
            buf.set_position(0);
        }

        let mut buf = Cursor::new(vec![0; 128]);
        let mut key_builder = GenericBinaryBuilder::<Offset>::new();
        let mut value_builder = GenericBinaryBuilder::<Offset>::new();

        for (key, value) in mem_table.iter() {
            clear(&mut buf);
            key.encode(&mut buf).await.map_err(EncodeError::Key)?;
            key_builder.append_value(buf.get_ref());

            if let Some(value) = value {
                clear(&mut buf);
                value.encode(&mut buf).await.unwrap();
                value_builder.append_value(buf.get_ref());
            } else {
                value_builder.append_null();
            }
        }
        let keys = key_builder.finish();
        let values = value_builder.finish();

        RecordBatch::try_new(ELSM_SCHEMA.clone(), vec![Arc::new(keys), Arc::new(values)])
            .map_err(WriteError::Arrow)
    }
}

impl<K, V, O, WP> Oracle<K> for Db<K, V, O, WP>
where
    K: Ord,
    O: Oracle<K>,
    WP: WalProvider,
{
    type Timestamp = O::Timestamp;

    fn start_read(&self) -> Self::Timestamp {
        self.oracle.start_read()
    }

    fn read_commit(&self, ts: Self::Timestamp) {
        self.oracle.read_commit(ts)
    }

    fn start_write(&self) -> Self::Timestamp {
        self.oracle.start_write()
    }

    fn write_commit(
        &self,
        read_at: Self::Timestamp,
        write_at: Self::Timestamp,
        in_write: std::collections::HashSet<Arc<K>>,
    ) -> Result<(), oracle::WriteConflict<K>> {
        self.oracle.write_commit(read_at, write_at, in_write)
    }
}

pub trait GetWrite<K, V>: Oracle<K>
where
    K: Ord,
{
    fn get<G>(
        &self,
        key: &Arc<K>,
        ts: &Self::Timestamp,
        f: impl FnOnce(&V) -> G + Send + 'static,
    ) -> impl Future<Output = Option<G>>
    where
        G: Send + 'static,
        Self::Timestamp: Sync;

    fn write(
        &self,
        record_type: RecordType,
        key: Arc<K>,
        ts: Self::Timestamp,
        value: Option<V>,
    ) -> impl Future<Output = Result<(), Box<dyn error::Error + Send + Sync + 'static>>>;

    fn write_batch(
        &self,
        kvs: impl ExactSizeIterator<Item = (Arc<K>, Self::Timestamp, Option<V>)>,
    ) -> impl Future<Output = Result<(), Box<dyn error::Error + Send + Sync + 'static>>>;
}

impl<K, V, O, WP> GetWrite<K, V> for Db<K, V, O, WP>
where
    K: Encode + Ord + Hash + Send + Sync + 'static,
    V: Encode + Send + 'static,
    O: Oracle<K>,
    O::Timestamp: Encode + Copy + Send + Sync + 'static,
    WP: WalProvider,
    WP::File: AsyncWrite,
{
    async fn write(
        &self,
        record_type: RecordType,
        key: Arc<K>,
        ts: O::Timestamp,
        value: Option<V>,
    ) -> Result<(), Box<dyn error::Error + Send + Sync + 'static>> {
        Db::write(self, record_type, key, ts, value).await?;
        Ok(())
    }

    async fn get<G>(
        &self,
        key: &Arc<K>,
        ts: &O::Timestamp,
        f: impl FnOnce(&V) -> G + Send + 'static,
    ) -> Option<G>
    where
        G: Send + 'static,
        O::Timestamp: Sync,
    {
        Db::get(self, key, ts, f).await
    }

    async fn write_batch(
        &self,
        kvs: impl ExactSizeIterator<Item = (Arc<K>, O::Timestamp, Option<V>)>,
    ) -> Result<(), Box<dyn error::Error + Send + Sync + 'static>> {
        Db::write_batch(self, kvs).await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use arrow::array::{Array, AsArray};
    use std::ops::Bound;
    use std::sync::Arc;

    use executor::ExecutorBuilder;
    use futures::io::Cursor;

    use crate::mem_table::MemTable;
    use crate::serdes::Encode;
    use crate::{
        oracle::LocalOracle, transaction::CommitError, wal::provider::in_mem::InMemProvider, Db,
        DbOption, Offset,
    };

    #[test]
    fn read_committed() {
        ExecutorBuilder::new().build().unwrap().block_on(async {
            let db = Arc::new(
                Db::new(
                    LocalOracle::default(),
                    InMemProvider::default(),
                    DbOption {
                        max_wal_size: 64 * 1024 * 1024,
                        immutable_chunk_num: 1,
                    },
                )
                .unwrap(),
            );

            let mut txn = db.new_txn();
            txn.set("key0".to_string(), 0);
            txn.set("key1".to_string(), 1);
            txn.commit().await.unwrap();

            let mut t0 = db.new_txn();
            let mut t1 = db.new_txn();

            t0.set(
                "key0".into(),
                t0.get(&Arc::new("key1".to_owned()), |v| *v).await.unwrap(),
            );
            t1.set(
                "key1".into(),
                t1.get(&Arc::new("key0".to_owned()), |v| *v).await.unwrap(),
            );

            t0.commit().await.unwrap();
            t1.commit().await.unwrap();

            let txn = db.new_txn();

            assert_eq!(
                txn.get(&Arc::from("key0".to_string()), |v| *v).await,
                Some(1)
            );
            assert_eq!(
                txn.get(&Arc::from("key1".to_string()), |v| *v).await,
                Some(0)
            );
        });
    }

    #[test]
    fn range_scan() {
        ExecutorBuilder::new().build().unwrap().block_on(async {
            let db = Arc::new(
                Db::new(
                    LocalOracle::default(),
                    InMemProvider::default(),
                    DbOption {
                        max_wal_size: 64 * 1024 * 1024,
                        immutable_chunk_num: 1,
                    },
                )
                .unwrap(),
            );

            let mut txn = db.new_txn();
            txn.set("key0".to_string(), 0);
            txn.set("key1".to_string(), 1);
            txn.set("key2".to_string(), 2);
            txn.set("key3".to_string(), 3);
            txn.commit().await.unwrap();

            let items = db
                .range_scan(
                    Bound::Excluded(Arc::new("key0".to_string())),
                    Bound::Excluded(Arc::new("key3".to_string())),
                    &1,
                    |v| *v,
                )
                .await;

            assert_eq!(items.len(), 2);
            assert_eq!(items[0], 1);
            assert_eq!(items[1], 2);
        });
    }

    #[test]
    fn write_conflicts() {
        ExecutorBuilder::new().build().unwrap().block_on(async {
            let db = Arc::new(
                Db::new(
                    LocalOracle::default(),
                    InMemProvider::default(),
                    DbOption {
                        max_wal_size: 64 * 1024 * 1024,
                        immutable_chunk_num: 1,
                    },
                )
                .unwrap(),
            );

            let mut txn = db.new_txn();
            txn.set("key0".to_string(), 0);
            txn.set("key1".to_string(), 1);
            txn.commit().await.unwrap();

            let mut t0 = db.new_txn();
            let mut t1 = db.new_txn();
            let mut t2 = db.new_txn();

            t0.set(
                "key0".into(),
                t0.get(&Arc::new("key1".to_owned()), |v| *v).await.unwrap(),
            );
            t1.set(
                "key0".into(),
                t1.get(&Arc::new("key0".to_owned()), |v| *v).await.unwrap(),
            );
            t1.set("key2".into(), 2);
            t2.set("key2".into(), 3);

            t0.commit().await.unwrap();

            let commit = t1.commit().await;
            assert!(commit.is_err());
            assert!(t2.commit().await.is_ok());
            if let Err(CommitError::WriteConflict(keys)) = commit {
                assert_eq!(db.new_txn().get(&keys[0], |v| *v).await, Some(1));
                return;
            }
            panic!("unreachable");
        });
    }

    #[test]
    fn freeze() {
        fn clear(buf: &mut Cursor<Vec<u8>>) {
            buf.get_mut().clear();
            buf.set_position(0);
        }

        ExecutorBuilder::new().build().unwrap().block_on(async {
            let key_1 = Arc::new("key_1".to_owned());
            let key_2 = Arc::new("key_2".to_owned());
            let key_3 = Arc::new("key_3".to_owned());
            let value_1 = "value_1".to_owned();
            let value_2 = "value_2".to_owned();

            let mut mem_table = MemTable::default();

            mem_table.insert(key_1.clone(), 0, Some(value_1.clone()));
            mem_table.insert(key_2.clone(), 0, Some(value_2.clone()));
            mem_table.insert(key_3.clone(), 0, None);

            let batch = Db::<String, String, LocalOracle<String>, InMemProvider>::freeze(mem_table)
                .await
                .unwrap();

            let keys = batch.column(0);
            let values = batch.column(1);

            let mut buf = Cursor::new(Vec::new());
            key_1.encode(&mut buf).await.unwrap();
            assert_eq!(
                keys.as_binary::<Offset>().value(0),
                buf.get_ref().as_slice()
            );
            clear(&mut buf);
            key_2.encode(&mut buf).await.unwrap();
            assert_eq!(
                keys.as_binary::<Offset>().value(1),
                buf.get_ref().as_slice()
            );
            clear(&mut buf);
            key_3.encode(&mut buf).await.unwrap();
            assert_eq!(
                keys.as_binary::<Offset>().value(2),
                buf.get_ref().as_slice()
            );
            clear(&mut buf);
            value_1.encode(&mut buf).await.unwrap();
            assert_eq!(
                values.as_binary::<Offset>().value(0),
                buf.get_ref().as_slice()
            );
            clear(&mut buf);
            value_2.encode(&mut buf).await.unwrap();
            assert_eq!(
                values.as_binary::<Offset>().value(1),
                buf.get_ref().as_slice()
            );
            assert!(values.as_binary::<Offset>().is_null(2))
        });
    }
}
