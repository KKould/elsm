pub(crate) mod cleaner;
pub(crate) mod edit;
pub(crate) mod set;

use std::{mem, sync::Arc};

use arrow::{
    array::{RecordBatch, Scalar},
    compute::kernels::cmp::eq,
};
use futures::{
    channel::mpsc::{SendError, Sender},
    executor::block_on,
    SinkExt, StreamExt,
};
use parquet::arrow::{
    arrow_reader::{ArrowPredicateFn, ArrowReaderMetadata, RowFilter},
    ParquetRecordBatchStreamBuilder, ProjectionMask,
};
use thiserror::Error;
use tracing::error;

use crate::{
    schema::Schema,
    scope::Scope,
    serdes::Encode,
    stream::{level_stream::LevelStream, table_stream::TableStream, EStreamImpl, StreamError},
    version::cleaner::CleanTag,
    wal::{
        provider::{FileProvider, FileType},
        FileId, FileManager,
    },
};

pub const MAX_LEVEL: usize = 7;

pub(crate) type VersionRef<S, FP> = Arc<Version<S, FP>>;

pub(crate) struct Version<S, FP>
where
    S: Schema,
    FP: FileProvider,
{
    pub(crate) num: usize,
    pub(crate) level_slice: [Vec<Scope<S::PrimaryKey>>; MAX_LEVEL],
    pub(crate) clean_sender: Sender<CleanTag>,
    pub(crate) file_manager: Arc<FileManager<FP>>,
}

impl<S, FP> Clone for Version<S, FP>
where
    S: Schema,
    FP: FileProvider,
{
    fn clone(&self) -> Self {
        let mut level_slice = Version::<S, FP>::level_slice_new();

        for (level, scopes) in self.level_slice.iter().enumerate() {
            for scope in scopes {
                level_slice[level].push(scope.clone());
            }
        }

        Self {
            num: self.num,
            level_slice,
            clean_sender: self.clean_sender.clone(),
            file_manager: self.file_manager.clone(),
        }
    }
}

impl<S, FP> Version<S, FP>
where
    S: Schema,
    FP: FileProvider,
{
    pub(crate) async fn query(
        &self,
        key: &S::PrimaryKey,
    ) -> Result<Option<RecordBatch>, VersionError<S>> {
        let key_array = S::to_primary_key_array(vec![key.clone()]);

        for scope in self.level_slice[0].iter().rev() {
            if !scope.is_between(key) {
                continue;
            }
            if let Some(batch) =
                Self::read_parquet(scope.gen, &key_array, &self.file_manager).await?
            {
                return Ok(Some(batch));
            }
        }
        for level in self.level_slice[1..6].iter() {
            if level.is_empty() {
                continue;
            }
            let index = Self::scope_search(key, level);
            if !level[index].is_between(key) {
                continue;
            }
            if let Some(batch) =
                Self::read_parquet(level[index].gen, &key_array, &self.file_manager).await?
            {
                return Ok(Some(batch));
            }
        }

        Ok(None)
    }

    pub(crate) fn scope_search(key: &S::PrimaryKey, level: &[Scope<S::PrimaryKey>]) -> usize {
        level
            .binary_search_by(|scope| scope.min.cmp(key))
            .unwrap_or_else(|index| index.saturating_sub(1))
    }

    pub(crate) fn tables_len(&self, level: usize) -> usize {
        self.level_slice[level].len()
    }

    pub(crate) fn level_slice_new() -> [Vec<Scope<S::PrimaryKey>>; 7] {
        [
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
        ]
    }

    pub(crate) async fn iters<'a>(
        &self,
        iters: &mut Vec<EStreamImpl<'a, S, FP>>,
        lower: Option<&S::PrimaryKey>,
        upper: Option<&S::PrimaryKey>,
    ) -> Result<(), StreamError<S::PrimaryKey, S>> {
        for scope in self.level_slice[0].iter() {
            iters.push(EStreamImpl::Table(
                TableStream::new(self.file_manager.clone(), scope.gen, lower, upper).await?,
            ))
        }
        for scopes in self.level_slice[1..].iter() {
            if scopes.is_empty() {
                continue;
            }
            let gens = scopes.iter().map(|scope| scope.gen).collect::<Vec<_>>();
            iters.push(EStreamImpl::Level(
                LevelStream::new(self.file_manager.clone(), gens, lower, upper).await?,
            ));
        }
        Ok(())
    }

    async fn read_parquet(
        scope_gen: FileId,
        key_scalar: &S::PrimaryKeyArray,
        file_manager: &FileManager<FP>,
    ) -> Result<Option<RecordBatch>, VersionError<S>> {
        let mut file = file_manager
            .file_provider
            .open(scope_gen, FileType::PARQUET)
            .await
            .map_err(VersionError::Io)?;
        let meta = ArrowReaderMetadata::load_async(&mut file, Default::default())
            .await
            .map_err(VersionError::Parquet)?;
        let mut builder = ParquetRecordBatchStreamBuilder::new_with_metadata(file, meta);
        let file_metadata = builder.metadata().file_metadata();

        let key_scalar = unsafe { mem::transmute::<_, &'static S::PrimaryKeyArray>(key_scalar) };
        let filter = ArrowPredicateFn::new(
            ProjectionMask::roots(file_metadata.schema_descr(), [0]),
            move |record_batch| eq(record_batch.column(0), &Scalar::new(&key_scalar)),
        );
        let row_filter = RowFilter::new(vec![Box::new(filter)]);
        builder = builder.with_row_filter(row_filter);

        let mut stream = builder.build().map_err(VersionError::Parquet)?;

        if let Some(result) = stream.next().await {
            return Ok(Some(result.map_err(VersionError::Parquet)?));
        }
        Ok(None)
    }
}

impl<S, FP> Drop for Version<S, FP>
where
    S: Schema,
    FP: FileProvider,
{
    fn drop(&mut self) {
        block_on(async {
            if let Err(err) = self
                .clean_sender
                .send(CleanTag::Clean {
                    version_num: self.num,
                })
                .await
            {
                error!("[Version Drop Error]: {}", err)
            }
        });
    }
}

#[derive(Debug, Error)]
pub enum VersionError<S>
where
    S: Schema,
{
    #[error("version encode error: {0}")]
    Encode(#[source] <S::PrimaryKey as Encode>::Error),
    #[error("version io error: {0}")]
    Io(#[source] std::io::Error),
    #[error("version parquet error: {0}")]
    Parquet(#[source] parquet::errors::ParquetError),
    #[error("version send error: {0}")]
    Send(#[source] SendError),
}
