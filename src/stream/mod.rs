use std::{
    fmt::Debug,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use executor::futures::Stream;
use pin_project::pin_project;
use thiserror::Error;

use crate::{
    index_batch::stream::IndexBatchStream,
    mem_table::stream::MemTableStream,
    schema::Schema,
    serdes::{Decode, Encode},
    stream::{buf_stream::BufStream, level_stream::LevelStream, table_stream::TableStream},
    transaction::TransactionStream,
};

pub(crate) mod batch_stream;
pub(crate) mod buf_stream;
pub(crate) mod level_stream;
pub(crate) mod merge_inner_stream;
pub(crate) mod merge_stream;
pub(crate) mod table_stream;

#[pin_project(project = EStreamImplProj)]
pub(crate) enum EStreamImpl<'a, S, G, F>
where
    S: Schema,
    G: Send + Sync + 'static,
    F: Fn(&S) -> G + Sync + 'static,
{
    Buf(#[pin] BufStream<'a, S::PrimaryKey, G, StreamError<S::PrimaryKey, S>>),
    IndexBatch(#[pin] IndexBatchStream<'a, S, G, F>),
    MemTable(#[pin] MemTableStream<'a, S, G, F>),
    TransactionInner(#[pin] TransactionStream<'a, S, G, F, StreamError<S::PrimaryKey, S>>),
}

#[pin_project(project = EInnerStreamImplProj)]
pub(crate) enum EInnerStreamImpl<'a, S>
where
    S: Schema,
{
    Table(#[pin] TableStream<'a, S>),
    Level(#[pin] LevelStream<'a, S>),
}

impl<'a, S, G, F> Stream for EStreamImpl<'a, S, G, F>
where
    S: Schema,
    G: Send + Sync + 'static,
    F: Fn(&S) -> G + Sync + 'static,
{
    type Item = Result<(Arc<S::PrimaryKey>, Option<G>), StreamError<S::PrimaryKey, S>>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.project() {
            EStreamImplProj::Buf(stream) => stream.poll_next(cx),
            EStreamImplProj::IndexBatch(stream) => stream.poll_next(cx),
            EStreamImplProj::MemTable(stream) => stream.poll_next(cx),
            EStreamImplProj::TransactionInner(stream) => stream.poll_next(cx),
        }
    }
}

impl<'a, S> Stream for EInnerStreamImpl<'a, S>
where
    S: Schema,
{
    type Item = Result<(Arc<S::PrimaryKey>, Option<S>), StreamError<S::PrimaryKey, S>>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.project() {
            EInnerStreamImplProj::Table(stream) => stream.poll_next(cx),
            EInnerStreamImplProj::Level(stream) => stream.poll_next(cx),
        }
    }
}

#[derive(Debug, Error)]
pub enum StreamError<K, V>
where
    K: Encode + Decode,
    V: Decode,
{
    #[error("compaction key encode error: {0}")]
    KeyEncode(#[source] <K as Encode>::Error),
    #[error("compaction key decode error: {0}")]
    KeyDecode(#[source] <K as Decode>::Error),
    #[error("compaction value decode error: {0}")]
    ValueDecode(#[source] <V as Decode>::Error),
    #[error("compaction io error: {0}")]
    Io(#[source] std::io::Error),
    #[error("compaction arrow error: {0}")]
    Arrow(#[source] arrow::error::ArrowError),
    #[error("compaction parquet error: {0}")]
    Parquet(#[source] parquet::errors::ParquetError),
}
