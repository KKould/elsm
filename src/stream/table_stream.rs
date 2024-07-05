use std::{
    marker::PhantomData,
    pin::{pin, Pin},
    sync::Arc,
    task::{Context, Poll},
};

use arrow::{
    array::Scalar,
    compute::kernels::cmp::{gt_eq, lt_eq},
};
use futures::{Stream, StreamExt};
use parquet::arrow::{
    arrow_reader::{ArrowPredicate, ArrowPredicateFn, ArrowReaderMetadata, RowFilter},
    async_reader::ParquetRecordBatchStream,
    ParquetRecordBatchStreamBuilder, ProjectionMask,
};
use pin_project::pin_project;

use crate::{
    schema::Schema,
    stream::{batch_stream::BatchStream, StreamError},
    wal::{
        provider::{FileProvider, FileType},
        FileId, FileManager,
    },
};

#[pin_project]
pub(crate) struct TableStream<'stream, S, FP>
where
    S: Schema,
    FP: FileProvider,
{
    inner: ParquetRecordBatchStream<FP::File>,
    stream: Option<BatchStream<S>>,
    file_manager: Arc<FileManager<FP>>,
    _p: PhantomData<&'stream ()>,
}

impl<'stream, S, FP> TableStream<'stream, S, FP>
where
    S: Schema,
    FP: FileProvider,
{
    pub(crate) async fn new(
        file_manager: Arc<FileManager<FP>>,
        gen: FileId,
        lower: Option<&S::PrimaryKey>,
        upper: Option<&S::PrimaryKey>,
    ) -> Result<TableStream<'stream, S, FP>, StreamError<S::PrimaryKey, S>> {
        let lower = if let Some(l) = lower {
            Some(S::to_primary_key_array(vec![l.clone()]))
        } else {
            None
        };
        let upper = if let Some(u) = upper {
            Some(S::to_primary_key_array(vec![u.clone()]))
        } else {
            None
        };

        let mut file = file_manager
            .file_provider
            .open(gen, FileType::PARQUET)
            .await
            .map_err(StreamError::Io)?;
        let meta = ArrowReaderMetadata::load_async(&mut file, Default::default())
            .await
            .map_err(StreamError::Parquet)?;
        let mut builder = ParquetRecordBatchStreamBuilder::new_with_metadata(file, meta);
        let file_metadata = builder.metadata().file_metadata();

        let mut predicates = Vec::with_capacity(2);

        if let Some(lower_scalar) = lower {
            predicates.push(Box::new(ArrowPredicateFn::new(
                ProjectionMask::roots(file_metadata.schema_descr(), [0]),
                move |record_batch| gt_eq(record_batch.column(0), &Scalar::new(&lower_scalar)),
            )) as Box<dyn ArrowPredicate>)
        }
        if let Some(upper_scalar) = upper {
            predicates.push(Box::new(ArrowPredicateFn::new(
                ProjectionMask::roots(file_metadata.schema_descr(), [0]),
                move |record_batch| lt_eq(record_batch.column(0), &Scalar::new(&upper_scalar)),
            )) as Box<dyn ArrowPredicate>)
        }

        let row_filter = RowFilter::new(predicates);
        builder = builder.with_row_filter(row_filter);

        let mut reader = builder.build().map_err(StreamError::Parquet)?;

        let mut stream = None;
        if let Some(result) = reader.next().await {
            stream = Some(BatchStream::new(result.map_err(StreamError::Parquet)?));
        }

        Ok(TableStream {
            inner: reader,
            stream,
            file_manager,
            _p: Default::default(),
        })
    }
}

impl<S, FP> Stream for TableStream<'_, S, FP>
where
    S: Schema,
    FP: FileProvider,
{
    type Item = Result<(S::PrimaryKey, Option<S>), StreamError<S::PrimaryKey, S>>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.stream.is_none() {
            return Poll::Ready(None);
        }
        // Safety: It cannot be none here, because it has been judged above
        match Pin::new(self.stream.as_mut().unwrap()).poll_next(cx) {
            Poll::Ready(None) => match Pin::new(&mut self.inner).poll_next(cx) {
                Poll::Ready(Some(Ok(batch))) => {
                    self.stream = Some(BatchStream::new(batch));
                    self.poll_next(cx)
                }
                Poll::Ready(Some(Err(err))) => Poll::Ready(Some(Err(StreamError::Parquet(err)))),
                Poll::Ready(None) => Poll::Ready(None),
                Poll::Pending => Poll::Pending,
            },
            poll => poll,
        }
    }
}
