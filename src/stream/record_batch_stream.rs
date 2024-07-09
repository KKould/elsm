use std::{
    pin::Pin,
    task::{Context, Poll},
};

use arrow::array::RecordBatch;
use futures::Stream;
use pin_project::pin_project;

use crate::{
    schema::{BatchBuilder, Schema},
    stream::{merge_stream::MergeStream, StreamError},
    wal::provider::FileProvider,
};

#[pin_project]
pub(crate) struct RecordBatchStream<'stream, S, FP>
where
    S: Schema,
    FP: FileProvider,
{
    row_count: usize,
    batch_size: usize,
    inner: MergeStream<'stream, S, FP>,
    builder: S::BatchBuilder,
}

impl<'stream, S, FP> RecordBatchStream<'stream, S, FP>
where
    S: Schema,
    FP: FileProvider,
{
    pub(crate) fn new(iter: MergeStream<'stream, S, FP>, batch_size: usize) -> Self {
        Self {
            row_count: 0,
            batch_size,
            inner: iter,
            builder: S::batch_builder(),
        }
    }
}

impl<'stream, S, FP> Stream for RecordBatchStream<'stream, S, FP>
where
    S: Schema,
    FP: FileProvider,
{
    type Item = Result<RecordBatch, StreamError<S::PrimaryKey, S>>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        while self.row_count <= self.batch_size {
            match Pin::new(&mut self.inner).poll_next(cx) {
                Poll::Ready(Some(Ok((_, schema)))) => {
                    if let Some(schema) = schema {
                        self.builder.add(schema);
                        self.row_count += 1;
                    }
                }
                Poll::Ready(Some(Err(err))) => return Poll::Ready(Some(Err(err))),
                Poll::Ready(None) => break,
                Poll::Pending => return Poll::Pending,
            }
        }
        Poll::Ready((self.row_count != 0).then(|| {
            self.row_count = 0;
            Ok(self.builder.finish())
        }))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{
        BooleanArray, Int16Array, Int32Array, Int64Array, Int8Array, RecordBatch, StringArray,
        UInt16Array, UInt32Array, UInt64Array, UInt8Array,
    };
    use futures::{executor::block_on, StreamExt};

    use crate::{
        schema::Schema,
        stream::{
            buf_stream::BufStream, merge_stream::MergeStream,
            record_batch_stream::RecordBatchStream, EStreamImpl,
        },
        tests::UserInner,
        wal::provider::in_mem::InMemProvider,
    };

    #[test]
    fn iter() {
        block_on(async {
            let iter_1 = BufStream::new(vec![
                (
                    1,
                    Some(UserInner::new(
                        1,
                        "1".to_string(),
                        true,
                        0,
                        0,
                        0,
                        0,
                        0,
                        0,
                        0,
                        0,
                    )),
                ),
                (
                    2,
                    Some(UserInner::new(
                        2,
                        "2".to_string(),
                        false,
                        0,
                        0,
                        0,
                        0,
                        0,
                        0,
                        0,
                        0,
                    )),
                ),
            ]);

            let mut iterator = RecordBatchStream::new(
                MergeStream::<UserInner, InMemProvider>::new(vec![EStreamImpl::Buf(iter_1)])
                    .await
                    .unwrap(),
                3,
            );

            assert_eq!(
                iterator.next().await.unwrap().unwrap(),
                RecordBatch::try_new(
                    UserInner::arrow_schema(),
                    vec![
                        Arc::new(UInt64Array::from(vec![1, 2])),
                        Arc::new(StringArray::from(vec!["1".to_string(), "2".to_string()])),
                        Arc::new(BooleanArray::from(vec![true, false])),
                        Arc::new(Int8Array::from(vec![0, 0])),
                        Arc::new(Int16Array::from(vec![0, 0])),
                        Arc::new(Int32Array::from(vec![0, 0])),
                        Arc::new(Int64Array::from(vec![0, 0])),
                        Arc::new(UInt8Array::from(vec![0, 0])),
                        Arc::new(UInt16Array::from(vec![0, 0])),
                        Arc::new(UInt32Array::from(vec![0, 0])),
                        Arc::new(UInt64Array::from(vec![0, 0])),
                    ]
                )
                .unwrap()
            );
            assert!(iterator.next().await.is_none());
        });
    }
}
