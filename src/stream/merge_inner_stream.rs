use std::{
    cmp::Reverse,
    collections::BinaryHeap,
    pin::{pin, Pin},
    sync::Arc,
    task::{Context, Poll},
};

use executor::futures::{Stream, StreamExt};
use pin_project::pin_project;

use crate::{
    schema::Schema,
    stream::{EStreamImpl, StreamError},
    utils::CmpKeyItem,
};

#[pin_project]
pub struct MergeInnerStream<'stream, S>
where
    S: Schema,
{
    #[allow(clippy::type_complexity)]
    heap: BinaryHeap<Reverse<(CmpKeyItem<Arc<S::PrimaryKey>, Option<S>>, usize)>>,
    iters: Vec<EStreamImpl<'stream, S>>,
    item_buf: Option<(Arc<S::PrimaryKey>, Option<S>)>,
}

impl<'stream, S> MergeInnerStream<'stream, S>
where
    S: Schema,
{
    pub(crate) async fn new(
        mut iters: Vec<EStreamImpl<'stream, S>>,
    ) -> Result<Self, StreamError<S::PrimaryKey, S>> {
        let mut heap = BinaryHeap::new();

        for (i, iter) in iters.iter_mut().enumerate() {
            if let Some(result) = Pin::new(iter).next().await {
                let (key, value) = result?;

                heap.push(Reverse((CmpKeyItem { key, _value: value }, i)));
            }
        }
        let mut iterator = MergeInnerStream {
            iters,
            heap,
            item_buf: None,
        };

        {
            let mut iterator = pin!(&mut iterator);
            let _ = iterator.next().await;
        }

        Ok(iterator)
    }
}

impl<'stream, S> Stream for MergeInnerStream<'stream, S>
where
    S: Schema,
{
    type Item = Result<(Arc<S::PrimaryKey>, Option<S>), StreamError<S::PrimaryKey, S>>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.project();
        while let Some(Reverse((
            CmpKeyItem {
                key: item_key,
                _value: item_value,
            },
            idx,
        ))) = this.heap.pop()
        {
            match Pin::new(&mut this.iters[idx]).poll_next(cx) {
                Poll::Ready(Some(item)) => {
                    let (key, value) = item?;
                    this.heap
                        .push(Reverse((CmpKeyItem { key, _value: value }, idx)));

                    if let Some((buf_key, _)) = &this.item_buf {
                        if buf_key == &item_key {
                            continue;
                        }
                    }
                }
                Poll::Ready(None) => (),
                Poll::Pending => return Poll::Pending,
            };
            return Poll::Ready(this.item_buf.replace((item_key, item_value)).map(Ok));
        }
        Poll::Ready(this.item_buf.take().map(Ok))
    }
}
