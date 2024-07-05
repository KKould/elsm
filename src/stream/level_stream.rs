use std::{
    collections::VecDeque,
    pin::{pin, Pin},
    sync::Arc,
    task::{Context, Poll},
};

use futures::{Future, Stream};
use pin_project::pin_project;

use crate::{
    schema::Schema,
    stream::{table_stream::TableStream, StreamError},
    wal::{provider::FileProvider, FileId, FileManager},
};

type LevelStreamFuture<'stream, S, FP> = Pin<
    Box<
        dyn Future<
                Output = Result<
                    TableStream<'stream, S, FP>,
                    StreamError<<S as Schema>::PrimaryKey, S>,
                >,
            > + Send
            + 'stream,
    >,
>;

#[pin_project]
pub(crate) struct LevelStream<'stream, S, FP>
where
    S: Schema,
    FP: FileProvider,
{
    lower: Option<S::PrimaryKey>,
    upper: Option<S::PrimaryKey>,
    file_manager: Arc<FileManager<FP>>,
    gens: VecDeque<FileId>,
    stream: Option<TableStream<'stream, S, FP>>,
    future: Option<LevelStreamFuture<'stream, S, FP>>,
}

impl<'stream, S, FP> LevelStream<'stream, S, FP>
where
    S: Schema,
    FP: FileProvider,
{
    pub(crate) async fn new(
        file_manager: Arc<FileManager<FP>>,
        gens: Vec<FileId>,
        lower: Option<&S::PrimaryKey>,
        upper: Option<&S::PrimaryKey>,
    ) -> Result<Self, StreamError<S::PrimaryKey, S>> {
        let mut gens = VecDeque::from(gens);
        let mut stream = None;

        if let Some(gen) = gens.pop_front() {
            stream =
                Some(TableStream::<S, FP>::new(file_manager.clone(), gen, lower, upper).await?);
        }

        Ok(Self {
            lower: lower.cloned(),
            upper: upper.cloned(),
            file_manager,
            gens,
            stream,
            future: None,
        })
    }
}

impl<S, FP> Stream for LevelStream<'_, S, FP>
where
    S: Schema,
    FP: FileProvider,
{
    type Item = Result<(S::PrimaryKey, Option<S>), StreamError<S::PrimaryKey, S>>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if let Some(future) = self.future.as_mut() {
            return match future.as_mut().poll(cx) {
                Poll::Ready(Ok(stream)) => {
                    self.stream = Some(stream);
                    self.poll_next(cx)
                }
                Poll::Ready(Err(err)) => Poll::Ready(Some(Err(err))),
                Poll::Pending => {
                    self.future = None;
                    Poll::Pending
                }
            };
        }
        if let Some(stream) = &mut self.stream {
            return match Pin::new(stream).poll_next(cx) {
                Poll::Ready(None) => match self.gens.pop_front() {
                    None => Poll::Ready(None),
                    Some(gen) => {
                        let file_manager = self.file_manager.clone();
                        let min = self.lower.clone();
                        let max = self.upper.clone();

                        let mut future = Box::pin(async move {
                            TableStream::<S, FP>::new(file_manager, gen, min.as_ref(), max.as_ref())
                                .await
                        });

                        match future.as_mut().poll(cx) {
                            Poll::Ready(Ok(stream)) => {
                                self.stream = Some(stream);
                                self.poll_next(cx)
                            }
                            Poll::Ready(Err(err)) => Poll::Ready(Some(Err(err))),
                            Poll::Pending => {
                                self.future = Some(future);
                                Poll::Pending
                            }
                        }
                    }
                },
                poll => poll,
            };
        }
        Poll::Ready(None)
    }
}
