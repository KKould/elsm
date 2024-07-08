use std::{
    io,
    io::SeekFrom,
    pin::{pin, Pin},
    sync::Arc,
    task::{Context, Poll},
};

use async_stream::stream;
use crossbeam_queue::SegQueue;
use futures::{
    io::{AsyncRead, AsyncSeek, AsyncWrite, Cursor},
    ready, Stream,
};
use ulid::Ulid;

use super::{FileProvider, FileType};
use crate::wal::FileId;

#[derive(Debug, Default, Clone)]
pub struct InMemProvider {
    wals: Arc<SegQueue<Vec<u8>>>,
}

impl InMemProvider {
    pub fn into_inner(self) -> Arc<SegQueue<Vec<u8>>> {
        self.wals
    }
}

impl FileProvider for InMemProvider {
    type File = Buf;

    async fn open(&self, _fid: FileId, _file_type: FileType) -> std::io::Result<Self::File> {
        Ok(Buf {
            buf: Some(Cursor::new(Vec::new())),
            wals: self.wals.clone(),
        })
    }

    fn remove(&self, _fid: FileId, _file_type: FileType) -> io::Result<()> {
        // FIXME
        Ok(())
    }

    fn wal_list(&self) -> io::Result<impl Stream<Item = io::Result<(Self::File, FileId)>>> {
        Ok(stream! {
            yield Ok((Buf {
                buf: Some(Cursor::new(Vec::new())),
                wals: self.wals.clone(),
            }, Ulid::new()))
        })
    }
}

pub struct Buf {
    buf: Option<Cursor<Vec<u8>>>,
    wals: Arc<SegQueue<Vec<u8>>>,
}

impl AsyncWrite for Buf {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        pin!(self.buf.as_mut().unwrap()).poll_write(cx, buf)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        pin!(self.buf.as_mut().unwrap()).poll_flush(cx)
    }

    fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        if let Err(e) = ready!(pin!(self.buf.as_mut().unwrap()).poll_close(cx)) {
            return Poll::Ready(Err(e));
        }
        let buf = self.buf.take().unwrap().into_inner();
        self.wals.push(buf);
        Poll::Ready(Ok(()))
    }
}

impl AsyncRead for Buf {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<io::Result<usize>> {
        pin!(self.buf.as_mut().unwrap()).poll_read(cx, buf)
    }
}

impl AsyncSeek for Buf {
    fn poll_seek(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        pos: SeekFrom,
    ) -> Poll<io::Result<u64>> {
        pin!(self.buf.as_mut().unwrap()).poll_seek(cx, pos)
    }
}
