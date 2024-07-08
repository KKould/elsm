pub mod fs;
pub mod in_mem;

use std::{
    fmt::{Display, Formatter},
    future::Future,
    io,
};

use futures::Stream;
use tokio::io::{AsyncRead, AsyncSeek, AsyncWrite};

use crate::wal::FileId;

pub enum FileType {
    WAL,
    PARQUET,
}

pub trait FileProvider: Send + Sync + 'static {
    type File: AsyncRead + AsyncWrite + AsyncSeek + Unpin + Send + Sync + 'static;

    fn open(
        &self,
        fid: FileId,
        file_type: FileType,
    ) -> impl Future<Output = io::Result<Self::File>> + Send;

    // FIXME: async
    fn remove(&self, fid: FileId, file_type: FileType) -> io::Result<()>;

    fn wal_list(&self) -> io::Result<impl Stream<Item = io::Result<(Self::File, FileId)>>>;
}

impl Display for FileType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            FileType::WAL => write!(f, "wal"),
            FileType::PARQUET => write!(f, "parquet"),
        }
    }
}
