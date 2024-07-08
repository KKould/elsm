use std::{io, path::PathBuf};

use futures::{AsyncRead, AsyncWrite};
use parquet::file::properties::WriterProperties;

use crate::{
    oracle::Oracle,
    record::Record,
    schema,
    serdes::{Decode, Encode},
    wal::{provider::FileProvider, WriteError},
    Db, DbOption,
};

pub struct DbBuilder {
    option: DbOption,
}

impl DbBuilder {
    pub fn path(path: impl Into<PathBuf> + Send) -> Self {
        DbBuilder {
            option: DbOption::new(path),
        }
    }

    pub fn max_mem_table_size(mut self, max_mem_table_size: usize) -> Self {
        self.option.max_mem_table_size = max_mem_table_size;
        self
    }

    pub fn immutable_chunk_num(mut self, immutable_chunk_num: usize) -> Self {
        self.option.immutable_chunk_num = immutable_chunk_num;
        self
    }

    pub fn major_threshold_with_sst_size(mut self, major_threshold_with_sst_size: usize) -> Self {
        self.option.major_threshold_with_sst_size = major_threshold_with_sst_size;
        self
    }

    pub fn level_sst_magnification(mut self, level_sst_magnification: usize) -> Self {
        self.option.level_sst_magnification = level_sst_magnification;
        self
    }

    pub fn max_sst_file_size(mut self, max_sst_file_size: usize) -> Self {
        self.option.max_sst_file_size = max_sst_file_size;
        self
    }

    pub fn clean_channel_buffer(mut self, clean_channel_buffer: usize) -> Self {
        self.option.clean_channel_buffer = clean_channel_buffer;
        self
    }

    pub fn write_parquet_option(mut self, write_parquet_option: Option<WriterProperties>) -> Self {
        self.option.write_parquet_option = write_parquet_option;
        self
    }

    pub async fn build<S, O, FP>(
        self,
        oracle: O,
        file_provider: FP,
    ) -> Result<Db<S, O, FP>, WriteError<<Record<S::PrimaryKey, S> as Encode>::Error>>
    where
        S: schema::Schema,
        O: Oracle<S::PrimaryKey> + 'static,
        FP: FileProvider,
        FP::File: AsyncWrite + AsyncRead,
        io::Error: From<<S as Decode>::Error>,
    {
        Db::new(oracle, file_provider, self.option).await
    }
}
