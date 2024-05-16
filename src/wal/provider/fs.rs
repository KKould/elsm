use async_stream::stream;
use executor::futures::Stream;
use regex::Regex;
use std::fs::OpenOptions;
use std::{
    fs, io,
    path::{Path, PathBuf},
};

use super::WalProvider;

pub struct Fs {
    path: PathBuf,
}

impl Fs {
    pub fn new(path: impl AsRef<Path>) -> io::Result<Self> {
        std::fs::create_dir_all(path.as_ref())?;
        Ok(Self {
            path: path.as_ref().to_owned(),
        })
    }
}

impl WalProvider for Fs {
    type File = executor::fs::File;

    async fn open(&self, fid: u32) -> io::Result<Self::File> {
        Ok(OpenOptions::new()
            .create(true)
            .write(true)
            .read(true)
            .open(self.path.join(format!("{}.wal", fid)))?
            .into())
    }

    fn list(&self) -> impl Stream<Item = io::Result<Self::File>> {
        stream! {
            let re = Regex::new(r"^\d+\.wal$").unwrap();

            for entry in fs::read_dir(&self.path)? {
                let entry = entry?;
                let path = entry.path();
                if path.is_file() {
                    if let Some(filename) = path.file_name().and_then(|s| s.to_str()) {
                        if re.is_match(filename) {
                            yield Ok(OpenOptions::new()
                                .create(true)
                                .write(true)
                                .read(true)
                                .open(self.path.join(filename))?.into())
                        }
                    }
                }
            }
        }
    }
}
