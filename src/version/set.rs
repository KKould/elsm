use std::{fs::OpenOptions, io::SeekFrom, sync::Arc};

use async_lock::RwLock;
use futures::{channel::mpsc::Sender, SinkExt};
use tokio::{
    fs,
    io::{AsyncSeekExt, AsyncWriteExt},
};

use crate::{
    schema::Schema,
    serdes::Encode,
    version::{cleaner::CleanTag, edit::VersionEdit, Version, VersionError, VersionRef},
    wal::{provider::FileProvider, FileId, FileManager},
    DbOption,
};

pub(crate) struct VersionSetInner<S, FP>
where
    S: Schema,
    FP: FileProvider,
{
    current: VersionRef<S, FP>,
    log: fs::File,
}

pub(crate) struct VersionSet<S, FP>
where
    S: Schema,
    FP: FileProvider,
{
    inner: Arc<RwLock<VersionSetInner<S, FP>>>,
    clean_sender: Sender<CleanTag>,
    file_manager: Arc<FileManager<FP>>,
}

impl<S, FP> Clone for VersionSet<S, FP>
where
    S: Schema,
    FP: FileProvider,
{
    fn clone(&self) -> Self {
        VersionSet {
            inner: self.inner.clone(),
            clean_sender: self.clean_sender.clone(),
            file_manager: self.file_manager.clone(),
        }
    }
}

impl<S, FP> VersionSet<S, FP>
where
    S: Schema,
    FP: FileProvider,
{
    pub(crate) async fn new(
        option: &DbOption,
        clean_sender: Sender<CleanTag>,
        file_manager: Arc<FileManager<FP>>,
    ) -> Result<Self, VersionError<S>> {
        let mut log = fs::File::from(
            OpenOptions::new()
                .create(true)
                .write(true)
                .read(true)
                .open(option.version_path())
                .map_err(VersionError::Io)?,
        );
        let edits = VersionEdit::recover(&mut log).await;
        log.seek(SeekFrom::End(0)).await.map_err(VersionError::Io)?;

        let set = VersionSet::<S, FP> {
            inner: Arc::new(RwLock::new(VersionSetInner {
                current: Arc::new(Version {
                    num: 0,
                    level_slice: Version::<S, FP>::level_slice_new(),
                    clean_sender: clean_sender.clone(),
                    file_manager: file_manager.clone(),
                }),
                log,
            })),
            clean_sender,
            file_manager,
        };
        set.apply_edits(edits, None, true).await?;

        Ok(set)
    }

    pub(crate) async fn current(&self) -> VersionRef<S, FP> {
        self.inner.read().await.current.clone()
    }

    pub(crate) async fn apply_edits(
        &self,
        version_edits: Vec<VersionEdit<S::PrimaryKey>>,
        delete_gens: Option<Vec<FileId>>,
        is_recover: bool,
    ) -> Result<(), VersionError<S>> {
        let mut guard = self.inner.write().await;

        let mut new_version = Version::clone(&guard.current);

        for version_edit in version_edits {
            if !is_recover {
                version_edit
                    .encode(&mut guard.log)
                    .await
                    .map_err(VersionError::Encode)?;
            }
            match version_edit {
                VersionEdit::Add { mut scope, level } => {
                    if let Some(wal_ids) = scope.wal_ids.take() {
                        for wal_id in wal_ids {
                            self.file_manager.remove_wal_file(wal_id).unwrap();
                        }
                    }
                    new_version.level_slice[level as usize].push(scope);
                }
                VersionEdit::Remove { gen, level } => {
                    if let Some(i) = new_version.level_slice[level as usize]
                        .iter()
                        .position(|scope| scope.gen == gen)
                    {
                        new_version.level_slice[level as usize].remove(i);
                    }
                }
            }
        }
        if let Some(delete_gens) = delete_gens {
            new_version
                .clean_sender
                .send(CleanTag::Add {
                    version_num: new_version.num,
                    gens: delete_gens,
                })
                .await
                .map_err(VersionError::Send)?;
        }
        guard.log.flush().await.map_err(VersionError::Io)?;
        guard.current = Arc::new(new_version);
        Ok(())
    }
}
