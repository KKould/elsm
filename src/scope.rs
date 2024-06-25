use executor::futures::{
    util::{AsyncReadExt, AsyncWriteExt},
    AsyncRead, AsyncWrite,
};
use snowflake::ProcessUniqueId;

use crate::serdes::{Decode, Encode};

#[derive(Debug, Eq, PartialEq)]
pub struct Scope<K>
where
    K: Encode + Decode + Ord + Clone,
{
    pub(crate) min: K,
    pub(crate) max: K,
    pub(crate) gen: ProcessUniqueId,
}

impl<K> Clone for Scope<K>
where
    K: Encode + Decode + Ord + Clone,
{
    fn clone(&self) -> Self {
        Scope {
            min: self.min.clone(),
            max: self.max.clone(),
            gen: self.gen,
        }
    }
}

impl<K> Scope<K>
where
    K: Encode + Decode + Ord + Clone,
{
    pub(crate) fn is_between(&self, key: &K) -> bool {
        self.min.le(key) && self.max.ge(key)
    }

    pub(crate) fn is_meet(&self, target: &Scope<K>) -> bool {
        (self.min.le(&target.min) && self.max.ge(&target.min))
            || (self.min.le(&target.max) && self.max.ge(&target.max))
            || (self.min.le(&target.min)) && self.max.ge(&target.max)
            || (self.min.ge(&target.min)) && self.max.le(&target.max)
    }
}

impl<K> Encode for Scope<K>
where
    K: Encode + Decode + Ord + Clone,
{
    type Error = <K as Encode>::Error;

    async fn encode<W: AsyncWrite + Unpin + Send + Sync>(
        &self,
        writer: &mut W,
    ) -> Result<(), Self::Error> {
        self.min.encode(writer).await?;
        self.max.encode(writer).await?;

        writer
            .write_all(&bincode::serialize(&self.gen).unwrap())
            .await?;
        Ok(())
    }

    fn size(&self) -> usize {
        // ProcessUniqueId: usize + u64
        self.min.size() + self.max.size() + 16
    }
}

impl<K> Decode for Scope<K>
where
    K: Encode + Decode + Ord + Clone,
{
    type Error = <K as Decode>::Error;

    async fn decode<R: AsyncRead + Unpin>(reader: &mut R) -> Result<Self, Self::Error> {
        let min = K::decode(reader).await?;
        let max = K::decode(reader).await?;

        let gen = {
            let mut slice = [0; 16];
            reader.read_exact(&mut slice).await?;
            bincode::deserialize(&slice).unwrap()
        };

        Ok(Scope { min, max, gen })
    }
}
