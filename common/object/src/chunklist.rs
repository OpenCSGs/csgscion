//! ChunkList（SPEC.md §7）：大文件的分块清单，仅服务端存储内部使用。

use crate::codec::{Reader, put_id, put_uvarint};
use crate::error::{Error, Result};
use crate::id::{ObjectId, ObjectKind};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Chunk {
    pub id: ObjectId,
    pub size: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChunkList {
    total_size: u64,
    chunks: Vec<Chunk>,
}

const KIND: u8 = ObjectKind::ChunkList as u8;

impl ChunkList {
    /// 由分块构造，`total_size` 自动求和。
    pub fn new(chunks: Vec<Chunk>) -> Result<Self> {
        let total_size = chunks
            .iter()
            .try_fold(0u64, |acc, c| acc.checked_add(c.size))
            .ok_or(Error::InvalidChunkList("total size overflows u64"))?;
        let list = Self { total_size, chunks };
        list.validate()?;
        Ok(list)
    }

    pub fn total_size(&self) -> u64 {
        self.total_size
    }

    pub fn chunks(&self) -> &[Chunk] {
        &self.chunks
    }

    fn validate(&self) -> Result<()> {
        if self.chunks.is_empty() {
            return Err(Error::InvalidChunkList("no chunks"));
        }
        let mut sum: u64 = 0;
        for c in &self.chunks {
            if c.id.kind() != ObjectKind::Blob {
                return Err(Error::IdKindMismatch("chunk"));
            }
            if c.size == 0 {
                return Err(Error::InvalidChunkList("empty chunk"));
            }
            sum = sum
                .checked_add(c.size)
                .ok_or(Error::InvalidChunkList("total size overflows u64"))?;
        }
        if sum != self.total_size {
            return Err(Error::InvalidChunkList("sizes do not add up to total_size"));
        }
        Ok(())
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut out = vec![KIND, crate::FORMAT_VERSION];
        put_uvarint(&mut out, self.total_size);
        put_uvarint(&mut out, self.chunks.len() as u64);
        for c in &self.chunks {
            put_id(&mut out, &c.id);
            put_uvarint(&mut out, c.size);
        }
        out
    }

    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let mut r = Reader::new(bytes);
        r.header(KIND)?;
        let total_size = r.uvarint()?;
        let n = r.count()?;
        let mut chunks = Vec::with_capacity(n);
        for _ in 0..n {
            let id = r.id()?;
            let size = r.uvarint()?;
            chunks.push(Chunk { id, size });
        }
        r.finish()?;
        let list = Self { total_size, chunks };
        list.validate()?;
        Ok(list)
    }

    pub fn id(&self) -> ObjectId {
        ObjectId::hash(ObjectKind::ChunkList, &self.encode())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn chunk(data: &[u8]) -> Chunk {
        Chunk {
            id: ObjectId::of_blob(data),
            size: data.len() as u64,
        }
    }

    #[test]
    fn new_sums_sizes_and_roundtrips() {
        let l = ChunkList::new(vec![chunk(b"abc"), chunk(b"de")]).expect("valid");
        assert_eq!(l.total_size(), 5);
        assert_eq!(l.chunks().len(), 2);
        assert_eq!(ChunkList::decode(&l.encode()), Ok(l.clone()));
        assert_eq!(l.id().kind(), ObjectKind::ChunkList);
    }

    #[test]
    fn new_empty_rejected() {
        assert_eq!(ChunkList::new(vec![]), Err(Error::InvalidChunkList("no chunks")));
    }

    #[test]
    fn new_zero_size_chunk_rejected() {
        assert_eq!(
            ChunkList::new(vec![chunk(b"a"), chunk(b"")]),
            Err(Error::InvalidChunkList("empty chunk"))
        );
    }

    #[test]
    fn new_non_blob_chunk_rejected() {
        let c = Chunk {
            id: ObjectId::hash(ObjectKind::Tree, b""),
            size: 1,
        };
        assert_eq!(ChunkList::new(vec![c]), Err(Error::IdKindMismatch("chunk")));
    }

    #[test]
    fn new_overflow_rejected() {
        let big = Chunk {
            id: ObjectId::of_blob(b"x"),
            size: u64::MAX,
        };
        assert_eq!(
            ChunkList::new(vec![big.clone(), big]),
            Err(Error::InvalidChunkList("total size overflows u64"))
        );
    }

    #[test]
    fn validate_overflow_rejected() {
        let big = Chunk {
            id: ObjectId::of_blob(b"x"),
            size: u64::MAX,
        };
        let l = ChunkList {
            total_size: 0,
            chunks: vec![big.clone(), big],
        };
        assert_eq!(l.validate(), Err(Error::InvalidChunkList("total size overflows u64")));
    }

    #[test]
    fn decode_total_mismatch_rejected() {
        let l = ChunkList::new(vec![chunk(b"abc")]).expect("valid");
        let mut bytes = l.encode();
        bytes[2] = 4; // total_size 3 → 4
        assert_eq!(
            ChunkList::decode(&bytes),
            Err(Error::InvalidChunkList("sizes do not add up to total_size"))
        );
    }

    #[test]
    fn decode_truncated_and_trailing_rejected() {
        let bytes = ChunkList::new(vec![chunk(b"abc")]).expect("valid").encode();
        for cut in 0..bytes.len() {
            assert!(ChunkList::decode(&bytes[..cut]).is_err(), "cut at {cut}");
        }
        let mut extra = bytes.clone();
        extra.push(0);
        assert_eq!(ChunkList::decode(&extra), Err(Error::TrailingBytes(1)));
    }
}
