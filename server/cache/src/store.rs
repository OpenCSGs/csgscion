//! 对象的本地存储：foyer 混合缓存（内存层 + 可选的磁盘层）。
//!
//! 对象按内容寻址、不可变，缓存永不需要失效，只按容量淘汰。内存层淘汰的对象写入磁盘层
//! （`WriteOnEviction`），磁盘层命中时重新放回内存。

use std::io::{Read, Write};
use std::path::PathBuf;

use bytes::Bytes;
use foyer::{
    BlockEngineConfig, Code, DeviceBuilder, FsDeviceBuilder, HybridCache, HybridCacheBuilder, HybridCachePolicy,
};
use scion_object::{ID_LEN, ObjectId};

/// 缓存的 key：对象 id。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct CacheKey(pub ObjectId);

impl Code for CacheKey {
    fn encode(&self, writer: &mut impl Write) -> foyer::Result<()> {
        writer.write_all(&self.0.to_bytes()).map_err(foyer::Error::io_error)
    }

    fn decode(reader: &mut impl Read) -> foyer::Result<Self> {
        let mut buf = [0u8; ID_LEN];
        reader.read_exact(&mut buf).map_err(foyer::Error::io_error)?;
        ObjectId::from_bytes(&buf)
            .map(CacheKey)
            .map_err(|e| foyer::Error::io_error(std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string())))
    }

    fn estimated_size(&self) -> usize {
        ID_LEN
    }
}

/// 磁盘层配置。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DiskConfig {
    pub dir: PathBuf,
    pub capacity_bytes: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StorageConfig {
    /// 内存层容量（字节）。
    pub memory_bytes: usize,
    /// 磁盘层；`None` 只用内存。
    pub disk: Option<DiskConfig>,
    /// 超过这个大小的对象不缓存（照常回源并返回）。
    pub max_object_bytes: usize,
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            memory_bytes: 1 << 30,
            disk: None,
            max_object_bytes: 8 << 20,
        }
    }
}

/// 磁盘层的块大小，必须不小于最大的可缓存对象。
const BLOCK_BYTES: usize = 16 << 20;

pub struct ObjectCache {
    inner: HybridCache<CacheKey, Bytes>,
    max_object_bytes: usize,
}

impl ObjectCache {
    pub async fn open(cfg: &StorageConfig) -> Result<Self, String> {
        if cfg.max_object_bytes > BLOCK_BYTES {
            return Err(format!(
                "max cached object size {} exceeds the disk block size {BLOCK_BYTES}",
                cfg.max_object_bytes
            ));
        }
        let builder = HybridCacheBuilder::new()
            .with_name("scion-objects")
            .with_policy(HybridCachePolicy::WriteOnEviction)
            .with_flush_on_close(true)
            .memory(cfg.memory_bytes.max(1))
            .with_weighter(|_k: &CacheKey, v: &Bytes| v.len() + ID_LEN)
            .storage();
        let builder = match &cfg.disk {
            Some(d) => {
                std::fs::create_dir_all(&d.dir).map_err(|e| format!("create cache dir {}: {e}", d.dir.display()))?;
                let device = FsDeviceBuilder::new(&d.dir)
                    .with_capacity(d.capacity_bytes)
                    .build()
                    .map_err(|e| format!("open cache dir {}: {e}", d.dir.display()))?;
                builder.with_engine_config(BlockEngineConfig::new(device).with_block_size(BLOCK_BYTES))
            }
            None => builder,
        };
        let inner = builder.build().await.map_err(|e| format!("open object cache: {e}"))?;
        Ok(Self {
            inner,
            max_object_bytes: cfg.max_object_bytes,
        })
    }

    /// 读取；磁盘层出错时视为未命中（记录日志，回源兜底）。
    pub async fn get(&self, id: &ObjectId) -> Option<Bytes> {
        match self.inner.get(&CacheKey(*id)).await {
            Ok(e) => e.map(|e| e.value().clone()),
            Err(e) => {
                tracing::warn!(%id, error = %e, "object cache read failed");
                None
            }
        }
    }

    /// 写入。过大的对象不缓存，返回是否写入。
    pub fn insert(&self, id: ObjectId, data: Bytes) -> bool {
        if data.len() > self.max_object_bytes {
            return false;
        }
        self.inner.insert(CacheKey(id), data);
        true
    }

    /// 删除（内存层与磁盘层）。
    pub fn remove(&self, id: &ObjectId) {
        self.inner.remove(&CacheKey(*id));
    }

    /// 是否在内存层（不查磁盘）。
    pub fn in_memory(&self, id: &ObjectId) -> bool {
        self.inner.memory().contains(&CacheKey(*id))
    }

    /// 关闭：把内存层刷到磁盘层，便于重启后恢复。
    pub async fn close(&self) {
        if let Err(e) = self.inner.close().await {
            tracing::warn!(error = %e, "closing object cache failed");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn key_round_trips_and_rejects_garbage() {
        let k = CacheKey(ObjectId::of_blob(b"x"));
        let mut buf = Vec::new();
        k.encode(&mut buf).expect("encode");
        assert_eq!(buf.len(), k.estimated_size());
        assert_eq!(CacheKey::decode(&mut buf.as_slice()).expect("decode"), k);
        assert!(CacheKey::decode(&mut &buf[..3]).is_err(), "short read");
        let mut bad = buf.clone();
        bad[0] = 0xff;
        assert!(CacheKey::decode(&mut bad.as_slice()).is_err(), "bad algorithm byte");
        let mut sink = [0u8; 2];
        assert!(k.encode(&mut sink.as_mut_slice()).is_err(), "writer too small");
    }

    #[tokio::test]
    async fn memory_cache_stores_and_skips_large_objects() {
        let c = ObjectCache::open(&StorageConfig {
            memory_bytes: 1 << 20,
            disk: None,
            max_object_bytes: 8,
        })
        .await
        .expect("open");
        let small = Bytes::from_static(b"small");
        let id = ObjectId::of_blob(&small);
        assert_eq!(c.get(&id).await, None);
        assert!(c.insert(id, small.clone()));
        assert_eq!(c.get(&id).await, Some(small));
        assert!(c.in_memory(&id));
        let big = Bytes::from_static(b"far too large");
        let big_id = ObjectId::of_blob(&big);
        assert!(!c.insert(big_id, big));
        assert_eq!(c.get(&big_id).await, None);
        c.close().await;
    }

    #[tokio::test]
    async fn disk_tier_serves_objects_evicted_from_memory() {
        let dir = tempfile::tempdir().expect("tempdir");
        let cfg = StorageConfig {
            memory_bytes: 64 << 10,
            disk: Some(DiskConfig {
                dir: dir.path().join("cache"),
                capacity_bytes: 64 << 20,
            }),
            max_object_bytes: 8 << 20,
        };
        let c = ObjectCache::open(&cfg).await.expect("open");
        let mut ids = Vec::new();
        for i in 0..64u32 {
            let data = Bytes::from(vec![i as u8; 4096]);
            let id = ObjectId::of_blob(&data);
            c.insert(id, data);
            ids.push(id);
        }
        // 早写入的对象已被挤出内存层
        assert!(!c.in_memory(&ids[0]));
        // 磁盘层是异步写入的：等它落盘
        let mut found = false;
        for _ in 0..100 {
            if c.get(&ids[0]).await.is_some() {
                found = true;
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
        assert!(found, "evicted object served from disk");
        assert_eq!(c.get(&ids[0]).await, Some(Bytes::from(vec![0u8; 4096])));
        c.close().await;
    }

    #[tokio::test]
    async fn open_rejects_bad_config() {
        let too_big = StorageConfig {
            max_object_bytes: BLOCK_BYTES + 1,
            ..StorageConfig::default()
        };
        assert!(ObjectCache::open(&too_big).await.is_err());
        let file = tempfile::NamedTempFile::new().expect("file");
        let bad_dir = StorageConfig {
            disk: Some(DiskConfig {
                dir: file.path().join("sub"),
                capacity_bytes: 1 << 20,
            }),
            ..StorageConfig::default()
        };
        assert!(ObjectCache::open(&bad_dir).await.is_err());
    }
}
