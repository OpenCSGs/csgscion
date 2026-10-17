//! 创建提交（服务端设计文档 §6.1）。

use std::sync::Arc;

use bytes::Bytes;
use scion_merge::Rebased;
use scion_object::{Changeset, FileOp, ObjectId, ObjectKind};
use scion_store::{BlobStore, ChangesetRecord, IndexedObject, MetaStore, PathChange};
use scion_tree::{BlobTreeSource, BuildResult, apply_delta};

use crate::{LandError, Result};

/// 校验并创建提交：父提交必须已存在，引用的文件必须已上传，delta 必须能应用到第一个父提交上。
pub struct ChangesetWriter {
    meta: Arc<dyn MetaStore>,
    blobs: Arc<dyn BlobStore>,
}

impl ChangesetWriter {
    pub fn new(meta: Arc<dyn MetaStore>, blobs: Arc<dyn BlobStore>) -> Self {
        Self { meta, blobs }
    }

    pub fn meta(&self) -> &Arc<dyn MetaStore> {
        &self.meta
    }

    pub fn blobs(&self) -> &Arc<dyn BlobStore> {
        &self.blobs
    }

    /// 创建客户端提交的 changeset。
    pub async fn create(&self, repo: &str, cs: &Changeset) -> Result<ChangesetRecord> {
        self.create_with(repo, cs, None).await
    }

    /// 创建 rebase 产生的 changeset：先写入合并产生的 blob，再复用 rebase 时建好的目录树。
    pub async fn create_rebased(&self, repo: &str, cs: &Changeset, rebased: Rebased) -> Result<ChangesetRecord> {
        if !rebased.merged_blobs.is_empty() {
            self.store(repo, &rebased.merged_blobs).await?;
        }
        self.create_with(repo, cs, Some(rebased.built)).await
    }

    async fn create_with(&self, repo: &str, cs: &Changeset, built: Option<BuildResult>) -> Result<ChangesetRecord> {
        if !self.meta.repo_exists(repo).await? {
            return Err(LandError::RepoNotFound(repo.to_owned()));
        }

        let mut generation = 0u64;
        let mut base = None;
        let mut missing_parents = Vec::new();
        for (i, p) in cs.parents.iter().enumerate() {
            match self.meta.get_changeset(repo, p).await? {
                Some(rec) => {
                    generation = generation.max(rec.generation);
                    if i == 0 {
                        base = Some(rec.root_tree);
                    }
                }
                None => missing_parents.push(*p),
            }
        }
        if !missing_parents.is_empty() {
            return Err(LandError::ObjectsNotUploaded(missing_parents));
        }

        let puts: Vec<(&str, ObjectId, u64)> = cs
            .delta
            .iter()
            .filter_map(|c| match &c.op {
                FileOp::Put { id, size, .. } => Some((c.path.as_str(), *id, *size)),
                FileOp::Delete => None,
            })
            .collect();
        let ids: Vec<ObjectId> = puts.iter().map(|(_, id, _)| *id).collect();
        let sizes = self.meta.object_sizes(repo, &ids).await?;
        let mut missing: Vec<ObjectId> = ids.iter().filter(|id| !sizes.contains_key(id)).copied().collect();
        missing.dedup();
        if !missing.is_empty() {
            return Err(LandError::ObjectsNotUploaded(missing));
        }
        // 目录树里的 size 来自客户端声明，必须与实际上传的大小一致
        for (path, id, declared) in &puts {
            let actual = sizes[id];
            if actual != *declared {
                return Err(LandError::InvalidChangeset {
                    path: (*path).to_owned(),
                    message: format!("{path:?} declares {declared} bytes but {id} is {actual} bytes"),
                });
            }
        }

        let built = match built {
            Some(b) => b,
            None => apply_delta(&BlobTreeSource(self.blobs.clone()), base, &cs.delta).await?,
        };

        let cs_bytes = Bytes::from(cs.encode());
        let cs_id = ObjectId::hash(ObjectKind::Changeset, &cs_bytes);
        let mut objects = built.new_trees;
        objects.push((cs_id, cs_bytes));
        self.store(repo, &objects).await?;

        let (fp_depth, fp_jump) = scion_store::fp_skip(self.meta.as_ref(), repo, cs.first_parent().copied()).await?;
        let rec = ChangesetRecord {
            id: cs_id,
            root_tree: built.root,
            parents: cs.parents.clone(),
            generation: generation.saturating_add(1),
            fp_depth,
            fp_jump,
        };
        // 先写路径索引，再写记录：记录存在即表示索引完整
        let changes: Vec<PathChange> = cs
            .delta
            .iter()
            .map(|c| PathChange {
                path: c.path.clone(),
                deleted: matches!(c.op, FileOp::Delete),
            })
            .collect();
        self.meta.index_paths(repo, cs_id, rec.generation, &changes).await?;
        scion_query::index_git_sha(self.meta.as_ref(), repo, cs_id, cs).await?;
        self.meta.put_changeset(repo, &rec).await?;
        Ok(rec)
    }

    /// 写入对象字节并登记到仓库索引。顺序：先字节、后索引（索引里有的对象一定可读）。
    pub async fn store(&self, repo: &str, objects: &[(ObjectId, Bytes)]) -> Result<()> {
        for (id, bytes) in objects {
            self.blobs.put(*id, bytes.clone()).await?;
        }
        let index: Vec<_> = objects
            .iter()
            .map(|(id, b)| IndexedObject {
                id: *id,
                size: b.len() as u64,
            })
            .collect();
        Ok(self.meta.index_objects(repo, &index).await?)
    }
}
