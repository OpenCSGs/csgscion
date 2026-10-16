//! 为旧数据补建派生索引：跳表（fp_depth / fp_jump）、PathHistory、GitSha。
//!
//! 按代数从低到高处理全部提交，保证父提交先完成。可以重复执行：已有的跳表值不会被覆盖，
//! 索引写入都是幂等的。

use scion_object::FileOp;
use scion_store::{BlobStore, ChangesetRecord, MetaStore, PathChange, fp_skip};

use crate::{Result, index_git_sha, load_changeset};

/// 补建的统计。
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ReindexStats {
    pub changesets: usize,
    /// 补上跳表字段的提交数。
    pub skip_filled: usize,
    /// 写入的 PathHistory 条目数（含早已存在的）。
    pub paths: usize,
    /// 带 git sha 的提交数。
    pub git_shas: usize,
}

/// 读出仓库的全部提交记录，按代数从低到高。同一代数的记录超过一个窗口时自动扩大窗口。
async fn all_records(meta: &dyn MetaStore, repo: &str, window: usize) -> Result<Vec<ChangesetRecord>> {
    let mut window = window.max(1);
    let mut max_generation = u64::MAX >> 1;
    let mut out: Vec<ChangesetRecord> = Vec::new();
    loop {
        let recs = meta.changesets_below(repo, max_generation, window).await?;
        let (Some(lowest), Some(highest)) = (
            recs.iter().map(|r| r.generation).min(),
            recs.iter().map(|r| r.generation).max(),
        ) else {
            break;
        };
        if recs.len() == window && lowest == highest {
            // 整个窗口都是同一代数，无法确定是否读全：扩大窗口重读
            window *= 2;
            continue;
        }
        let full = recs.len() == window;
        // 窗口满时，最低代数可能没读全：留到下一轮
        out.extend(recs.into_iter().filter(|r| !full || r.generation > lowest));
        if !full {
            break;
        }
        max_generation = lowest;
    }
    out.sort_by_key(|r| r.generation);
    Ok(out)
}

/// 补建一个仓库的派生索引。
pub async fn reindex(meta: &dyn MetaStore, blobs: &dyn BlobStore, repo: &str) -> Result<ReindexStats> {
    reindex_with_window(meta, blobs, repo, 4096).await
}

pub(crate) async fn reindex_with_window(
    meta: &dyn MetaStore,
    blobs: &dyn BlobStore,
    repo: &str,
    window: usize,
) -> Result<ReindexStats> {
    let mut stats = ReindexStats::default();
    for rec in all_records(meta, repo, window).await? {
        stats.changesets += 1;
        if rec.fp_depth == 0 {
            let (depth, jump) = fp_skip(meta, repo, rec.first_parent()).await?;
            if depth > 0 {
                meta.set_fp_skip(repo, &rec.id, depth, jump).await?;
                stats.skip_filled += 1;
            }
        }
        let cs = load_changeset(blobs, rec.id).await?;
        let changes: Vec<PathChange> = cs
            .delta
            .iter()
            .map(|c| PathChange {
                path: c.path.clone(),
                deleted: matches!(c.op, FileOp::Delete),
            })
            .collect();
        meta.index_paths(repo, rec.id, rec.generation, &changes).await?;
        stats.paths += changes.len();
        if crate::git_sha_of(&cs).is_some() {
            index_git_sha(meta, repo, rec.id, &cs).await?;
            stats.git_shas += 1;
        }
    }
    Ok(stats)
}

/// 一行文字摘要（供命令行和日志使用）。
pub fn describe(stats: &ReindexStats, repo: &str) -> String {
    format!(
        "{repo}: {} changesets, {} skip pointers filled, {} path entries, {} git shas",
        stats.changesets, stats.skip_filled, stats.paths, stats.git_shas
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{GIT_SHA1_EXTENSION, blame, lookup_git_sha};
    use bytes::Bytes;
    use scion_object::{Changeset, EntryMode, FileChange, ObjectId, Signature, Tree};
    use scion_store::MemStore;
    use std::collections::BTreeMap;

    const REPO: &str = "r";

    /// 模拟索引建立之前的旧数据：只写对象和记录（深度 0），不写任何索引。
    async fn legacy_history(s: &MemStore, n: u8) -> Vec<ObjectId> {
        s.create_repo(REPO).await.expect("repo");
        let sig = Signature {
            name: "a".into(),
            email: "a@x".into(),
            time_ms: 0,
            tz_offset_minutes: 0,
        };
        let mut ids: Vec<ObjectId> = Vec::new();
        let mut content = String::new();
        for i in 0..n {
            content.push_str(&format!("line {i}\n"));
            let blob = ObjectId::of_blob(content.as_bytes());
            s.put(blob, Bytes::from(content.clone())).await.expect("blob");
            let tree = Tree::new(vec![scion_object::TreeEntry {
                name: "f".into(),
                mode: EntryMode::File,
                id: blob,
                size: content.len() as u64,
            }])
            .expect("tree");
            s.put(tree.id(), Bytes::from(tree.encode())).await.expect("tree");
            let mut ext = BTreeMap::new();
            ext.insert(GIT_SHA1_EXTENSION.to_string(), vec![i; 20]);
            let parents: Vec<_> = ids.last().copied().into_iter().collect();
            let cs = Changeset::new(
                parents.clone(),
                sig.clone(),
                sig.clone(),
                "m",
                [i; 16],
                vec![FileChange::put("f", EntryMode::File, blob, content.len() as u64)],
                ext,
            )
            .expect("cs");
            s.put(cs.id(), Bytes::from(cs.encode())).await.expect("cs");
            let rec = ChangesetRecord {
                id: cs.id(),
                root_tree: tree.id(),
                parents,
                generation: u64::from(i) + 1,
                fp_depth: 0,
                fp_jump: None,
            };
            s.put_changeset(REPO, &rec).await.expect("rec");
            ids.push(cs.id());
        }
        ids
    }

    #[tokio::test]
    async fn backfills_skip_paths_and_git_shas_idempotently() {
        let s = MemStore::new();
        let ids = legacy_history(&s, 30).await;
        assert!(s.path_history(REPO, "f", 100, 100).await.expect("ok").is_empty());
        let stats = reindex(&s, &s, REPO).await.expect("reindex");
        assert_eq!(
            stats,
            ReindexStats {
                changesets: 30,
                skip_filled: 30,
                paths: 30,
                git_shas: 30
            }
        );
        let tip = *ids.last().expect("tip");
        let rec = s.get_changeset(REPO, &tip).await.expect("ok").expect("tip");
        assert_eq!(rec.fp_depth, 30);
        assert_eq!(s.path_history(REPO, "f", 100, 100).await.expect("ok").len(), 30);
        assert_eq!(lookup_git_sha(&s, REPO, &[7; 20]).await, Ok(Some(ids[7])));
        // blame 走索引路径并给出正确结果
        assert_eq!(blame(&s, &s, REPO, tip, "f").await.expect("blame").lines, ids);
        // 再执行一次：不重复补写跳表
        let again = reindex(&s, &s, REPO).await.expect("again");
        assert_eq!(again.skip_filled, 0);
        assert_eq!(
            describe(&again, REPO),
            "r: 30 changesets, 0 skip pointers filled, 30 path entries, 30 git shas"
        );
    }

    #[tokio::test]
    async fn small_windows_still_read_every_record() {
        let s = MemStore::new();
        legacy_history(&s, 10).await;
        // 同一代数的额外根提交，迫使窗口扩大
        let sig = Signature {
            name: "a".into(),
            email: "a@x".into(),
            time_ms: 0,
            tz_offset_minutes: 0,
        };
        for k in 0..5u8 {
            let cs = Changeset::new(
                vec![],
                sig.clone(),
                sig.clone(),
                format!("root {k}"),
                [100 + k; 16],
                vec![],
                BTreeMap::new(),
            )
            .expect("cs");
            s.put(cs.id(), Bytes::from(cs.encode())).await.expect("cs");
            let rec = ChangesetRecord {
                id: cs.id(),
                root_tree: Tree::default().id(),
                parents: vec![],
                generation: 1,
                fp_depth: 0,
                fp_jump: None,
            };
            s.put(Tree::default().id(), Bytes::from(Tree::default().encode()))
                .await
                .expect("tree");
            s.put_changeset(REPO, &rec).await.expect("rec");
        }
        let stats = reindex_with_window(&s, &s, REPO, 2).await.expect("reindex");
        assert_eq!(stats.changesets, 15);
        assert_eq!(stats.skip_filled, 15);
        assert_eq!(
            reindex_with_window(&s, &s, "r", 0)
                .await
                .expect("window clamped")
                .changesets,
            15
        );
    }

    #[tokio::test]
    async fn errors_propagate() {
        let s = MemStore::new();
        assert!(reindex(&s, &s, "nope").await.is_err());
        s.create_repo(REPO).await.expect("repo");
        let id = ObjectId::hash(scion_object::ObjectKind::Changeset, b"no object");
        let rec = ChangesetRecord {
            id,
            root_tree: id,
            parents: vec![],
            generation: 1,
            fp_depth: 0,
            fp_jump: None,
        };
        s.put_changeset(REPO, &rec).await.expect("rec");
        assert!(matches!(
            reindex(&s, &s, REPO).await,
            Err(crate::QueryError::Corrupt(..))
        ));
    }
}
