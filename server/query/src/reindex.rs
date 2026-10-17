//! 为旧数据补建派生索引：谱系（父提交列表、跳表 fp_depth / fp_jump）、PathHistory、GitSha。
//!
//! 按代数从低到高处理全部提交，保证父提交先完成。谱系以提交对象为准：记录中的父提交或跳表与
//! 对象推导的结果不一致时（M1 的数据只记录了第一个父提交，或者之前按错误的父提交算过跳表）直接覆盖。
//! 可以重复执行，索引写入都是幂等的。
//!
//! 运行中的服务端会缓存提交记录：应在服务端启动之前运行，或者运行后重启服务端。

use scion_object::FileOp;
use scion_store::{BlobStore, ChangesetRecord, MetaStore, PathChange, fp_skip};

use crate::{Result, index_git_sha, load_changeset};

/// 补建的统计。
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ReindexStats {
    pub changesets: usize,
    /// 修正了父提交列表或跳表的提交数。
    pub lineage_repaired: usize,
    /// 写入的 PathHistory 条目数（含早已存在的）。
    pub paths: usize,
    /// 带 git sha 的提交数。
    pub git_shas: usize,
}

/// 读出仓库的全部提交记录，按代数从低到高。同一代数的记录超过一个窗口时自动扩大窗口。
pub async fn all_records(meta: &dyn MetaStore, repo: &str, window: usize) -> Result<Vec<ChangesetRecord>> {
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
        let cs = load_changeset(blobs, rec.id).await?;
        // 父提交已按代数顺序修复完毕，这里的计算基于正确的数据
        let (depth, jump) = fp_skip(meta, repo, cs.parents.first().copied()).await?;
        if rec.parents != cs.parents || (rec.fp_depth, rec.fp_jump) != (depth, jump) {
            meta.repair_lineage(repo, &rec.id, &cs.parents, depth, jump).await?;
            stats.lineage_repaired += 1;
        }
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
        "{repo}: {} changesets, {} lineage repaired, {} path entries, {} git shas",
        stats.changesets, stats.lineage_repaired, stats.paths, stats.git_shas
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
                lineage_repaired: 30,
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
        // 再执行一次：谱系已正确，不再写入
        let again = reindex(&s, &s, REPO).await.expect("again");
        assert_eq!(again.lineage_repaired, 0);
        assert_eq!(
            describe(&again, REPO),
            "r: 30 changesets, 0 lineage repaired, 30 path entries, 30 git shas"
        );
    }

    /// M1 升级上来的数据：记录里父提交为空（只在旧的 first_parent 列），跳表还可能已按"根提交"算错。
    /// reindex 以提交对象为准全部修正，修正后历史查询能走过这些提交。
    #[tokio::test]
    async fn repairs_missing_parents_and_wrong_skip_pointers() {
        let s = MemStore::new();
        let ids = legacy_history(&s, 20).await;
        for id in &ids {
            // 父提交丢失，深度被错误地算成 1（根）
            s.repair_lineage(REPO, id, &[], 1, None).await.expect("corrupt");
        }
        let tip = *ids.last().expect("tip");
        let broken = crate::log(&s, &s, REPO, &[tip], None, 100, 1000).await.expect("log");
        assert_eq!(broken.entries.len(), 1, "history stops at the first legacy commit");

        let stats = reindex(&s, &s, REPO).await.expect("reindex");
        // 根提交（无父提交、深度 1）本来就是对的
        assert_eq!(stats.lineage_repaired, 19);
        for (i, id) in ids.iter().enumerate() {
            let rec = s.get_changeset(REPO, id).await.expect("ok").expect("rec");
            let parents: Vec<_> = i.checked_sub(1).map(|p| ids[p]).into_iter().collect();
            assert_eq!((rec.parents, rec.fp_depth), (parents, i as u64 + 1), "commit {i}");
        }
        let log = crate::log(&s, &s, REPO, &[tip], None, 100, 1000).await.expect("log");
        assert_eq!(log.entries.len(), 20);
        assert_eq!(crate::is_ancestor(&s, REPO, ids[0], tip).await, Ok(true));
        assert_eq!(blame(&s, &s, REPO, tip, "f").await.expect("blame").lines, ids);
        assert_eq!(reindex(&s, &s, REPO).await.expect("again").lineage_repaired, 0);
    }

    /// 合并提交的记录只有第一个父提交（M1 只存 first_parent）：从对象补全全部父提交。
    #[tokio::test]
    async fn restores_all_parents_of_merge_commits() {
        let s = MemStore::new();
        let ids = legacy_history(&s, 3).await;
        let sig = Signature {
            name: "a".into(),
            email: "a@x".into(),
            time_ms: 0,
            tz_offset_minutes: 0,
        };
        let merge = Changeset::new(
            vec![ids[2], ids[0]],
            sig.clone(),
            sig,
            "merge",
            [9; 16],
            vec![],
            BTreeMap::new(),
        )
        .expect("cs");
        s.put(merge.id(), Bytes::from(merge.encode())).await.expect("cs");
        let root = s
            .get_changeset(REPO, &ids[2])
            .await
            .expect("ok")
            .expect("rec")
            .root_tree;
        let rec = ChangesetRecord {
            id: merge.id(),
            root_tree: root,
            parents: vec![ids[2]],
            generation: 4,
            fp_depth: 0,
            fp_jump: None,
        };
        s.put_changeset(REPO, &rec).await.expect("rec");
        reindex(&s, &s, REPO).await.expect("reindex");
        let rec = s.get_changeset(REPO, &merge.id()).await.expect("ok").expect("rec");
        assert_eq!((rec.parents, rec.fp_depth), (vec![ids[2], ids[0]], 4));
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
        assert_eq!(stats.lineage_repaired, 15);
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
