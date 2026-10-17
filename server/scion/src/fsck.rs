//! `scion admin fsck`：校验仓库数据的完整性（服务端设计文档 §11）。
//!
//! 对每个仓库检查：
//! - 每个 ref 指向的提交有记录；
//! - 每条提交记录的父提交有记录，提交对象存在、哈希正确、能解码，且父提交与记录一致；
//! - 从每个提交的根目录树出发的引用闭包：目录树存在、哈希正确、能解码；文件在仓库的对象索引里，
//!   索引中的大小与目录项一致，字节存在（`--verify` 时读出并校验哈希）；
//! - redact 过的文件只检查索引；已清除（purge）的允许字节不存在。
//!
//! 只读，不修改任何数据。

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use futures::StreamExt;
use scion_object::{Changeset, EntryMode, ObjectId, Tree};
use scion_store::{BlobStore, MetaStore};

/// 并发检查的文件数。
const CONCURRENCY: usize = 32;
/// 一次批量查询的对象数。
const BATCH: usize = 500;
/// 报告中最多列出的问题数（总数照常统计）。
const MAX_PROBLEMS: usize = 1_000;

#[derive(Debug, Clone, Default)]
pub struct FsckOptions {
    /// 只检查这些仓库；为空时检查全部。
    pub repos: Vec<String>,
    /// 读出每个文件并校验哈希（默认只确认存在）。
    pub verify: bool,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct FsckReport {
    pub repos: usize,
    pub refs: usize,
    pub changesets: usize,
    pub trees: usize,
    pub files: usize,
    pub redacted: usize,
    pub problem_count: usize,
    /// 最多 [`MAX_PROBLEMS`] 条。
    pub problems: Vec<String>,
}

impl FsckReport {
    fn problem(&mut self, repo: &str, msg: String) {
        self.problem_count += 1;
        if self.problems.len() < MAX_PROBLEMS {
            self.problems.push(format!("{repo}: {msg}"));
        }
    }

    pub fn ok(&self) -> bool {
        self.problem_count == 0
    }

    /// 命令行输出。
    pub fn lines(&self) -> Vec<String> {
        let mut out = vec![format!(
            "checked {} repositories: {} refs, {} changesets, {} trees, {} files ({} redacted)",
            self.repos, self.refs, self.changesets, self.trees, self.files, self.redacted
        )];
        out.extend(self.problems.iter().cloned());
        if self.problem_count > self.problems.len() {
            out.push(format!("... and {} more", self.problem_count - self.problems.len()));
        }
        out.push(if self.ok() {
            "no problems found".to_string()
        } else {
            format!("{} problem(s) found", self.problem_count)
        });
        out
    }
}

/// 读出并校验一个对象的字节。
async fn load(blobs: &dyn BlobStore, id: &ObjectId) -> Result<Vec<u8>, String> {
    match blobs.get(id).await {
        Ok(Some(b)) if id.verify(&b) => Ok(b.to_vec()),
        Ok(Some(_)) => Err(format!("{id} fails its hash check")),
        Ok(None) => Err(format!("{id} is missing from the blob store")),
        Err(e) => Err(format!("{id} could not be read: {e}")),
    }
}

pub async fn fsck(
    meta: &Arc<dyn MetaStore>,
    blobs: &Arc<dyn BlobStore>,
    opts: &FsckOptions,
) -> anyhow::Result<FsckReport> {
    let repos = if opts.repos.is_empty() {
        meta.list_repos().await?
    } else {
        opts.repos.clone()
    };
    let mut report = FsckReport::default();
    for repo in &repos {
        if !meta.repo_exists(repo).await? {
            report.problem(repo, "repository does not exist".into());
            continue;
        }
        report.repos += 1;
        check_repo(meta, blobs, repo, opts.verify, &mut report).await?;
    }
    Ok(report)
}

async fn check_repo(
    meta: &Arc<dyn MetaStore>,
    blobs: &Arc<dyn BlobStore>,
    repo: &str,
    verify: bool,
    report: &mut FsckReport,
) -> anyhow::Result<()> {
    let records = scion_query::all_records(meta.as_ref(), repo, 4096).await?;
    let known: HashSet<ObjectId> = records.iter().map(|r| r.id).collect();

    for (name, state) in meta.list_refs(repo).await? {
        report.refs += 1;
        if !known.contains(&state.target) {
            report.problem(
                repo,
                format!("ref {name:?} points to {} which has no record", state.target),
            );
        }
    }

    // 提交对象与根目录树（目录树按 id 去重）
    let mut trees: Vec<ObjectId> = Vec::new();
    let mut seen_trees: HashSet<ObjectId> = HashSet::new();
    let mut indexed: Vec<ObjectId> = Vec::new();
    for rec in &records {
        report.changesets += 1;
        for p in &rec.parents {
            if !known.contains(p) {
                report.problem(repo, format!("changeset {} has parent {p} with no record", rec.id));
            }
        }
        match load(blobs.as_ref(), &rec.id).await {
            Ok(bytes) => match Changeset::decode(&bytes) {
                Ok(cs) if cs.parents != rec.parents => report.problem(
                    repo,
                    format!("changeset {} lists different parents than its record", rec.id),
                ),
                Ok(_) => {}
                Err(e) => report.problem(repo, format!("changeset {} does not decode: {e}", rec.id)),
            },
            Err(e) => report.problem(repo, format!("changeset {e}")),
        }
        indexed.push(rec.id);
        if seen_trees.insert(rec.root_tree) {
            trees.push(rec.root_tree);
        }
    }

    // 目录树的引用闭包；文件按 id 去重，记下目录项中的大小
    let mut files: HashMap<ObjectId, u64> = HashMap::new();
    while let Some(id) = trees.pop() {
        report.trees += 1;
        indexed.push(id);
        let tree = match load(blobs.as_ref(), &id).await {
            Ok(bytes) => match Tree::decode(&bytes) {
                Ok(t) => t,
                Err(e) => {
                    report.problem(repo, format!("tree {id} does not decode: {e}"));
                    continue;
                }
            },
            Err(e) => {
                report.problem(repo, format!("tree {e}"));
                continue;
            }
        };
        for e in tree.entries() {
            match e.mode {
                EntryMode::Tree => {
                    if seen_trees.insert(e.id) {
                        trees.push(e.id);
                    }
                }
                _ => {
                    files.entry(e.id).or_insert(e.size);
                }
            }
        }
    }

    // 提交与目录树都应在仓库的对象索引里
    for chunk in indexed.chunks(BATCH) {
        for id in meta.missing_objects(repo, chunk).await? {
            report.problem(repo, format!("{id} is not in the repository's object index"));
        }
    }

    // 文件：索引、大小、redaction、字节
    let ids: Vec<ObjectId> = files.keys().copied().collect();
    let mut present: Vec<ObjectId> = Vec::new();
    let mut redacted: HashMap<ObjectId, bool> = HashMap::new();
    for chunk in ids.chunks(BATCH) {
        let sizes = meta.object_sizes(repo, chunk).await?;
        for id in chunk {
            match sizes.get(id) {
                None => report.problem(repo, format!("file {id} is not in the repository's object index")),
                Some(size) if *size != files[id] => report.problem(
                    repo,
                    format!(
                        "file {id} is {size} bytes in the index but {} in its tree entry",
                        files[id]
                    ),
                ),
                Some(_) => present.push(*id),
            }
        }
        for r in meta.get_redactions(repo, chunk).await? {
            if r.active {
                redacted.insert(r.id, r.purged_at_ms.is_some());
            }
        }
    }
    report.files += ids.len();
    report.redacted += redacted.len();
    let to_check: Vec<ObjectId> = present.into_iter().filter(|id| !redacted.contains_key(id)).collect();
    let results: Vec<Option<String>> = futures::stream::iter(to_check)
        .map(|id| {
            let blobs = blobs.clone();
            async move {
                if verify {
                    load(blobs.as_ref(), &id).await.err().map(|e| format!("file {e}"))
                } else {
                    match blobs.exists(&id).await {
                        Ok(true) => None,
                        Ok(false) => Some(format!("file {id} is missing from the blob store")),
                        Err(e) => Some(format!("file {id} could not be checked: {e}")),
                    }
                }
            }
        })
        .buffer_unordered(CONCURRENCY)
        .collect()
        .await;
    for msg in results.into_iter().flatten() {
        report.problem(repo, msg);
    }
    // redact 但还没清除的：字节应当还在（清除之前可以撤销）
    for (id, purged) in redacted {
        if !purged && !blobs.exists(&id).await? {
            report.problem(repo, format!("redacted file {id} lost its bytes before being purged"));
        }
    }
    Ok(())
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use bytes::Bytes;
    use scion_object::{FileChange, ObjectKind, Signature};
    use scion_store::{IndexedObject, MemStore, RefExpect};

    pub(crate) struct Fixture {
        pub store: Arc<MemStore>,
        pub meta: Arc<dyn MetaStore>,
        pub blobs: Arc<dyn BlobStore>,
        pub head: ObjectId,
    }

    fn sig() -> Signature {
        Signature {
            name: "Ada".into(),
            email: "ada@example.com".into(),
            time_ms: 1,
            tz_offset_minutes: 0,
        }
    }

    /// 仓库 `r`：两个提交，`main` 指向第二个；`docs/secret.txt` 在第一个提交里。
    pub(crate) async fn fixture() -> Fixture {
        let store = Arc::new(MemStore::new());
        store.create_repo("r").await.expect("repo");
        let writer = scion_land::ChangesetWriter::new(store.clone(), store.clone());
        let mut parent: Vec<ObjectId> = Vec::new();
        let mut head = None;
        for (n, files) in [
            vec![("README", "hello"), ("docs/secret.txt", "password=hunter2")],
            vec![("README", "hello again")],
        ]
        .into_iter()
        .enumerate()
        {
            let mut delta = Vec::new();
            for (path, content) in files {
                let id = ObjectId::of_blob(content.as_bytes());
                store
                    .put(id, Bytes::copy_from_slice(content.as_bytes()))
                    .await
                    .expect("put");
                store
                    .index_objects(
                        "r",
                        &[IndexedObject {
                            id,
                            size: content.len() as u64,
                        }],
                    )
                    .await
                    .expect("index");
                delta.push(FileChange::put(path, EntryMode::File, id, content.len() as u64));
            }
            let n = u8::try_from(n).expect("small");
            let cs = Changeset::new(
                parent.clone(),
                sig(),
                sig(),
                format!("c{n}"),
                [n; 16],
                delta,
                Default::default(),
            )
            .expect("changeset");
            let rec = writer.create("r", &cs).await.expect("create");
            parent = vec![rec.id];
            head = Some(rec.id);
        }
        let head = head.expect("head");
        store.cas_ref("r", "main", RefExpect::Absent, head).await.expect("ref");
        Fixture {
            meta: store.clone(),
            blobs: store.clone(),
            store,
            head,
        }
    }

    #[tokio::test]
    async fn clean_repository_has_no_problems() {
        let fx = fixture().await;
        for verify in [false, true] {
            let r = fsck(&fx.meta, &fx.blobs, &FsckOptions { repos: vec![], verify })
                .await
                .expect("fsck");
            assert!(r.ok(), "{:?}", r.problems);
            assert_eq!((r.repos, r.refs, r.changesets, r.files), (1, 1, 2, 3));
            assert!(r.trees >= 3, "two roots and docs/");
            assert_eq!(r.lines().last().map(String::as_str), Some("no problems found"));
        }
    }

    #[tokio::test]
    async fn reports_missing_bytes_dangling_refs_and_unknown_repos() {
        let fx = fixture().await;
        let readme = ObjectId::of_blob(b"hello");
        fx.store.delete(&readme).await.expect("delete");
        let bogus = ObjectId::hash(ObjectKind::Changeset, b"bogus");
        fx.store
            .cas_ref("r", "dangling", RefExpect::Absent, bogus)
            .await
            .expect("ref");
        let r = fsck(
            &fx.meta,
            &fx.blobs,
            &FsckOptions {
                repos: vec!["r".into(), "nope".into()],
                verify: false,
            },
        )
        .await
        .expect("fsck");
        assert!(!r.ok());
        let text = r.lines().join("\n");
        assert!(
            text.contains(&format!("file {readme} is missing from the blob store")),
            "{text}"
        );
        assert!(text.contains("ref \"dangling\" points to"), "{text}");
        assert!(text.contains("nope: repository does not exist"), "{text}");
        assert!(text.ends_with("3 problem(s) found"), "{text}");
        // --verify 读出字节
        let r = fsck(
            &fx.meta,
            &fx.blobs,
            &FsckOptions {
                repos: vec!["r".into()],
                verify: true,
            },
        )
        .await
        .expect("fsck");
        assert!(r.lines().join("\n").contains("is missing from the blob store"));
        // 提交对象丢失
        fx.store.delete(&fx.head).await.expect("delete");
        let r = fsck(&fx.meta, &fx.blobs, &FsckOptions::default()).await.expect("fsck");
        assert!(
            r.lines()
                .join("\n")
                .contains(&format!("changeset {} is missing", fx.head))
        );
    }

    #[tokio::test]
    async fn redacted_files_may_lose_bytes_only_after_purge() {
        let fx = fixture().await;
        let secret = ObjectId::of_blob(b"password=hunter2");
        let red = scion_api::redaction::Redactor::new(fx.meta.clone(), fx.blobs.clone());
        let (rid, _) = red
            .redact("r", &[secret], &[], "leak", "root", false, 1)
            .await
            .expect("redact");
        fx.store.delete(&secret).await.expect("lost");
        let r = fsck(&fx.meta, &fx.blobs, &FsckOptions::default()).await.expect("fsck");
        assert_eq!(r.redacted, 1);
        assert!(r.lines().join("\n").contains("lost its bytes before being purged"));
        red.purge(&rid, 0, 2).await.expect("purge");
        red.run_purges(2).await.expect("run");
        let r = fsck(&fx.meta, &fx.blobs, &FsckOptions::default()).await.expect("fsck");
        assert!(r.ok(), "{:?}", r.problems);
    }

    #[test]
    fn problem_list_is_capped() {
        let mut r = FsckReport::default();
        for i in 0..MAX_PROBLEMS + 5 {
            r.problem("r", format!("p{i}"));
        }
        let lines = r.lines();
        assert_eq!(r.problems.len(), MAX_PROBLEMS);
        assert!(lines.iter().any(|l| l == "... and 5 more"));
    }
}
