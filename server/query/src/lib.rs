//! 历史查询：log、祖先判断、merge-base（服务端设计文档 §8 Query）。
//!
//! 只依赖 [`MetaStore`] 里的提交记录（父提交与代数）；log 需要提交说明时再从 [`BlobStore`] 读取提交对象。
//! 遍历按代数从高到低进行：祖先的代数一定比后代小，因此可以用代数剪枝，并保证分页时不会重复。

mod blame;

pub use blame::{BLAME_KIND, BLAME_VERSION, BlameResult, blame};

use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashMap, HashSet};

use scion_object::{Changeset, ObjectId};
use scion_store::{BlobStore, ChangesetRecord, MetaStore};
use thiserror::Error;

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum QueryError {
    /// 提交记录不存在（id 不属于该仓库）。
    #[error("changeset {0} not found")]
    NotFound(ObjectId),
    /// 提交对象缺失或无法解码。
    #[error("changeset {0} is missing or corrupt: {1}")]
    Corrupt(ObjectId, String),
    #[error(transparent)]
    Store(#[from] scion_store::Error),
    #[error("path {0:?} not found")]
    PathNotFound(String),
    #[error("{0:?} is a directory")]
    NotAFile(String),
    /// 目录树缺失或损坏，或读取目录树时存储出错。
    #[error(transparent)]
    Tree(#[from] scion_tree::TreeError),
}

pub type Result<T> = std::result::Result<T, QueryError>;

/// 一次查询内的提交记录缓存。
pub(crate) struct Graph<'a> {
    meta: &'a dyn MetaStore,
    repo: &'a str,
    records: HashMap<ObjectId, ChangesetRecord>,
}

impl<'a> Graph<'a> {
    pub(crate) fn new(meta: &'a dyn MetaStore, repo: &'a str) -> Self {
        Self {
            meta,
            repo,
            records: HashMap::new(),
        }
    }

    pub(crate) async fn get(&mut self, id: ObjectId) -> Result<&ChangesetRecord> {
        if !self.records.contains_key(&id) {
            let rec = self
                .meta
                .get_changeset(self.repo, &id)
                .await?
                .ok_or(QueryError::NotFound(id))?;
            self.records.insert(id, rec);
        }
        Ok(&self.records[&id])
    }
}

/// `ancestor` 是否是 `descendant` 的祖先（含相等）。
pub async fn is_ancestor(meta: &dyn MetaStore, repo: &str, ancestor: ObjectId, descendant: ObjectId) -> Result<bool> {
    let mut g = Graph::new(meta, repo);
    is_ancestor_in(&mut g, ancestor, descendant).await
}

async fn is_ancestor_in(g: &mut Graph<'_>, ancestor: ObjectId, descendant: ObjectId) -> Result<bool> {
    let target_gen = g.get(ancestor).await?.generation;
    let mut queue = vec![descendant];
    let mut seen = HashSet::new();
    while let Some(c) = queue.pop() {
        if c == ancestor {
            return Ok(true);
        }
        if !seen.insert(c) {
            continue;
        }
        let rec = g.get(c).await?.clone();
        // 代数不大于目标的提交不可能以目标为祖先（相等时只能是它自己，上面已判断）
        if rec.generation <= target_gen {
            continue;
        }
        queue.extend(rec.parents.iter().copied());
    }
    Ok(false)
}

const PARENT1: u8 = 1;
const PARENT2: u8 = 2;
const STALE: u8 = 4;
const RESULT: u8 = 8;

/// `a` 与 `b` 的最佳公共祖先（互不为祖先），按代数从高到低排序。没有公共祖先时为空。
pub async fn merge_bases(meta: &dyn MetaStore, repo: &str, a: ObjectId, b: ObjectId) -> Result<Vec<ObjectId>> {
    let mut g = Graph::new(meta, repo);
    if a == b {
        g.get(a).await?;
        return Ok(vec![a]);
    }
    let mut flags: HashMap<ObjectId, u8> = HashMap::new();
    let mut queue: BinaryHeap<(u64, ObjectId)> = BinaryHeap::new();
    for (id, f) in [(a, PARENT1), (b, PARENT2)] {
        let generation_of = g.get(id).await?.generation;
        *flags.entry(id).or_default() |= f;
        queue.push((generation_of, id));
    }
    let mut candidates = Vec::new();
    // 队列里还有未被标记为 STALE 的提交时继续
    while queue.iter().any(|(_, id)| flags[id] & STALE == 0) {
        let Some((_, c)) = queue.pop() else { break };
        let mut f = flags[&c] & (PARENT1 | PARENT2 | STALE);
        if f & (PARENT1 | PARENT2) == (PARENT1 | PARENT2) {
            if flags[&c] & RESULT == 0 {
                *flags.entry(c).or_default() |= RESULT;
                candidates.push(c);
            }
            f |= STALE;
        }
        let parents = g.get(c).await?.parents.clone();
        for p in parents {
            let pf = flags.entry(p).or_default();
            if *pf & f == f {
                continue;
            }
            *pf |= f;
            let generation_of = g.get(p).await?.generation;
            queue.push((generation_of, p));
        }
    }
    // 去掉是其它候选祖先的候选
    let mut bases = Vec::new();
    for (i, c) in candidates.iter().enumerate() {
        let mut redundant = false;
        for (j, other) in candidates.iter().enumerate() {
            if i != j && is_ancestor_in(&mut g, *c, *other).await? {
                redundant = true;
                break;
            }
        }
        if !redundant {
            bases.push(*c);
        }
    }
    let mut keyed = Vec::with_capacity(bases.len());
    for id in bases {
        keyed.push((Reverse(g.get(id).await?.generation), id));
    }
    keyed.sort();
    Ok(keyed.into_iter().map(|(_, id)| id).collect())
}

/// log 中的一条：提交记录与提交对象。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogEntry {
    pub record: ChangesetRecord,
    pub changeset: Changeset,
}

/// 一页 log。`next` 为下一页的起点（尚未展开的前沿提交）；为 None 表示没有更多。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogPage {
    pub entries: Vec<LogEntry>,
    pub next: Option<Vec<ObjectId>>,
}

/// 提交是否改动了 `path`（相对第一个父提交）：改动的路径等于它、在它下面，或是它的上级。
fn touches(cs: &Changeset, path: &str) -> bool {
    let under = |q: &str, dir: &str| q.len() > dir.len() && q.starts_with(dir) && q.as_bytes()[dir.len()] == b'/';
    cs.delta
        .iter()
        .any(|c| c.path == path || under(&c.path, path) || under(path, &c.path))
}

/// 读取并解码提交对象。
pub async fn load_changeset(blobs: &dyn BlobStore, id: ObjectId) -> Result<Changeset> {
    let bytes = blobs
        .get(&id)
        .await?
        .ok_or_else(|| QueryError::Corrupt(id, "object not found".into()))?;
    Changeset::decode(&bytes).map_err(|e| QueryError::Corrupt(id, e.to_string()))
}

/// 从 `start`（一个或多个提交）开始按代数从新到旧列出历史，覆盖所有父提交。
///
/// - `path` 非空时只返回改动了该路径的提交（相对第一个父提交，与 delta 的语义一致）；
/// - 每页最多 `limit` 条，最多检查 `max_scan` 个提交；扫描到上限时提前返回，`next` 仍可继续。
pub async fn log(
    meta: &dyn MetaStore,
    blobs: &dyn BlobStore,
    repo: &str,
    start: &[ObjectId],
    path: Option<&str>,
    limit: usize,
    max_scan: usize,
) -> Result<LogPage> {
    let mut g = Graph::new(meta, repo);
    let mut queue: BinaryHeap<(u64, ObjectId)> = BinaryHeap::new();
    let mut queued = HashSet::new();
    for id in start {
        if queued.insert(*id) {
            queue.push((g.get(*id).await?.generation, *id));
        }
    }
    let path = path.filter(|p| !p.is_empty());
    let mut entries = Vec::new();
    let mut scanned = 0;
    while entries.len() < limit.max(1) && scanned < max_scan.max(1) {
        let Some((_, id)) = queue.pop() else { break };
        scanned += 1;
        let record = g.get(id).await?.clone();
        for p in &record.parents {
            if queued.insert(*p) {
                queue.push((g.get(*p).await?.generation, *p));
            }
        }
        let changeset = load_changeset(blobs, id).await?;
        if path.is_none_or(|p| touches(&changeset, p)) {
            entries.push(LogEntry { record, changeset });
        }
    }
    let next = if queue.is_empty() {
        None
    } else {
        let mut frontier: Vec<_> = queue.into_sorted_vec().into_iter().rev().map(|(_, id)| id).collect();
        frontier.dedup();
        Some(frontier)
    };
    Ok(LogPage { entries, next })
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use scion_object::{EntryMode, FileChange, Signature};
    use scion_store::MemStore;
    use std::collections::BTreeMap;

    const REPO: &str = "r";

    /// 构造提交图的小工具：直接写入提交对象和记录（不建树，root_tree 用占位）。
    struct Builder {
        store: MemStore,
        n: u8,
    }

    impl Builder {
        async fn new() -> Self {
            let store = MemStore::new();
            store.create_repo(REPO).await.expect("repo");
            Self { store, n: 0 }
        }

        async fn commit(&mut self, parents: &[ObjectId], paths: &[&str]) -> ObjectId {
            self.n += 1;
            let sig = Signature {
                name: "a".into(),
                email: "a@x".into(),
                time_ms: 0,
                tz_offset_minutes: 0,
            };
            let delta = paths
                .iter()
                .map(|p| FileChange::put(*p, EntryMode::File, ObjectId::of_blob(b"x"), 1))
                .collect();
            let cs = Changeset::new(
                parents.to_vec(),
                sig.clone(),
                sig,
                format!("c{}", self.n),
                [self.n; 16],
                delta,
                BTreeMap::new(),
            )
            .expect("valid");
            let bytes = Bytes::from(cs.encode());
            let id = cs.id();
            self.store.put(id, bytes).await.expect("put");
            let mut generation = 0;
            for p in parents {
                generation = generation.max(
                    self.store
                        .get_changeset(REPO, p)
                        .await
                        .expect("ok")
                        .expect("parent")
                        .generation,
                );
            }
            let rec = ChangesetRecord {
                id,
                root_tree: ObjectId::hash(scion_object::ObjectKind::Tree, b""),
                parents: parents.to_vec(),
                generation: generation + 1,
            };
            self.store.put_changeset(REPO, &rec).await.expect("record");
            id
        }
    }

    fn ids(page: &LogPage) -> Vec<ObjectId> {
        page.entries.iter().map(|e| e.record.id).collect()
    }

    /// a ─ b ─ c ─────── m ─ n
    ///      \           /
    ///       d ─ e ────'
    async fn diamond() -> (Builder, [ObjectId; 7]) {
        let mut b = Builder::new().await;
        let a = b.commit(&[], &["root.txt"]).await;
        let bb = b.commit(&[a], &["src/b.rs"]).await;
        let c = b.commit(&[bb], &["src/c.rs"]).await;
        let d = b.commit(&[bb], &["docs/d.md"]).await;
        let e = b.commit(&[d], &["docs/e.md"]).await;
        let m = b.commit(&[c, e], &[]).await;
        let n = b.commit(&[m], &["src/n.rs"]).await;
        (b, [a, bb, c, d, e, m, n])
    }

    #[tokio::test]
    async fn is_ancestor_cases() {
        let (b, [a, bb, c, d, e, m, n]) = diamond().await;
        let s = &b.store;
        assert!(is_ancestor(s, REPO, a, n).await.expect("ok"));
        assert!(is_ancestor(s, REPO, e, m).await.expect("ok"), "via second parent");
        assert!(is_ancestor(s, REPO, n, n).await.expect("ok"), "reflexive");
        assert!(!is_ancestor(s, REPO, c, e).await.expect("ok"), "siblings");
        assert!(!is_ancestor(s, REPO, n, a).await.expect("ok"), "reversed");
        assert!(is_ancestor(s, REPO, bb, d).await.expect("ok"));
        let ghost = ObjectId::hash(scion_object::ObjectKind::Changeset, b"ghost");
        assert_eq!(is_ancestor(s, REPO, ghost, n).await, Err(QueryError::NotFound(ghost)));
    }

    #[tokio::test]
    async fn merge_bases_cases() {
        let (b, [a, bb, c, _d, e, m, n]) = diamond().await;
        let s = &b.store;
        assert_eq!(merge_bases(s, REPO, c, e).await.expect("ok"), vec![bb]);
        assert_eq!(
            merge_bases(s, REPO, n, e).await.expect("ok"),
            vec![e],
            "one is ancestor of the other"
        );
        assert_eq!(merge_bases(s, REPO, m, m).await.expect("ok"), vec![m]);
        assert_eq!(merge_bases(s, REPO, a, n).await.expect("ok"), vec![a]);
        let ghost = ObjectId::hash(scion_object::ObjectKind::Changeset, b"ghost");
        assert_eq!(merge_bases(s, REPO, ghost, n).await, Err(QueryError::NotFound(ghost)));
        assert_eq!(
            merge_bases(s, REPO, ghost, ghost).await,
            Err(QueryError::NotFound(ghost))
        );
    }

    #[tokio::test]
    async fn merge_bases_criss_cross_has_two_bases() {
        // x ─ p1 ─ q1          两个分支互相合并后再分叉：最佳公共祖先有两个
        //  \    ╳
        //   p2 ─ q2
        let mut b = Builder::new().await;
        let x = b.commit(&[], &["x"]).await;
        let p1 = b.commit(&[x], &["p1"]).await;
        let p2 = b.commit(&[x], &["p2"]).await;
        let q1 = b.commit(&[p1, p2], &["q1"]).await;
        let q2 = b.commit(&[p2, p1], &["q2"]).await;
        let mut bases = merge_bases(&b.store, REPO, q1, q2).await.expect("ok");
        bases.sort();
        let mut want = vec![p1, p2];
        want.sort();
        assert_eq!(bases, want);
        // 没有公共祖先
        let lonely = b.commit(&[], &["other"]).await;
        assert!(merge_bases(&b.store, REPO, lonely, q1).await.expect("ok").is_empty());
    }

    #[tokio::test]
    async fn log_orders_by_generation_and_covers_merges() {
        let (b, [a, bb, c, d, e, m, n]) = diamond().await;
        let page = log(&b.store, &b.store, REPO, &[n], None, 100, 1000).await.expect("log");
        let got = ids(&page);
        assert_eq!(got.len(), 7);
        assert_eq!(&got[..2], &[n, m]);
        assert_eq!(got[6], a);
        assert!(got.contains(&c) && got.contains(&d) && got.contains(&e) && got.contains(&bb));
        assert_eq!(page.next, None);
        assert_eq!(page.entries[0].changeset.message, "c7");
    }

    #[tokio::test]
    async fn log_paginates_without_duplicates() {
        let (b, all) = diamond().await;
        let mut start = vec![all[6]];
        let mut seen = Vec::new();
        loop {
            let page = log(&b.store, &b.store, REPO, &start, None, 2, 1000).await.expect("log");
            assert!(page.entries.len() <= 2);
            seen.extend(ids(&page));
            match page.next {
                Some(n) => start = n,
                None => break,
            }
        }
        let unique: HashSet<_> = seen.iter().collect();
        assert_eq!(seen.len(), 7);
        assert_eq!(unique.len(), 7, "no duplicates across pages");
    }

    #[tokio::test]
    async fn log_filters_by_path() {
        let (b, [a, bb, c, _d, _e, _m, n]) = diamond().await;
        let page = log(&b.store, &b.store, REPO, &[n], Some("src"), 100, 1000)
            .await
            .expect("log");
        assert_eq!(ids(&page), vec![n, c, bb]);
        let page = log(&b.store, &b.store, REPO, &[n], Some("root.txt"), 100, 1000)
            .await
            .expect("log");
        assert_eq!(ids(&page), vec![a]);
        let page = log(&b.store, &b.store, REPO, &[n], Some("src/c.rs/deeper"), 100, 1000)
            .await
            .expect("log");
        assert_eq!(ids(&page), vec![c], "a file that is a parent of the path counts");
        let page = log(&b.store, &b.store, REPO, &[n], Some(""), 1, 1000)
            .await
            .expect("empty path = no filter");
        assert_eq!(ids(&page), vec![n]);
    }

    #[tokio::test]
    async fn log_scan_budget_returns_partial_page() {
        let (b, [_a, _bb, _c, _d, _e, _m, n]) = diamond().await;
        let page = log(&b.store, &b.store, REPO, &[n], Some("root.txt"), 100, 3)
            .await
            .expect("log");
        assert!(page.entries.is_empty(), "root.txt changed only in the oldest commit");
        let next = page.next.expect("can continue");
        let page = log(&b.store, &b.store, REPO, &next, Some("root.txt"), 100, 1000)
            .await
            .expect("log");
        assert_eq!(page.entries.len(), 1);
    }

    #[tokio::test]
    async fn log_errors() {
        let mut b = Builder::new().await;
        let ghost = ObjectId::hash(scion_object::ObjectKind::Changeset, b"ghost");
        assert_eq!(
            log(&b.store, &b.store, REPO, &[ghost], None, 10, 10).await,
            Err(QueryError::NotFound(ghost))
        );
        // 记录存在但提交对象缺失
        let rec = ChangesetRecord {
            id: ghost,
            root_tree: ghost,
            parents: vec![],
            generation: 1,
        };
        b.store.put_changeset(REPO, &rec).await.expect("record");
        assert!(matches!(
            log(&b.store, &b.store, REPO, &[ghost], None, 10, 10).await,
            Err(QueryError::Corrupt(..))
        ));
        // 提交对象无法解码
        let junk = Bytes::from_static(b"not a changeset");
        let bad = ObjectId::hash(scion_object::ObjectKind::Changeset, &junk);
        b.store.put(bad, junk).await.expect("put");
        b.store
            .put_changeset(REPO, &ChangesetRecord { id: bad, ..rec })
            .await
            .expect("record");
        assert!(matches!(
            log(&b.store, &b.store, REPO, &[bad], None, 10, 10).await,
            Err(QueryError::Corrupt(..))
        ));
        // 仓库不存在
        let _ = b.commit(&[], &["a"]).await;
        assert!(matches!(
            log(&b.store, &b.store, "nope", &[bad], None, 10, 10).await,
            Err(QueryError::Store(_))
        ));
    }

    #[test]
    fn touches_rules() {
        let sig = Signature {
            name: "a".into(),
            email: "a@x".into(),
            time_ms: 0,
            tz_offset_minutes: 0,
        };
        let cs = Changeset::new(
            vec![],
            sig.clone(),
            sig,
            "m",
            [0; 16],
            vec![FileChange::delete("src/lib.rs")],
            BTreeMap::new(),
        )
        .expect("valid");
        assert!(touches(&cs, "src/lib.rs"));
        assert!(touches(&cs, "src"));
        assert!(!touches(&cs, "sr"), "prefix must end at a path boundary");
        assert!(!touches(&cs, "src/lib"));
        assert!(touches(&cs, "src/lib.rs/x"));
    }

    #[test]
    fn error_display() {
        assert!(QueryError::PathNotFound("p".into()).to_string().contains("not found"));
        assert!(QueryError::NotAFile("d".into()).to_string().contains("directory"));
        let id = ObjectId::hash(scion_object::ObjectKind::Changeset, b"x");
        assert!(QueryError::NotFound(id).to_string().contains("not found"));
        assert!(QueryError::Corrupt(id, "bad".into()).to_string().contains("corrupt"));
    }
}
