//! Blame：文件每一行最后是在哪个提交引入的。
//!
//! 沿第一父提交链往回，找出文件每个版本的引入提交；从最老的版本往新算，相邻版本做行级 diff，
//! 未变的行沿用上一版的归属，新增的行归到引入提交。每个版本的结果按 (引入提交, 路径) 存为派生数据，
//! 下次往回走到已有结果就停止，只计算增量。
//!
//! M2 只沿第一父提交链追溯：经由合并进来的改动归到合并提交上。

use std::collections::HashMap;

use async_trait::async_trait;
use scion_object::{EntryMode, ObjectId, Tree};
use scion_store::{BlobStore, MetaStore};
use scion_tree::{TreeError, TreeSource, lookup};

use crate::{Graph, QueryError, Result};

/// 派生数据种类与算法版本。
pub const BLAME_KIND: &str = "blame";
pub const BLAME_VERSION: u32 = 1;

/// 某个版本文件的逐行归属。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BlameResult {
    /// 文件内容。
    pub blob: ObjectId,
    /// 引入这个版本的提交。
    pub introduced_by: ObjectId,
    /// 第 i 行（从 0 开始）最后被改动的提交。
    pub lines: Vec<ObjectId>,
}

/// 从 `&dyn BlobStore` 读取目录树。
struct Trees<'a>(&'a dyn BlobStore);

#[async_trait]
impl TreeSource for Trees<'_> {
    async fn load_tree(&self, id: &ObjectId) -> scion_tree::Result<Tree> {
        let bytes = self
            .0
            .get(id)
            .await
            .map_err(|e| TreeError::Storage(e.to_string()))?
            .ok_or_else(|| TreeError::BadTree(*id, "not found".into()))?;
        Tree::decode(&bytes).map_err(|e| TreeError::BadTree(*id, e.to_string()))
    }
}

/// 文件在某个提交中的 blob；不存在或是目录时为 None。
async fn file_at(g: &mut Graph<'_>, trees: &Trees<'_>, cs: ObjectId, path: &str) -> Result<Option<ObjectId>> {
    let root = g.get(cs).await?.root_tree;
    Ok(lookup(trees, root, path)
        .await?
        .filter(|e| e.mode != EntryMode::Tree)
        .map(|e| e.id))
}

fn cache_key(introduced_by: &ObjectId, path: &str) -> Vec<u8> {
    let mut k = introduced_by.to_bytes().to_vec();
    k.extend_from_slice(path.as_bytes());
    k
}

fn put_varint(out: &mut Vec<u8>, mut v: u64) {
    while v >= 0x80 {
        out.push((v as u8) | 0x80);
        v >>= 7;
    }
    out.push(v as u8);
}

fn get_varint(b: &[u8], pos: &mut usize) -> Option<u64> {
    let mut v = 0u64;
    for shift in (0..64).step_by(7) {
        let byte = *b.get(*pos)?;
        *pos += 1;
        v |= u64::from(byte & 0x7f) << shift;
        if byte & 0x80 == 0 {
            return Some(v);
        }
    }
    None
}

/// 编码：提交个数、各提交 id、区间个数、各区间 (行数, 提交下标)。
fn encode(lines: &[ObjectId]) -> Vec<u8> {
    let mut commits: Vec<ObjectId> = Vec::new();
    let mut index: HashMap<ObjectId, u64> = HashMap::new();
    let mut ranges: Vec<(u64, u64)> = Vec::new();
    for id in lines {
        let i = *index.entry(*id).or_insert_with(|| {
            commits.push(*id);
            commits.len() as u64 - 1
        });
        match ranges.last_mut() {
            Some((n, last)) if *last == i => *n += 1,
            _ => ranges.push((1, i)),
        }
    }
    let mut out = Vec::new();
    put_varint(&mut out, commits.len() as u64);
    for c in &commits {
        out.extend_from_slice(&c.to_bytes());
    }
    put_varint(&mut out, ranges.len() as u64);
    for (n, i) in ranges {
        put_varint(&mut out, n);
        put_varint(&mut out, i);
    }
    out
}

fn decode(b: &[u8]) -> Option<Vec<ObjectId>> {
    let mut pos = 0;
    let n = usize::try_from(get_varint(b, &mut pos)?).ok()?;
    let mut commits = Vec::with_capacity(n.min(b.len()));
    for _ in 0..n {
        let raw = b.get(pos..pos + scion_object::ID_LEN)?;
        commits.push(ObjectId::from_bytes(raw).ok()?);
        pos += scion_object::ID_LEN;
    }
    let ranges = get_varint(b, &mut pos)?;
    let mut lines = Vec::new();
    for _ in 0..ranges {
        let count = get_varint(b, &mut pos)?;
        let idx = usize::try_from(get_varint(b, &mut pos)?).ok()?;
        let id = *commits.get(idx)?;
        lines.extend(std::iter::repeat_n(id, usize::try_from(count).ok()?));
    }
    (pos == b.len()).then_some(lines)
}

async fn content(blobs: &dyn BlobStore, id: &ObjectId) -> Result<Vec<u8>> {
    blobs
        .get(id)
        .await?
        .map(|b| b.to_vec())
        .ok_or_else(|| QueryError::Corrupt(*id, "file content missing".into()))
}

/// 由上一版的逐行归属和两个版本的内容，算出新版本的逐行归属。
fn carry(old: &[u8], old_lines: &[ObjectId], new: &[u8], introduced_by: ObjectId) -> Vec<ObjectId> {
    let diff = similar::TextDiff::from_lines(old, new);
    let mut out = Vec::new();
    for op in diff.ops() {
        match op.tag() {
            similar::DiffTag::Equal => out.extend_from_slice(&old_lines[op.old_range()]),
            similar::DiffTag::Delete => {}
            similar::DiffTag::Insert | similar::DiffTag::Replace => {
                out.extend(std::iter::repeat_n(introduced_by, op.new_range().len()))
            }
        }
    }
    out
}

fn line_count(data: &[u8]) -> usize {
    similar::TextDiff::from_lines(&[][..], data)
        .ops()
        .iter()
        .map(|op| op.new_range().len())
        .sum()
}

/// `path` 在提交 `cs` 中的逐行归属。
pub async fn blame(
    meta: &dyn MetaStore,
    blobs: &dyn BlobStore,
    repo: &str,
    cs: ObjectId,
    path: &str,
) -> Result<BlameResult> {
    let mut g = Graph::new(meta, repo);
    let trees = Trees(blobs);
    let root = g.get(cs).await?.root_tree;
    let entry = lookup(&trees, root, path)
        .await?
        .ok_or_else(|| QueryError::PathNotFound(path.to_owned()))?;
    if entry.mode == EntryMode::Tree {
        return Err(QueryError::NotAFile(path.to_owned()));
    }

    // 往回收集各版本 (引入提交, blob)，从新到旧；遇到已缓存的版本就停
    let mut versions: Vec<(ObjectId, ObjectId)> = Vec::new();
    let mut base: Option<Vec<ObjectId>> = None;
    let (mut cur, mut blob) = (cs, entry.id);
    loop {
        // 找到当前 blob 的引入提交：沿第一父提交链走到 blob 不同为止
        loop {
            let Some(parent) = g.get(cur).await?.first_parent() else {
                break;
            };
            if file_at(&mut g, &trees, parent, path).await? != Some(blob) {
                break;
            }
            cur = parent;
        }
        // 已缓存则从这里开始往新算；缓存损坏（无法解码）时忽略并重新计算
        let cached = meta
            .get_derived(repo, BLAME_KIND, BLAME_VERSION, &cache_key(&cur, path))
            .await?;
        if let Some(lines) = cached.as_deref().and_then(decode) {
            base = Some(lines);
            versions.push((cur, blob));
            break;
        }
        versions.push((cur, blob));
        let Some(parent) = g.get(cur).await?.first_parent() else {
            break;
        };
        match file_at(&mut g, &trees, parent, path).await? {
            Some(older) => {
                cur = parent;
                blob = older;
            }
            None => break,
        }
    }

    // 从最老的版本往新计算
    versions.reverse();
    let mut iter = versions.into_iter();
    let (first_cs, first_blob) = iter.next().ok_or_else(|| QueryError::PathNotFound(path.to_owned()))?;
    let mut prev = content(blobs, &first_blob).await?;
    let mut lines = match base {
        Some(l) => l,
        None => {
            let l = vec![first_cs; line_count(&prev)];
            meta.put_derived(
                repo,
                BLAME_KIND,
                BLAME_VERSION,
                &cache_key(&first_cs, path),
                &encode(&l),
            )
            .await?;
            l
        }
    };
    let mut last = (first_cs, first_blob);
    for (introduced_by, blob) in iter {
        let data = content(blobs, &blob).await?;
        lines = carry(&prev, &lines, &data, introduced_by);
        meta.put_derived(
            repo,
            BLAME_KIND,
            BLAME_VERSION,
            &cache_key(&introduced_by, path),
            &encode(&lines),
        )
        .await?;
        prev = data;
        last = (introduced_by, blob);
    }
    Ok(BlameResult {
        blob: last.1,
        introduced_by: last.0,
        lines,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use scion_object::{Changeset, FileChange, Signature};
    use scion_store::{ChangesetRecord, MemStore};
    use std::collections::BTreeMap;

    const REPO: &str = "r";

    struct Repo {
        store: MemStore,
        n: u8,
    }

    impl Repo {
        async fn new() -> Self {
            let store = MemStore::new();
            store.create_repo(REPO).await.expect("repo");
            Self { store, n: 0 }
        }

        /// 提交：用 scion-tree 真正建树，写入内容、目录树、提交对象与记录。
        async fn commit(&mut self, parents: &[ObjectId], files: &[(&str, Option<&str>)]) -> ObjectId {
            self.n += 1;
            let sig = Signature {
                name: "a".into(),
                email: "a@x".into(),
                time_ms: 0,
                tz_offset_minutes: 0,
            };
            let mut delta = Vec::new();
            for (path, content) in files {
                match content {
                    Some(c) => {
                        let id = ObjectId::of_blob(c.as_bytes());
                        self.store
                            .put(id, Bytes::copy_from_slice(c.as_bytes()))
                            .await
                            .expect("blob");
                        delta.push(FileChange::put(*path, EntryMode::File, id, c.len() as u64));
                    }
                    None => delta.push(FileChange::delete(*path)),
                }
            }
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
            let base = match parents.first() {
                Some(p) => Some(
                    self.store
                        .get_changeset(REPO, p)
                        .await
                        .expect("ok")
                        .expect("parent")
                        .root_tree,
                ),
                None => None,
            };
            let built = scion_tree::apply_delta(&Trees(&self.store), base, &cs.delta)
                .await
                .expect("tree");
            for (id, b) in &built.new_trees {
                self.store.put(*id, b.clone()).await.expect("tree");
            }
            let id = cs.id();
            self.store.put(id, Bytes::from(cs.encode())).await.expect("cs");
            let mut generation = 0;
            for p in parents {
                generation = generation.max(
                    self.store
                        .get_changeset(REPO, p)
                        .await
                        .expect("ok")
                        .expect("p")
                        .generation,
                );
            }
            let rec = ChangesetRecord {
                id,
                root_tree: built.root,
                parents: parents.to_vec(),
                generation: generation + 1,
            };
            self.store.put_changeset(REPO, &rec).await.expect("record");
            id
        }

        async fn blame(&self, cs: ObjectId, path: &str) -> Result<BlameResult> {
            blame(&self.store, &self.store, REPO, cs, path).await
        }
    }

    #[tokio::test]
    async fn attributes_each_line_to_the_commit_that_last_changed_it() {
        let mut r = Repo::new().await;
        let a = r.commit(&[], &[("f.txt", Some("one\ntwo\nthree\n"))]).await;
        let b = r.commit(&[a], &[("other", Some("x"))]).await; // 不改 f.txt
        let c = r.commit(&[b], &[("f.txt", Some("one\nTWO\nthree\nfour\n"))]).await;
        let d = r
            .commit(&[c], &[("f.txt", Some("zero\none\nTWO\nthree\nfour\n"))])
            .await;
        let res = r.blame(d, "f.txt").await.expect("blame");
        assert_eq!(res.lines, vec![d, a, c, a, c]);
        assert_eq!(res.introduced_by, d);
        // 在未改动该文件的提交上 blame：结果相同的版本
        let at_b = r.blame(b, "f.txt").await.expect("blame");
        assert_eq!(at_b.lines, vec![a, a, a]);
        assert_eq!(at_b.introduced_by, a);
    }

    #[tokio::test]
    async fn uses_cache_and_survives_corrupt_cache() {
        let mut r = Repo::new().await;
        let a = r.commit(&[], &[("f", Some("1\n2\n"))]).await;
        let b = r.commit(&[a], &[("f", Some("1\n2\n3\n"))]).await;
        let first = r.blame(b, "f").await.expect("blame");
        // 结果已缓存
        let cached = r
            .store
            .get_derived(REPO, BLAME_KIND, BLAME_VERSION, &cache_key(&b, "f"))
            .await
            .expect("ok");
        assert_eq!(cached.as_deref().and_then(decode), Some(first.lines.clone()));
        // 再加一个提交：只需从 b 的缓存往后算
        let c = r.commit(&[b], &[("f", Some("0\n1\n2\n3\n"))]).await;
        assert_eq!(r.blame(c, "f").await.expect("blame").lines, vec![c, a, a, b]);
        // 损坏的缓存被忽略，结果仍正确
        let mut r2 = Repo::new().await;
        let a2 = r2.commit(&[], &[("f", Some("x\n"))]).await;
        r2.store
            .put_derived(REPO, BLAME_KIND, BLAME_VERSION, &cache_key(&a2, "f"), b"garbage")
            .await
            .expect("put");
        assert_eq!(r2.blame(a2, "f").await.expect("blame").lines, vec![a2]);
    }

    #[tokio::test]
    async fn file_deleted_and_recreated_starts_fresh() {
        let mut r = Repo::new().await;
        let a = r.commit(&[], &[("f", Some("old\n"))]).await;
        let b = r.commit(&[a], &[("f", None), ("g", Some("g"))]).await;
        let c = r.commit(&[b], &[("f", Some("old\nnew\n"))]).await;
        assert_eq!(r.blame(c, "f").await.expect("blame").lines, vec![c, c]);
    }

    #[tokio::test]
    async fn merge_attributes_to_merge_commit_along_first_parent() {
        let mut r = Repo::new().await;
        let a = r.commit(&[], &[("f", Some("base\n"))]).await;
        let side = r.commit(&[a], &[("f", Some("base\nside\n"))]).await;
        let main = r.commit(&[a], &[("g", Some("g"))]).await;
        let m = r.commit(&[main, side], &[("f", Some("base\nside\n"))]).await;
        assert_eq!(r.blame(m, "f").await.expect("blame").lines, vec![a, m]);
    }

    #[tokio::test]
    async fn errors_and_edge_cases() {
        let mut r = Repo::new().await;
        let a = r.commit(&[], &[("dir/f", Some("")), ("noeol", Some("a\nb"))]).await;
        assert!(matches!(r.blame(a, "nope").await, Err(QueryError::PathNotFound(_))));
        assert!(matches!(r.blame(a, "dir").await, Err(QueryError::NotAFile(_))));
        assert_eq!(
            r.blame(a, "dir/f").await.expect("empty file").lines,
            Vec::<ObjectId>::new()
        );
        assert_eq!(r.blame(a, "noeol").await.expect("no trailing newline").lines.len(), 2);
        let ghost = ObjectId::hash(scion_object::ObjectKind::Changeset, b"ghost");
        assert!(matches!(r.blame(ghost, "f").await, Err(QueryError::NotFound(_))));
        // 内容缺失
        let empty = MemStore::new();
        let id = ObjectId::of_blob(b"missing");
        assert!(matches!(content(&empty, &id).await, Err(QueryError::Corrupt(..))));
    }

    #[test]
    fn encode_decode_roundtrip() {
        let (x, y) = (ObjectId::of_blob(b"x"), ObjectId::of_blob(b"y"));
        for lines in [vec![], vec![x], vec![x, x, y, y, y, x], vec![y; 300]] {
            assert_eq!(decode(&encode(&lines)), Some(lines.clone()));
        }
        assert_eq!(decode(b""), None);
        assert_eq!(decode(&[1, 0]), None, "truncated id");
        let mut extra = encode(&[x]);
        extra.push(0);
        assert_eq!(decode(&extra), None, "trailing bytes");
        let mut bad_idx = Vec::new();
        put_varint(&mut bad_idx, 0);
        put_varint(&mut bad_idx, 1);
        put_varint(&mut bad_idx, 1);
        put_varint(&mut bad_idx, 5);
        assert_eq!(decode(&bad_idx), None, "index out of range");
        assert_eq!(get_varint(&[0x80; 11], &mut 0), None, "overlong varint");
    }

    #[test]
    fn carry_and_line_count() {
        let (o, n) = (ObjectId::of_blob(b"o"), ObjectId::of_blob(b"n"));
        assert_eq!(carry(b"a\nb\n", &[o, o], b"a\nB\nc\n", n), vec![o, n, n]);
        assert_eq!(carry(b"a\nb\n", &[o, o], b"b\n", n), vec![o]);
        assert_eq!(line_count(b""), 0);
        assert_eq!(line_count(b"a\nb"), 2);
    }
}
