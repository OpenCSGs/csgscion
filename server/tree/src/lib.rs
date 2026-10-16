//! 增量建树：把 changeset 的 delta 应用到父提交的根目录树上（服务端设计文档 §4.3）。
//!
//! 只重建被改动路径上的目录，其余子树原样复用，代价是 O(改动数 × 目录深度)，与仓库大小无关。
//! 目录是隐式的：写入文件会创建上级目录，目录被删空后自动消失。

use std::collections::BTreeMap;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use scion_object::{EntryMode, FileChange, FileOp, ObjectId, Tree, TreeEntry};
use scion_store::BlobStore;
use thiserror::Error;

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum TreeError {
    /// 删除不存在的文件。
    #[error("cannot delete {0:?}: no such file")]
    DeleteMissing(String),
    /// 删除的路径是目录。目录是隐式的，需要逐个删除其中的文件。
    #[error("cannot delete {0:?}: it is a directory")]
    DeleteDirectory(String),
    /// 路径冲突：在文件下面写入、用文件覆盖目录，或同一 delta 同时写入 `a` 和 `a/b`。
    #[error("path conflict at {0:?}")]
    PathConflict(String),
    /// 目录树对象缺失或损坏。
    #[error("tree {0} is missing or corrupt: {1}")]
    BadTree(ObjectId, String),
    /// 读取存储失败（可重试）。
    #[error("storage error: {0}")]
    Storage(String),
}

pub type Result<T> = std::result::Result<T, TreeError>;

/// 读取目录树对象的来源。
#[async_trait]
pub trait TreeSource: Send + Sync {
    async fn load_tree(&self, id: &ObjectId) -> Result<Tree>;
}

/// 从 [`BlobStore`] 读取并解码目录树。
pub struct BlobTreeSource(pub Arc<dyn BlobStore>);

#[async_trait]
impl TreeSource for BlobTreeSource {
    async fn load_tree(&self, id: &ObjectId) -> Result<Tree> {
        let bytes = self
            .0
            .get(id)
            .await
            .map_err(|e| TreeError::Storage(e.to_string()))?
            .ok_or_else(|| TreeError::BadTree(*id, "not found".into()))?;
        Tree::decode(&bytes).map_err(|e| TreeError::BadTree(*id, e.to_string()))
    }
}

/// 建树结果。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BuildResult {
    pub root: ObjectId,
    /// 新产生的目录树对象（id 与规范编码），调用方需写入存储并登记到仓库索引。
    pub new_trees: Vec<(ObjectId, Bytes)>,
}

/// 把 `delta` 应用到 `base`（`None` 表示空仓库）上，返回新的根目录树。
///
/// `delta` 中的路径必须已通过 [`scion_object::validate_path`] 校验且互不重复（[`scion_object::Changeset`] 保证这一点）。
pub async fn apply_delta(src: &dyn TreeSource, base: Option<ObjectId>, delta: &[FileChange]) -> Result<BuildResult> {
    let changes: Vec<(&str, &FileOp)> = delta.iter().map(|c| (c.path.as_str(), &c.op)).collect();
    let mut new_trees = Vec::new();
    let root = match apply_dir(src, base, "", &changes, &mut new_trees).await? {
        Some(id) => id,
        None => emit(Tree::default(), &mut new_trees),
    };
    Ok(BuildResult { root, new_trees })
}

/// 把树编码并记入 `out`，返回其 id。
fn emit(tree: Tree, out: &mut Vec<(ObjectId, Bytes)>) -> ObjectId {
    let bytes = Bytes::from(tree.encode());
    let id = ObjectId::hash(scion_object::ObjectKind::Tree, &bytes);
    out.push((id, bytes));
    id
}

fn join(prefix: &str, name: &str) -> String {
    if prefix.is_empty() {
        name.to_owned()
    } else {
        format!("{prefix}/{name}")
    }
}

/// 一个子项上的改动：对它本身的操作（至多一个），以及对它下面路径的改动（相对路径）。
#[derive(Default)]
struct Group<'a> {
    own: Option<&'a FileOp>,
    below: Vec<(&'a str, &'a FileOp)>,
}

/// 递归处理一个目录。返回新目录树的 id；目录变空时返回 `None`。
fn apply_dir<'a>(
    src: &'a dyn TreeSource,
    base: Option<ObjectId>,
    prefix: &'a str,
    changes: &'a [(&'a str, &'a FileOp)],
    out: &'a mut Vec<(ObjectId, Bytes)>,
) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<Option<ObjectId>>> + Send + 'a>> {
    Box::pin(async move {
        // 没有改动的目录原样复用，不读取也不重新编码
        if changes.is_empty() {
            return Ok(base);
        }
        let mut entries: BTreeMap<String, TreeEntry> = match base {
            Some(id) => src
                .load_tree(&id)
                .await?
                .entries()
                .iter()
                .map(|e| (e.name.clone(), e.clone()))
                .collect(),
            None => BTreeMap::new(),
        };

        let mut groups: BTreeMap<&str, Group<'_>> = BTreeMap::new();
        for (path, op) in changes {
            match path.split_once('/') {
                None => groups.entry(path).or_default().own = Some(op),
                Some((first, rest)) => groups.entry(first).or_default().below.push((rest, op)),
            }
        }

        for (name, g) in groups {
            let path = join(prefix, name);
            let existing = entries.get(name).cloned();
            let existing_is_dir = existing.as_ref().is_some_and(|e| e.mode == EntryMode::Tree);
            match g.own {
                Some(FileOp::Put { mode, id, size }) => {
                    // 用文件替换目录：只有当同一个 delta 把该目录下的文件全部删掉时才允许
                    if existing_is_dir || !g.below.is_empty() {
                        let sub_base = existing.as_ref().filter(|e| e.mode == EntryMode::Tree).map(|e| e.id);
                        if apply_dir(src, sub_base, &path, &g.below, out).await?.is_some() {
                            return Err(TreeError::PathConflict(path));
                        }
                    }
                    let entry = TreeEntry {
                        name: name.to_owned(),
                        mode: *mode,
                        id: *id,
                        size: *size,
                    };
                    entries.insert(name.to_owned(), entry);
                    continue;
                }
                Some(FileOp::Delete) => match existing {
                    None => return Err(TreeError::DeleteMissing(path)),
                    Some(_) if existing_is_dir => return Err(TreeError::DeleteDirectory(path)),
                    Some(_) => {
                        entries.remove(name);
                    }
                },
                None => {}
            }
            if g.below.is_empty() {
                continue;
            }
            // 下面还有改动：此处必须是目录（或不存在、或刚被删除的文件，此时新建目录）
            let sub_base = match entries.get(name) {
                Some(e) if e.mode == EntryMode::Tree => Some(e.id),
                Some(_) => return Err(TreeError::PathConflict(path)),
                None => None,
            };
            match apply_dir(src, sub_base, &path, &g.below, out).await? {
                Some(id) => {
                    let entry = TreeEntry {
                        name: name.to_owned(),
                        mode: EntryMode::Tree,
                        id,
                        size: 0,
                    };
                    entries.insert(name.to_owned(), entry);
                }
                None => {
                    entries.remove(name);
                }
            }
        }

        if entries.is_empty() {
            return Ok(None);
        }
        let tree = Tree::new(entries.into_values().collect()).map_err(|e| {
            TreeError::BadTree(
                base.unwrap_or_else(|| ObjectId::hash(scion_object::ObjectKind::Tree, b"")),
                e.to_string(),
            )
        })?;
        Ok(Some(emit(tree, out)))
    })
}

/// 一个文件的变化：`old` 为 None 表示新增，`new` 为 None 表示删除。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileDiff {
    pub path: String,
    pub old: Option<TreeEntry>,
    pub new: Option<TreeEntry>,
}

/// 比较两个根目录树在 `path`（空串为整个仓库）下的文件差异，按路径排序。
/// `None` 表示空树。内容与 mode 都相同的子树直接跳过，代价只与变化的部分有关。
pub async fn diff(
    src: &dyn TreeSource,
    old_root: Option<ObjectId>,
    new_root: Option<ObjectId>,
    path: &str,
) -> Result<Vec<FileDiff>> {
    let at = |root: Option<ObjectId>| async move {
        match root {
            None => Ok(None),
            Some(r) if path.is_empty() => Ok(Some(TreeEntry {
                name: String::new(),
                mode: EntryMode::Tree,
                id: r,
                size: 0,
            })),
            Some(r) => lookup(src, r, path).await,
        }
    };
    let (old, new) = (at(old_root).await?, at(new_root).await?);
    let mut out = Vec::new();
    diff_entries(src, path, old, new, &mut out).await?;
    out.sort_by(|a, b| a.path.as_bytes().cmp(b.path.as_bytes()));
    Ok(out)
}

fn diff_entries<'a>(
    src: &'a dyn TreeSource,
    path: &'a str,
    old: Option<TreeEntry>,
    new: Option<TreeEntry>,
    out: &'a mut Vec<FileDiff>,
) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<()>> + Send + 'a>> {
    Box::pin(async move {
        if old.as_ref().map(|e| (e.mode, e.id)) == new.as_ref().map(|e| (e.mode, e.id)) {
            return Ok(());
        }
        let is_dir = |e: &Option<TreeEntry>| e.as_ref().is_some_and(|e| e.mode == EntryMode::Tree);
        let (old_dir, new_dir) = (is_dir(&old), is_dir(&new));
        if !old_dir && !new_dir {
            out.push(FileDiff {
                path: path.to_owned(),
                old,
                new,
            });
            return Ok(());
        }
        // 至少一侧是目录：文件那一侧单独记一条，目录那一侧逐个条目递归
        let (mut old_children, mut new_children) = (BTreeMap::new(), BTreeMap::new());
        match (&old, old_dir) {
            (Some(e), true) => old_children = children(src, e.id).await?,
            (Some(e), false) => out.push(FileDiff {
                path: path.to_owned(),
                old: Some(e.clone()),
                new: None,
            }),
            (None, _) => {}
        }
        match (&new, new_dir) {
            (Some(e), true) => new_children = children(src, e.id).await?,
            (Some(e), false) => out.push(FileDiff {
                path: path.to_owned(),
                old: None,
                new: Some(e.clone()),
            }),
            (None, _) => {}
        }
        let names: std::collections::BTreeSet<String> =
            old_children.keys().chain(new_children.keys()).cloned().collect();
        for name in names {
            let child = join(path, &name);
            diff_entries(src, &child, old_children.remove(&name), new_children.remove(&name), out).await?;
        }
        Ok(())
    })
}

async fn children(src: &dyn TreeSource, id: ObjectId) -> Result<BTreeMap<String, TreeEntry>> {
    Ok(src
        .load_tree(&id)
        .await?
        .entries()
        .iter()
        .map(|e| (e.name.clone(), e.clone()))
        .collect())
}

/// 按路径查找条目；`path` 为空字符串时表示根目录本身（返回 `None`，调用方直接使用根树）。
pub async fn lookup(src: &dyn TreeSource, root: ObjectId, path: &str) -> Result<Option<TreeEntry>> {
    if path.is_empty() {
        return Ok(None);
    }
    let mut dir = root;
    let mut parts = path.split('/').peekable();
    while let Some(name) = parts.next() {
        let tree = src.load_tree(&dir).await?;
        let Some(entry) = tree.get(name).cloned() else {
            return Ok(None);
        };
        if parts.peek().is_none() {
            return Ok(Some(entry));
        }
        if entry.mode != EntryMode::Tree {
            return Ok(None);
        }
        dir = entry.id;
    }
    Ok(None)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::sync::Mutex;

    /// 内存中的树来源，同时收集建树产生的新树。
    #[derive(Default)]
    struct MemSource {
        trees: Mutex<HashMap<ObjectId, Bytes>>,
    }

    impl MemSource {
        fn absorb(&self, r: &BuildResult) {
            let mut t = self.trees.lock().expect("lock");
            for (id, b) in &r.new_trees {
                t.insert(*id, b.clone());
            }
        }
    }

    #[async_trait]
    impl TreeSource for MemSource {
        async fn load_tree(&self, id: &ObjectId) -> Result<Tree> {
            let b = self.trees.lock().expect("lock").get(id).cloned();
            let b = b.ok_or_else(|| TreeError::BadTree(*id, "not found".into()))?;
            Tree::decode(&b).map_err(|e| TreeError::BadTree(*id, e.to_string()))
        }
    }

    fn put(path: &str, content: &str) -> FileChange {
        FileChange::put(
            path,
            EntryMode::File,
            ObjectId::of_blob(content.as_bytes()),
            content.len() as u64,
        )
    }

    fn del(path: &str) -> FileChange {
        FileChange::delete(path)
    }

    async fn build(src: &MemSource, base: Option<ObjectId>, delta: Vec<FileChange>) -> Result<ObjectId> {
        let mut delta = delta;
        delta.sort_by(|a, b| a.path.as_bytes().cmp(b.path.as_bytes()));
        let r = apply_delta(src, base, &delta).await?;
        src.absorb(&r);
        Ok(r.root)
    }

    /// 列出树中全部文件（路径 → blob id），用于和「全量重建」对照。
    async fn flatten(src: &MemSource, root: ObjectId) -> BTreeMap<String, ObjectId> {
        let mut out = BTreeMap::new();
        let mut stack = vec![(String::new(), root)];
        while let Some((prefix, id)) = stack.pop() {
            for e in src.load_tree(&id).await.expect("tree").entries() {
                let p = join(&prefix, &e.name);
                if e.mode == EntryMode::Tree {
                    stack.push((p, e.id));
                } else {
                    out.insert(p, e.id);
                }
            }
        }
        out
    }

    #[tokio::test]
    async fn empty_delta_on_empty_repo_gives_empty_tree() {
        let src = MemSource::default();
        let root = build(&src, None, vec![]).await.expect("build");
        assert_eq!(root, Tree::default().id());
    }

    #[tokio::test]
    async fn empty_delta_on_existing_tree_reuses_root() {
        let src = MemSource::default();
        let r1 = build(&src, None, vec![put("a/b", "b")]).await.expect("v1");
        let r = apply_delta(&src, Some(r1), &[]).await.expect("noop");
        assert_eq!(r.root, r1);
        assert!(r.new_trees.is_empty());
    }

    #[tokio::test]
    async fn creates_nested_directories() {
        let src = MemSource::default();
        let root = build(
            &src,
            None,
            vec![put("a/b/c.txt", "c"), put("a/d.txt", "d"), put("top", "t")],
        )
        .await
        .expect("build");
        let files = flatten(&src, root).await;
        assert_eq!(files.keys().collect::<Vec<_>>(), ["a/b/c.txt", "a/d.txt", "top"]);
        let e = lookup(&src, root, "a/b").await.expect("lookup").expect("dir");
        assert_eq!(e.mode, EntryMode::Tree);
    }

    #[tokio::test]
    async fn modify_reuses_untouched_subtrees() {
        let src = MemSource::default();
        let r1 = build(&src, None, vec![put("x/1", "1"), put("y/2", "2")])
            .await
            .expect("v1");
        let y1 = lookup(&src, r1, "y").await.expect("ok").expect("y").id;
        let delta = vec![put("x/1", "changed")];
        let r = apply_delta(&src, Some(r1), &delta).await.expect("v2");
        // 只有根和 x 两棵树是新的
        assert_eq!(r.new_trees.len(), 2);
        src.absorb(&r);
        assert_eq!(lookup(&src, r.root, "y").await.expect("ok").expect("y").id, y1);
    }

    #[tokio::test]
    async fn delete_removes_empty_dirs_up_to_root() {
        let src = MemSource::default();
        let r1 = build(&src, None, vec![put("a/b/c", "c")]).await.expect("v1");
        let r2 = build(&src, Some(r1), vec![del("a/b/c")]).await.expect("v2");
        assert_eq!(r2, Tree::default().id());
    }

    #[tokio::test]
    async fn file_replaced_by_directory_in_same_delta() {
        let src = MemSource::default();
        let r1 = build(&src, None, vec![put("a", "file")]).await.expect("v1");
        let r2 = build(&src, Some(r1), vec![del("a"), put("a/b", "now a dir")])
            .await
            .expect("v2");
        assert_eq!(flatten(&src, r2).await.keys().collect::<Vec<_>>(), ["a/b"]);
    }

    #[tokio::test]
    async fn directory_replaced_by_file_when_emptied_in_same_delta() {
        let src = MemSource::default();
        let r1 = build(&src, None, vec![put("a/x", "1"), put("a/sub/y", "2"), put("keep", "k")])
            .await
            .expect("v1");
        let r2 = build(&src, Some(r1), vec![del("a/x"), del("a/sub/y"), put("a", "now a file")])
            .await
            .expect("v2");
        let files = flatten(&src, r2).await;
        assert_eq!(files.keys().collect::<Vec<_>>(), ["a", "keep"]);
        // 没删干净则仍是冲突
        let r = build(&src, Some(r1), vec![del("a/x"), put("a", "file")]).await;
        assert_eq!(r, Err(TreeError::PathConflict("a".into())));
    }

    #[tokio::test]
    async fn mode_change_kept() {
        let src = MemSource::default();
        let r1 = build(&src, None, vec![put("run.sh", "x")]).await.expect("v1");
        let exec = FileChange::put("run.sh", EntryMode::Executable, ObjectId::of_blob(b"x"), 1);
        let r2 = build(&src, Some(r1), vec![exec]).await.expect("v2");
        let e = lookup(&src, r2, "run.sh").await.expect("ok").expect("entry");
        assert_eq!(e.mode, EntryMode::Executable);
    }

    #[tokio::test]
    async fn errors() {
        let src = MemSource::default();
        let r1 = build(&src, None, vec![put("dir/f", "f"), put("file", "x")])
            .await
            .expect("v1");
        let cases = [
            (vec![del("nope")], TreeError::DeleteMissing("nope".into())),
            (vec![del("dir/nope")], TreeError::DeleteMissing("dir/nope".into())),
            (vec![del("dir")], TreeError::DeleteDirectory("dir".into())),
            (vec![put("dir", "x")], TreeError::PathConflict("dir".into())),
            (vec![put("file/sub", "x")], TreeError::PathConflict("file".into())),
            (
                vec![put("new", "x"), put("new/sub", "y")],
                TreeError::PathConflict("new".into()),
            ),
        ];
        for (delta, want) in cases {
            assert_eq!(build(&src, Some(r1), delta.clone()).await, Err(want), "{delta:?}");
        }
    }

    #[tokio::test]
    async fn missing_base_tree_reported() {
        let src = MemSource::default();
        let ghost = ObjectId::hash(scion_object::ObjectKind::Tree, b"ghost");
        assert!(
            matches!(build(&src, Some(ghost), vec![put("a", "a")]).await, Err(TreeError::BadTree(id, _)) if id == ghost)
        );
    }

    async fn diff_paths(
        src: &MemSource,
        a: Option<ObjectId>,
        b: Option<ObjectId>,
        path: &str,
    ) -> Vec<(String, &'static str)> {
        diff(src, a, b, path)
            .await
            .expect("diff")
            .into_iter()
            .map(|d| {
                let kind = match (&d.old, &d.new) {
                    (None, Some(_)) => "A",
                    (Some(_), None) => "D",
                    _ => "M",
                };
                (d.path, kind)
            })
            .collect()
    }

    #[tokio::test]
    async fn diff_reports_adds_mods_deletes_sorted() {
        let src = MemSource::default();
        let r1 = build(
            &src,
            None,
            vec![put("a/x", "1"), put("a/y", "1"), put("b", "b"), put("keep/k", "k")],
        )
        .await
        .expect("v1");
        let r2 = build(
            &src,
            Some(r1),
            vec![put("a/x", "2"), del("a/y"), put("c/new", "n"), del("b")],
        )
        .await
        .expect("v2");
        assert_eq!(
            diff_paths(&src, Some(r1), Some(r2), "").await,
            [
                ("a/x".into(), "M"),
                ("a/y".into(), "D"),
                ("b".into(), "D"),
                ("c/new".into(), "A")
            ]
        );
        assert!(diff_paths(&src, Some(r1), Some(r1), "").await.is_empty());
        // 从空树开始：全部是新增
        assert_eq!(diff_paths(&src, None, Some(r1), "").await.len(), 4);
        assert_eq!(diff_paths(&src, Some(r1), None, "").await.len(), 4);
        assert!(diff_paths(&src, None, None, "").await.is_empty());
    }

    #[tokio::test]
    async fn diff_mode_change_and_type_change() {
        let src = MemSource::default();
        let r1 = build(&src, None, vec![put("f", "x"), put("d/inner", "i")])
            .await
            .expect("v1");
        let exec = FileChange::put("f", EntryMode::Executable, ObjectId::of_blob(b"x"), 1);
        let r2 = build(&src, Some(r1), vec![exec, del("d/inner"), put("d", "now a file")])
            .await
            .expect("v2");
        assert_eq!(
            diff_paths(&src, Some(r1), Some(r2), "").await,
            [("d".into(), "A"), ("d/inner".into(), "D"), ("f".into(), "M")]
        );
        // 反方向：文件变回目录
        assert_eq!(
            diff_paths(&src, Some(r2), Some(r1), "").await,
            [("d".into(), "D"), ("d/inner".into(), "A"), ("f".into(), "M")]
        );
    }

    #[tokio::test]
    async fn diff_limited_to_path() {
        let src = MemSource::default();
        let r1 = build(&src, None, vec![put("a/x", "1"), put("b/y", "1")])
            .await
            .expect("v1");
        let r2 = build(&src, Some(r1), vec![put("a/x", "2"), put("b/y", "2")])
            .await
            .expect("v2");
        assert_eq!(diff_paths(&src, Some(r1), Some(r2), "a").await, [("a/x".into(), "M")]);
        assert_eq!(diff_paths(&src, Some(r1), Some(r2), "b/y").await, [("b/y".into(), "M")]);
        assert!(diff_paths(&src, Some(r1), Some(r2), "nope").await.is_empty());
    }

    #[tokio::test]
    async fn diff_skips_identical_subtrees_without_loading() {
        let src = MemSource::default();
        let r1 = build(
            &src,
            None,
            vec![put("big/a", "1"), put("big/b", "2"), put("small", "s")],
        )
        .await
        .expect("v1");
        let r2 = build(&src, Some(r1), vec![put("small", "t")]).await.expect("v2");
        // 删掉 big 子树对象：如果 diff 去读它就会报错
        let big = lookup(&src, r1, "big").await.expect("ok").expect("big").id;
        src.trees.lock().expect("lock").remove(&big);
        assert_eq!(diff_paths(&src, Some(r1), Some(r2), "").await, [("small".into(), "M")]);
    }

    #[tokio::test]
    async fn diff_missing_tree_reported() {
        let src = MemSource::default();
        let r1 = build(&src, None, vec![put("a/x", "1")]).await.expect("v1");
        let ghost = ObjectId::hash(scion_object::ObjectKind::Tree, b"ghost");
        assert!(matches!(diff(&src, Some(r1), Some(ghost), "").await, Err(TreeError::BadTree(..))));
        assert!(matches!(diff(&src, Some(ghost), Some(r1), "a").await, Err(TreeError::BadTree(..))));
    }

    #[tokio::test]
    async fn lookup_cases() {
        let src = MemSource::default();
        let r = build(&src, None, vec![put("a/b", "b")]).await.expect("build");
        assert_eq!(lookup(&src, r, "").await, Ok(None));
        assert_eq!(lookup(&src, r, "zzz").await, Ok(None));
        assert_eq!(lookup(&src, r, "a/b/c").await, Ok(None), "below a file");
        assert_eq!(lookup(&src, r, "a/zzz").await, Ok(None));
        assert_eq!(lookup(&src, r, "a/b").await.expect("ok").map(|e| e.size), Some(1));
    }

    #[tokio::test]
    async fn blob_tree_source_reads_and_reports_errors() {
        let store = Arc::new(scion_store::MemStore::new());
        let t = Tree::default();
        store.put(t.id(), Bytes::from(t.encode())).await.expect("put");
        // 字节与 id 相符（存储层会校验），但不是合法的目录树编码
        let garbage = Bytes::from_static(b"\x02\x01\x05");
        let bad = ObjectId::hash(scion_object::ObjectKind::Tree, &garbage);
        store.put(bad, garbage).await.expect("put garbage");
        let src = BlobTreeSource(store);
        assert_eq!(src.load_tree(&t.id()).await, Ok(t));
        assert!(matches!(src.load_tree(&bad).await, Err(TreeError::BadTree(..))));
        let missing = ObjectId::hash(scion_object::ObjectKind::Tree, b"missing");
        assert!(matches!(src.load_tree(&missing).await, Err(TreeError::BadTree(..))));
    }

    #[test]
    fn error_display() {
        assert_eq!(
            TreeError::DeleteMissing("a".into()).to_string(),
            "cannot delete \"a\": no such file"
        );
        assert!(TreeError::Storage("x".into()).to_string().contains("storage"));
        assert!(TreeError::PathConflict("p".into()).to_string().contains("conflict"));
        assert!(TreeError::DeleteDirectory("d".into()).to_string().contains("directory"));
    }

    // 性质测试：任意一串提交，增量建树的结果与「从头全量建树」完全一致（目录树 id 相同）。
    proptest::proptest! {
        #![proptest_config(proptest::prelude::ProptestConfig { cases: 64, ..Default::default() })]
        #[test]
        fn incremental_equals_full_rebuild(
            steps in proptest::collection::vec(
                proptest::collection::btree_map("[ab]{1,2}(/[ab]{1,2}){0,2}", proptest::option::of("[xyz]{0,3}"), 1..6),
                1..6,
            )
        ) {
            let rt = tokio::runtime::Builder::new_current_thread().build().expect("rt");
            rt.block_on(async {
                let src = MemSource::default();
                let mut state: BTreeMap<String, String> = BTreeMap::new();
                let mut root: Option<ObjectId> = None;
                for step in steps {
                    // 构造对当前状态合法的 delta：删除只删存在的文件，写入不与现有文件/目录冲突
                    let mut next = state.clone();
                    let mut delta = Vec::new();
                    for (path, content) in step {
                        match content {
                            None if state.contains_key(&path) => {
                                next.remove(&path);
                                delta.push(del(&path));
                            }
                            Some(c) => {
                                let clash = next.keys().any(|k| k != &path && (k.starts_with(&format!("{path}/")) || path.starts_with(&format!("{k}/"))));
                                if !clash {
                                    next.insert(path.clone(), c.clone());
                                    delta.push(put(&path, &c));
                                }
                            }
                            None => {}
                        }
                    }
                    let r = build(&src, root, delta).await.expect("valid delta");
                    state = next;
                    root = Some(r);
                    // 全量重建
                    let full_src = MemSource::default();
                    let full: Vec<_> = state.iter().map(|(p, c)| put(p, c)).collect();
                    let full_root = build(&full_src, None, full).await.expect("full");
                    assert_eq!(r, full_root, "state {state:?}");
                }
            });
        }
    }
}
