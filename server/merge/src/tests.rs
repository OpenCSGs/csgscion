use std::sync::Arc;

use super::*;
use scion_object::ObjectKind;
use scion_store::MemStore;
use scion_tree::BlobTreeSource;

struct Fx {
    store: Arc<MemStore>,
    src: BlobTreeSource,
}

impl Fx {
    fn new() -> Self {
        let store = Arc::new(MemStore::new());
        Self {
            src: BlobTreeSource(store.clone()),
            store,
        }
    }

    /// 写入文件内容并返回对应的 put 改动。
    async fn put(&self, path: &str, data: &[u8]) -> FileChange {
        self.put_mode(path, EntryMode::File, data).await
    }

    async fn put_mode(&self, path: &str, mode: EntryMode, data: &[u8]) -> FileChange {
        let id = ObjectId::of_blob(data);
        self.store.put(id, Bytes::copy_from_slice(data)).await.expect("blob");
        FileChange::put(path, mode, id, data.len() as u64)
    }

    /// 在 `base` 上应用改动，写入新目录树，返回根。
    async fn tree(&self, base: Option<ObjectId>, changes: &[FileChange]) -> ObjectId {
        let mut changes = changes.to_vec();
        changes.sort_by(|a, b| a.path.cmp(&b.path));
        let built = apply_delta(&self.src, base, &changes).await.expect("tree");
        for (id, bytes) in built.new_trees {
            self.store.put(id, bytes).await.expect("store tree");
        }
        built.root
    }

    async fn rebase(&self, base: Option<ObjectId>, onto: Option<ObjectId>, delta: &[FileChange]) -> Outcome {
        self.rebase_with(base, onto, delta, Limits::default()).await
    }

    async fn rebase_with(
        &self,
        base: Option<ObjectId>,
        onto: Option<ObjectId>,
        delta: &[FileChange],
        limits: Limits,
    ) -> Outcome {
        let out = rebase_delta(&self.src, self.store.as_ref(), base, onto, delta, limits)
            .await
            .expect("rebase");
        // 与调用方一样：写入新目录树（合并产生的 blob 由各测试自己决定是否写入）
        if let Outcome::Clean(r) = &out {
            for (id, bytes) in &r.built.new_trees {
                self.store.put(*id, bytes.clone()).await.expect("store tree");
            }
        }
        out
    }

    async fn file(&self, root: ObjectId, path: &str) -> Option<(EntryMode, Bytes)> {
        let e = lookup(&self.src, root, path).await.expect("lookup")?;
        let data = self.store.get(&e.id).await.expect("get").expect("blob");
        Some((e.mode, data))
    }
}

fn clean(o: Outcome) -> Rebased {
    match o {
        Outcome::Clean(r) => r,
        Outcome::Conflicts(c) => panic!("unexpected conflicts {c:?}"),
    }
}

fn conflicts(o: Outcome) -> Vec<Conflict> {
    match o {
        Outcome::Clean(r) => panic!("expected conflicts, got {:?}", r.delta),
        Outcome::Conflicts(c) => c,
    }
}

fn id_of(c: &FileChange) -> ObjectId {
    put_id(&c.op).expect("put")
}

#[tokio::test]
async fn same_base_applies_delta_unchanged() {
    let fx = Fx::new();
    let base = fx.tree(None, &[fx.put("a", b"1").await]).await;
    let delta = vec![fx.put("a", b"2").await];
    let r = clean(fx.rebase(Some(base), Some(base), &delta).await);
    assert_eq!(r.delta, delta);
    assert!(r.merged_blobs.is_empty());
    assert_eq!(
        fx.file(r.built.root, "a").await.map(|f| f.1),
        Some(Bytes::from_static(b"2"))
    );
    // 空仓库上的第一个提交
    let r = clean(fx.rebase(None, None, &delta).await);
    assert_eq!(r.delta, delta);
}

#[tokio::test]
async fn disjoint_changes_apply_without_merging() {
    let fx = Fx::new();
    let base = fx
        .tree(None, &[fx.put("a", b"1").await, fx.put("dir/b", b"1").await])
        .await;
    let onto = fx.tree(Some(base), &[fx.put("dir/b", b"2").await]).await;
    let delta = vec![fx.put("a", b"2").await, fx.put("new", b"n").await];
    let r = clean(fx.rebase(Some(base), Some(onto), &delta).await);
    assert_eq!(r.delta, delta);
    assert!(r.merged_blobs.is_empty());
    let root = r.built.root;
    assert_eq!(fx.file(root, "a").await.map(|f| f.1), Some(Bytes::from_static(b"2")));
    assert_eq!(
        fx.file(root, "dir/b").await.map(|f| f.1),
        Some(Bytes::from_static(b"2"))
    );
    assert_eq!(fx.file(root, "new").await.map(|f| f.1), Some(Bytes::from_static(b"n")));
    // 改动已存在的根时结果与直接在 onto 上应用相同
    assert_eq!(root, fx.tree(Some(onto), &delta).await);
}

#[tokio::test]
async fn non_overlapping_text_edits_merge() {
    let fx = Fx::new();
    let base = fx.tree(None, &[fx.put("f", b"1\n2\n3\n4\n5\n").await]).await;
    let onto = fx
        .tree(Some(base), &[fx.put("f", b"1 theirs\n2\n3\n4\n5\n").await])
        .await;
    let delta = vec![fx.put("f", b"1\n2\n3\n4\n5 ours\n").await];
    let r = clean(fx.rebase(Some(base), Some(onto), &delta).await);
    let merged = Bytes::from_static(b"1 theirs\n2\n3\n4\n5 ours\n");
    assert_eq!(r.merged_blobs, vec![(ObjectId::of_blob(&merged), merged.clone())]);
    assert_eq!(id_of(&r.delta[0]), ObjectId::of_blob(&merged));
    assert_eq!(
        fx.store.get(&ObjectId::of_blob(&merged)).await,
        Ok(None),
        "caller stores merged blobs"
    );
    fx.store
        .put(ObjectId::of_blob(&merged), merged.clone())
        .await
        .expect("store");
    assert_eq!(fx.file(r.built.root, "f").await, Some((EntryMode::File, merged)));
}

#[tokio::test]
async fn overlapping_text_edits_conflict_with_three_ids() {
    let fx = Fx::new();
    let b = fx.put("f", b"1\n2\n3\n").await;
    let base = fx.tree(None, std::slice::from_ref(&b)).await;
    let t = fx.put("f", b"1\ntheirs\n3\n").await;
    let onto = fx.tree(Some(base), std::slice::from_ref(&t)).await;
    let o = fx.put("f", b"1\nours\n3\n").await;
    let c = conflicts(fx.rebase(Some(base), Some(onto), std::slice::from_ref(&o)).await);
    assert_eq!(
        c,
        vec![Conflict {
            path: "f".into(),
            kind: ConflictKind::Content,
            base: Some(id_of(&b)),
            ours: Some(id_of(&o)),
            theirs: Some(id_of(&t)),
        }]
    );
}

#[tokio::test]
async fn all_conflicts_are_reported_together() {
    let fx = Fx::new();
    let base = fx
        .tree(None, &[fx.put("a", b"1\n").await, fx.put("b", b"1\n").await])
        .await;
    let onto = fx
        .tree(Some(base), &[fx.put("a", b"t\n").await, fx.put("b", b"t\n").await])
        .await;
    let delta = vec![fx.put("a", b"o\n").await, fx.put("b", b"o\n").await];
    let c = conflicts(fx.rebase(Some(base), Some(onto), &delta).await);
    assert_eq!(c.iter().map(|c| c.path.as_str()).collect::<Vec<_>>(), ["a", "b"]);
}

#[tokio::test]
async fn identical_changes_on_both_sides_are_dropped() {
    let fx = Fx::new();
    let base = fx
        .tree(None, &[fx.put("a", b"1").await, fx.put("gone", b"x").await])
        .await;
    let onto = fx
        .tree(Some(base), &[fx.put("a", b"2").await, FileChange::delete("gone")])
        .await;
    let delta = vec![fx.put("a", b"2").await, FileChange::delete("gone")];
    let r = clean(fx.rebase(Some(base), Some(onto), &delta).await);
    assert!(r.delta.is_empty());
    assert_eq!(r.built.root, onto);
    // 双方新增了相同的文件
    let onto2 = fx.tree(Some(base), &[fx.put("new", b"same").await]).await;
    let r = clean(
        fx.rebase(Some(base), Some(onto2), &[fx.put("new", b"same").await])
            .await,
    );
    assert!(r.delta.is_empty());
}

#[tokio::test]
async fn modify_and_delete_conflict_both_ways() {
    let fx = Fx::new();
    let b = fx.put("f", b"1").await;
    let base = fx.tree(None, std::slice::from_ref(&b)).await;
    // 对方删除，本方修改
    let deleted = fx.tree(Some(base), &[FileChange::delete("f")]).await;
    let o = fx.put("f", b"ours").await;
    let c = conflicts(fx.rebase(Some(base), Some(deleted), std::slice::from_ref(&o)).await);
    assert_eq!(
        (c[0].kind, c[0].base, c[0].ours, c[0].theirs),
        (ConflictKind::ModifyDelete, Some(id_of(&b)), Some(id_of(&o)), None)
    );
    // 对方修改，本方删除
    let t = fx.put("f", b"theirs").await;
    let modified = fx.tree(Some(base), std::slice::from_ref(&t)).await;
    let c = conflicts(fx.rebase(Some(base), Some(modified), &[FileChange::delete("f")]).await);
    assert_eq!(
        (c[0].kind, c[0].ours, c[0].theirs),
        (ConflictKind::DeleteModify, None, Some(id_of(&t)))
    );
}

#[tokio::test]
async fn add_add_with_different_content_conflicts() {
    let fx = Fx::new();
    let base = fx.tree(None, &[fx.put("x", b"x").await]).await;
    let onto = fx.tree(Some(base), &[fx.put("new", b"theirs\n").await]).await;
    let c = conflicts(
        fx.rebase(Some(base), Some(onto), &[fx.put("new", b"ours\n").await])
            .await,
    );
    assert_eq!((c[0].kind, c[0].base), (ConflictKind::AddAdd, None));
}

#[tokio::test]
async fn binary_and_oversized_files_are_not_merged() {
    let fx = Fx::new();
    let base = fx
        .tree(
            None,
            &[fx.put("bin", b"a\0b\n1\n").await, fx.put("big", b"1\n2\n3\n").await],
        )
        .await;
    let onto = fx
        .tree(
            Some(base),
            &[fx.put("bin", b"a\0b\n1\n2\n").await, fx.put("big", b"t\n2\n3\n").await],
        )
        .await;
    let c = conflicts(
        fx.rebase(Some(base), Some(onto), &[fx.put("bin", b"0\na\0b\n1\n").await])
            .await,
    );
    assert_eq!(c[0].kind, ConflictKind::Binary);
    let small = Limits { max_merge_bytes: 4 };
    let c = conflicts(
        fx.rebase_with(Some(base), Some(onto), &[fx.put("big", b"1\n2\no\n").await], small)
            .await,
    );
    assert_eq!(c[0].kind, ConflictKind::TooLarge);
    // 同样的改动在默认上限下可以合并
    assert!(matches!(
        fx.rebase(Some(base), Some(onto), &[fx.put("big", b"1\n2\no\n").await])
            .await,
        Outcome::Clean(_)
    ));
}

#[tokio::test]
async fn executable_bit_and_content_merge_independently() {
    let fx = Fx::new();
    let base = fx.tree(None, &[fx.put("s", b"echo 1\n").await]).await;
    // 对方改内容，本方只加可执行位：取对方内容 + 可执行
    let onto = fx.tree(Some(base), &[fx.put("s", b"echo 2\n").await]).await;
    let chmod = fx.put_mode("s", EntryMode::Executable, b"echo 1\n").await;
    let r = clean(fx.rebase(Some(base), Some(onto), &[chmod]).await);
    assert!(r.merged_blobs.is_empty());
    assert_eq!(
        fx.file(r.built.root, "s").await,
        Some((EntryMode::Executable, Bytes::from_static(b"echo 2\n")))
    );
    // 对方加可执行位，本方改内容：取本方内容 + 可执行
    let onto = fx
        .tree(
            Some(base),
            &[fx.put_mode("s", EntryMode::Executable, b"echo 1\n").await],
        )
        .await;
    let edit = fx.put("s", b"echo 3\n").await;
    let r = clean(fx.rebase(Some(base), Some(onto), &[edit]).await);
    assert_eq!(
        fx.file(r.built.root, "s").await,
        Some((EntryMode::Executable, Bytes::from_static(b"echo 3\n")))
    );
    // 双方都加了可执行位，内容只有本方改：保留本方改动
    let both = fx.put_mode("s", EntryMode::Executable, b"echo 4\n").await;
    let r = clean(fx.rebase(Some(base), Some(onto), std::slice::from_ref(&both)).await);
    assert_eq!(r.delta, vec![both]);
    // 本方只改回与对方相同的结果：去掉改动
    let same = fx.put_mode("s", EntryMode::Executable, b"echo 1\n").await;
    let r = clean(fx.rebase(Some(base), Some(onto), &[same]).await);
    assert!(r.delta.is_empty());
}

#[tokio::test]
async fn mode_only_merge_drops_change_already_in_onto() {
    let fx = Fx::new();
    let base = fx.tree(None, &[fx.put("s", b"1\n").await]).await;
    // 对方：改内容并加可执行位；本方：只加可执行位 → 结果就是对方的版本
    let onto = fx
        .tree(Some(base), &[fx.put_mode("s", EntryMode::Executable, b"2\n").await])
        .await;
    let chmod = fx.put_mode("s", EntryMode::Executable, b"1\n").await;
    let r = clean(fx.rebase(Some(base), Some(onto), &[chmod]).await);
    assert!(r.delta.is_empty());
}

#[tokio::test]
async fn overlapping_symlink_changes_conflict() {
    let fx = Fx::new();
    let base = fx.tree(None, &[fx.put_mode("l", EntryMode::Symlink, b"a").await]).await;
    let onto = fx
        .tree(Some(base), &[fx.put_mode("l", EntryMode::Symlink, b"b").await])
        .await;
    let ours = fx.put_mode("l", EntryMode::Symlink, b"c").await;
    let c = conflicts(fx.rebase(Some(base), Some(onto), &[ours]).await);
    assert_eq!(c[0].kind, ConflictKind::Symlink);
}

#[tokio::test]
async fn file_directory_conflicts() {
    let fx = Fx::new();
    let base = fx.tree(None, &[fx.put("f", b"1").await, fx.put("x", b"x").await]).await;
    // 对方把 f 换成了目录 f/g，本方修改 f
    let onto = fx
        .tree(Some(base), &[FileChange::delete("f"), fx.put("f/g", b"g").await])
        .await;
    let c = conflicts(fx.rebase(Some(base), Some(onto), &[fx.put("f", b"2").await]).await);
    assert_eq!(c[0].kind, ConflictKind::Path);
    // 对方新建了文件 d，本方在 d/ 下新建文件：在应用阶段发现
    let onto = fx.tree(Some(base), &[fx.put("d", b"file").await]).await;
    let o = fx.put("d/x", b"x").await;
    let c = conflicts(fx.rebase(Some(base), Some(onto), std::slice::from_ref(&o)).await);
    assert_eq!(
        (c[0].path.as_str(), c[0].kind, c[0].ours),
        ("d", ConflictKind::Path, None)
    );
}

#[tokio::test]
async fn missing_objects_are_errors_not_conflicts() {
    let fx = Fx::new();
    let base = fx.tree(None, &[fx.put("f", b"1\n2\n").await]).await;
    let onto = fx.tree(Some(base), &[fx.put("f", b"t\n2\n").await]).await;
    // 本方内容的 blob 没有上传
    let ghost = FileChange::put("f", EntryMode::File, ObjectId::of_blob(b"1\no\n"), 4);
    let err = rebase_delta(
        &fx.src,
        fx.store.as_ref(),
        Some(base),
        Some(onto),
        &[ghost],
        Limits::default(),
    )
    .await;
    assert!(matches!(err, Err(MergeError::MissingBlob(_))));
    // 目录树不存在
    let bad = ObjectId::hash(ObjectKind::Tree, b"nope");
    let err = rebase_delta(
        &fx.src,
        fx.store.as_ref(),
        Some(base),
        Some(bad),
        &[fx.put("f", b"x").await],
        Limits::default(),
    )
    .await;
    assert!(matches!(err, Err(MergeError::Tree(_))));
}

#[test]
fn helpers() {
    assert_eq!(merge3(1, 1, 2), Some(2));
    assert_eq!(merge3(1, 2, 1), Some(2));
    assert_eq!(merge3(1, 2, 2), Some(2));
    assert_eq!(merge3(1, 2, 3), None);
    assert!(is_binary(b"a\0b"));
    assert!(!is_binary(b"text\n"));
    let mut late_nul = vec![b'a'; 9000];
    late_nul.push(0);
    assert!(!is_binary(&late_nul), "only the first 8000 bytes are inspected");
    let names: Vec<_> = [
        ConflictKind::Content,
        ConflictKind::AddAdd,
        ConflictKind::ModifyDelete,
        ConflictKind::DeleteModify,
        ConflictKind::Binary,
        ConflictKind::TooLarge,
        ConflictKind::Symlink,
        ConflictKind::Path,
    ]
    .iter()
    .map(|k| k.as_str())
    .collect();
    assert_eq!(
        names,
        [
            "content",
            "add_add",
            "modify_delete",
            "delete_modify",
            "binary",
            "too_large",
            "symlink",
            "path"
        ]
    );
    assert_eq!(Limits::default().max_merge_bytes, 8 << 20);
    assert_eq!(put_size(&FileOp::Delete), 0);
}
