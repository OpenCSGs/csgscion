use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU8, Ordering};

use bytes::Bytes;
use scion_merge::ConflictKind;
use scion_object::{Changeset, EntryMode, FileChange, ObjectId, ObjectKind, Signature};
use scion_query::{GIT_SHA1_EXTENSION, load_changeset};
use scion_store::{BlobStore, IndexedObject, MemStore, MetaStore};
use scion_tree::{BlobTreeSource, lookup};

use super::*;

const REPO: &str = "r";

struct Fx {
    store: Arc<MemStore>,
    w: Arc<ChangesetWriter>,
    n: AtomicU8,
}

fn sig() -> Signature {
    Signature {
        name: "Ada".into(),
        email: "ada@example.com".into(),
        time_ms: 1,
        tz_offset_minutes: 0,
    }
}

impl Fx {
    async fn new() -> Self {
        let store = Arc::new(MemStore::new());
        store.create_repo(REPO).await.expect("repo");
        Self {
            w: Arc::new(ChangesetWriter::new(store.clone(), store.clone())),
            store,
            n: AtomicU8::new(0),
        }
    }

    async fn upload(&self, data: &[u8]) -> ObjectId {
        let id = ObjectId::of_blob(data);
        self.store.put(id, Bytes::copy_from_slice(data)).await.expect("blob");
        self.store
            .index_objects(
                REPO,
                &[IndexedObject {
                    id,
                    size: data.len() as u64,
                }],
            )
            .await
            .expect("index");
        id
    }

    async fn changeset(&self, parents: &[ObjectId], files: &[(&str, Option<&str>)]) -> Changeset {
        self.changeset_ext(parents, files, BTreeMap::new()).await
    }

    async fn changeset_ext(
        &self,
        parents: &[ObjectId],
        files: &[(&str, Option<&str>)],
        ext: BTreeMap<String, Vec<u8>>,
    ) -> Changeset {
        let mut delta = Vec::new();
        for (path, content) in files {
            delta.push(match content {
                Some(c) => {
                    let id = self.upload(c.as_bytes()).await;
                    FileChange::put(*path, EntryMode::File, id, c.len() as u64)
                }
                None => FileChange::delete(*path),
            });
        }
        let n = self.n.fetch_add(1, Ordering::Relaxed);
        Changeset::new(parents.to_vec(), sig(), sig(), format!("c{n}"), [n; 16], delta, ext).expect("cs")
    }

    async fn commit(&self, parents: &[ObjectId], files: &[(&str, Option<&str>)]) -> ObjectId {
        let cs = self.changeset(parents, files).await;
        self.w.create(REPO, &cs).await.expect("create").id
    }

    async fn read(&self, cs: ObjectId, path: &str) -> Option<String> {
        let root = self.store.get_changeset(REPO, &cs).await.expect("get")?.root_tree;
        let e = lookup(&BlobTreeSource(self.store.clone()), root, path)
            .await
            .expect("lookup")?;
        let data = self.store.get(&e.id).await.expect("blob")?;
        Some(String::from_utf8(data.to_vec()).expect("utf8"))
    }

    async fn prepare(&self, tip: ObjectId, onto: Option<ObjectId>) -> Result<Prepared> {
        prepare(&self.w, REPO, tip, onto, StackOptions::default()).await
    }

    fn lander(&self) -> Lander {
        Lander::new(self.w.clone())
    }

    /// 从 tip 沿第一父提交往回的全部提交（新到旧）。
    async fn chain(&self, tip: ObjectId) -> Vec<ObjectId> {
        let mut out = Vec::new();
        let mut cur = Some(tip);
        while let Some(id) = cur {
            out.push(id);
            cur = self
                .store
                .get_changeset(REPO, &id)
                .await
                .expect("get")
                .expect("rec")
                .first_parent();
        }
        out
    }
}

// ---- 创建提交 ----

#[tokio::test]
async fn create_validates_parents_objects_and_delta() {
    let fx = Fx::new().await;
    let a = fx.commit(&[], &[("f", Some("1"))]).await;
    let rec = fx.store.get_changeset(REPO, &a).await.expect("get").expect("rec");
    assert_eq!((rec.generation, rec.fp_depth), (1, 1));
    assert_eq!(fx.read(a, "f").await.as_deref(), Some("1"));

    let ghost = ObjectId::hash(ObjectKind::Changeset, b"ghost");
    let cs = fx.changeset(&[ghost], &[]).await;
    assert_eq!(
        fx.w.create(REPO, &cs).await,
        Err(LandError::ObjectsNotUploaded(vec![ghost]))
    );

    let missing = ObjectId::of_blob(b"never uploaded");
    let cs = Changeset::new(
        vec![a],
        sig(),
        sig(),
        "m",
        [9; 16],
        vec![FileChange::put("g", EntryMode::File, missing, 14)],
        BTreeMap::new(),
    )
    .expect("cs");
    assert_eq!(
        fx.w.create(REPO, &cs).await,
        Err(LandError::ObjectsNotUploaded(vec![missing]))
    );

    let id = fx.upload(b"abc").await;
    let cs = Changeset::new(
        vec![a],
        sig(),
        sig(),
        "m",
        [9; 16],
        vec![FileChange::put("g", EntryMode::File, id, 99)],
        BTreeMap::new(),
    )
    .expect("cs");
    assert!(matches!(fx.w.create(REPO, &cs).await, Err(LandError::InvalidChangeset { path, .. }) if path == "g"));

    let cs = fx.changeset(&[a], &[("nope", None)]).await;
    assert!(matches!(fx.w.create(REPO, &cs).await, Err(LandError::InvalidChangeset { path, .. }) if path == "nope"));

    let cs = fx.changeset(&[], &[]).await;
    assert_eq!(
        fx.w.create("missing", &cs).await,
        Err(LandError::RepoNotFound("missing".into()))
    );
}

// ---- prepare ----

#[tokio::test]
async fn prepare_new_ref_and_already_included() {
    let fx = Fx::new().await;
    let a = fx.commit(&[], &[("f", Some("1"))]).await;
    let b = fx.commit(&[a], &[("f", Some("2"))]).await;
    let p = fx.prepare(b, None).await.expect("new ref");
    assert_eq!(p.tip, b);
    assert_eq!(p.landed.len(), 1);
    assert_eq!((p.landed[0].original, p.landed[0].landed), (b, b));
    // tip 已经在历史中
    for tip in [a, b] {
        let p = fx.prepare(tip, Some(b)).await.expect("included");
        assert_eq!((p.tip, p.landed.len()), (b, 0));
    }
}

#[tokio::test]
async fn prepare_fast_forward_records_the_stack() {
    let fx = Fx::new().await;
    let a = fx.commit(&[], &[("f", Some("1"))]).await;
    let b = fx.commit(&[a], &[("f", Some("2"))]).await;
    let c = fx.commit(&[b], &[("g", Some("3"))]).await;
    let p = fx.prepare(c, Some(a)).await.expect("ff");
    assert_eq!(p.tip, c);
    assert_eq!(p.landed.iter().map(|l| l.landed).collect::<Vec<_>>(), [b, c]);
    assert!(p.landed.iter().all(|l| l.original == l.landed));
    // 记录数受 max_stack 限制，但快进本身不受限
    let opts = StackOptions {
        max_stack: 1,
        ..StackOptions::default()
    };
    let p = prepare(&fx.w, REPO, c, Some(a), opts).await.expect("long ff");
    assert_eq!((p.tip, p.landed.len()), (c, 1));
    // 合并提交（第一父是 ref 当前提交）可以快进
    let side = fx.commit(&[a], &[("side", Some("s"))]).await;
    let merge = fx.commit(&[c, side], &[("side", Some("s"))]).await;
    let p = fx.prepare(merge, Some(c)).await.expect("merge ff");
    assert_eq!(p.tip, merge);
}

#[tokio::test]
async fn prepare_rebases_disjoint_stack() {
    let fx = Fx::new().await;
    let base = fx.commit(&[], &[("a", Some("1")), ("b", Some("1"))]).await;
    let head = fx.commit(&[base], &[("b", Some("2"))]).await;
    let mut ext = BTreeMap::new();
    ext.insert(GIT_SHA1_EXTENSION.to_string(), vec![7; 20]);
    ext.insert("other".to_string(), b"kept".to_vec());
    let cs1 = fx.changeset_ext(&[base], &[("a", Some("2"))], ext).await;
    let s1 = fx.w.create(REPO, &cs1).await.expect("s1").id;
    let s2 = fx.commit(&[s1], &[("a", Some("3")), ("c", Some("new"))]).await;

    let p = fx.prepare(s2, Some(head)).await.expect("rebase");
    assert_eq!(p.landed.len(), 2);
    assert_eq!(p.landed.iter().map(|l| l.original).collect::<Vec<_>>(), [s1, s2]);
    assert!(p.landed.iter().all(|l| l.original != l.landed));
    assert_eq!(p.tip, p.landed[1].landed);
    assert_eq!(fx.chain(p.tip).await[1..], [p.landed[0].landed, head, base]);
    assert_eq!(fx.read(p.tip, "a").await.as_deref(), Some("3"));
    assert_eq!(fx.read(p.tip, "b").await.as_deref(), Some("2"));
    assert_eq!(fx.read(p.tip, "c").await.as_deref(), Some("new"));
    // 保留作者、说明与 change id；去掉 git sha，保留其它扩展
    let r1 = load_changeset(fx.store.as_ref(), p.landed[0].landed)
        .await
        .expect("load");
    assert_eq!(
        (r1.message.as_str(), r1.change_id, r1.author.clone()),
        ("c2", cs1.change_id, sig())
    );
    assert_eq!(p.landed[0].change_id, cs1.change_id);
    assert!(!r1.extensions.contains_key(GIT_SHA1_EXTENSION));
    assert_eq!(r1.extensions.get("other").map(Vec::as_slice), Some(&b"kept"[..]));
    // 相同输入得到相同结果（rebase 是确定的）
    assert_eq!(fx.prepare(s2, Some(head)).await.expect("again").tip, p.tip);
}

#[tokio::test]
async fn prepare_merges_text_and_reports_conflicts() {
    let fx = Fx::new().await;
    let base = fx.commit(&[], &[("f", Some("1\n2\n3\n4\n"))]).await;
    let head = fx.commit(&[base], &[("f", Some("one\n2\n3\n4\n"))]).await;
    let ours = fx.commit(&[base], &[("f", Some("1\n2\n3\nfour\n"))]).await;
    let p = fx.prepare(ours, Some(head)).await.expect("merge");
    assert_eq!(fx.read(p.tip, "f").await.as_deref(), Some("one\n2\n3\nfour\n"));
    // 合并产生的 blob 已写入并登记，可以被引用
    let rec = fx.store.get_changeset(REPO, &p.tip).await.expect("get").expect("rec");
    let e = lookup(&BlobTreeSource(fx.store.clone()), rec.root_tree, "f")
        .await
        .expect("lookup")
        .expect("entry");
    assert!(
        fx.store
            .object_sizes(REPO, &[e.id])
            .await
            .expect("sizes")
            .contains_key(&e.id)
    );

    let clash = fx.commit(&[base], &[("f", Some("uno\n2\n3\n4\n"))]).await;
    match fx.prepare(clash, Some(head)).await {
        Err(LandError::Conflict { changeset, conflicts }) => {
            assert_eq!(changeset, clash);
            assert_eq!(
                (conflicts[0].path.as_str(), conflicts[0].kind),
                ("f", ConflictKind::Content)
            );
        }
        other => panic!("expected conflict, got {other:?}"),
    }
}

#[tokio::test]
async fn prepare_rejections() {
    let fx = Fx::new().await;
    let base = fx.commit(&[], &[("f", Some("1"))]).await;
    let head = fx.commit(&[base], &[("g", Some("1"))]).await;
    let s1 = fx.commit(&[base], &[("h", Some("1"))]).await;
    let s2 = fx.commit(&[s1], &[("i", Some("1"))]).await;

    let ff_only = StackOptions {
        fast_forward_only: true,
        ..StackOptions::default()
    };
    assert_eq!(
        prepare(&fx.w, REPO, s1, Some(head), ff_only).await,
        Err(LandError::NotFastForward { tip: head })
    );
    let shallow = StackOptions {
        max_stack: 1,
        ..StackOptions::default()
    };
    assert_eq!(
        prepare(&fx.w, REPO, s2, Some(head), shallow).await,
        Err(LandError::StackTooDeep(1))
    );
    let other_root = fx.commit(&[], &[("x", Some("x"))]).await;
    assert_eq!(
        fx.prepare(other_root, Some(head)).await,
        Err(LandError::UnrelatedHistory { tip: other_root })
    );
    let merge = fx.commit(&[s1, head], &[]).await;
    assert_eq!(fx.prepare(merge, Some(s2)).await, Err(LandError::MergeInStack(merge)));
    let ghost = ObjectId::hash(ObjectKind::Changeset, b"ghost");
    assert_eq!(
        fx.prepare(ghost, Some(head)).await,
        Err(LandError::ObjectsNotUploaded(vec![ghost]))
    );
}

// ---- Lander ----

#[tokio::test]
async fn land_creates_fast_forwards_rebases_and_is_idempotent() {
    let fx = Fx::new().await;
    let l = fx.lander();
    let a = fx.commit(&[], &[("f", Some("1"))]).await;
    let b = fx.commit(&[a], &[("f", Some("2"))]).await;
    let side = fx.commit(&[a], &[("g", Some("1"))]).await;

    let o = l.land(REPO, "main", a, false).await.expect("create");
    assert_eq!((o.state.target, o.state.version, o.landed.len()), (a, 1, 1));
    let o = l.land(REPO, "main", b, false).await.expect("ff");
    assert_eq!((o.state.target, o.state.version), (b, 2));
    // 重复请求：不再写入
    let again = l.land(REPO, "main", b, false).await.expect("retry");
    assert_eq!((again.state, again.landed.len()), (o.state, 0));
    // 落后的提交：服务端 rebase
    let o = l.land(REPO, "main", side, false).await.expect("rebase");
    assert_eq!(o.state.version, 3);
    assert_ne!(o.state.target, side);
    assert_eq!(fx.read(o.state.target, "f").await.as_deref(), Some("2"));
    assert_eq!(fx.read(o.state.target, "g").await.as_deref(), Some("1"));
    assert_eq!(
        l.land(REPO, "main", fx.commit(&[a], &[]).await, true).await,
        Err(LandError::NotFastForward { tip: o.state.target })
    );
}

#[tokio::test]
async fn land_rejects_bad_input() {
    let fx = Fx::new().await;
    let l = fx.lander();
    let a = fx.commit(&[], &[]).await;
    assert_eq!(
        l.land(REPO, "bad//ref", a, false).await,
        Err(LandError::InvalidRef("bad//ref".into()))
    );
    assert_eq!(
        l.land("nope", "main", a, false).await,
        Err(LandError::RepoNotFound("nope".into()))
    );
    let ghost = ObjectId::hash(ObjectKind::Changeset, b"ghost");
    assert_eq!(
        l.land(REPO, "main", ghost, false).await,
        Err(LandError::ObjectsNotUploaded(vec![ghost]))
    );
}

/// 同一基础上的多个互不冲突的提交并发合入：全部成功，每个改动在最终历史中恰好出现一次。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_lands_all_succeed_with_rebase() {
    let fx = Arc::new(Fx::new().await);
    let base = fx.commit(&[], &[("base", Some("0"))]).await;
    fx.lander().land(REPO, "main", base, false).await.expect("base");
    let mut tips = Vec::new();
    for i in 0..16 {
        let path = format!("f{i}");
        tips.push(fx.commit(&[base], &[(path.as_str(), Some("x"))]).await);
    }
    let lander = Arc::new(fx.lander().with_max_attempts(64));
    let tasks: Vec<_> = tips
        .into_iter()
        .map(|t| {
            let l = lander.clone();
            tokio::spawn(async move { l.land(REPO, "main", t, false).await })
        })
        .collect();
    for t in tasks {
        t.await.expect("join").expect("land");
    }
    let head = fx.store.get_ref(REPO, "main").await.expect("get").expect("ref");
    assert_eq!(head.version, 17);
    let chain = fx.chain(head.target).await;
    assert_eq!(chain.len(), 17);
    for i in 0..16 {
        assert_eq!(fx.read(head.target, &format!("f{i}")).await.as_deref(), Some("x"));
    }
}

#[tokio::test]
async fn contention_is_reported_after_max_attempts() {
    /// 每次条件更新都失败的存储。
    struct AlwaysMoved(Arc<MemStore>);
    #[async_trait::async_trait]
    impl MetaStore for AlwaysMoved {
        async fn create_repo(&self, r: &str) -> scion_store::Result<()> {
            self.0.create_repo(r).await
        }
        async fn repo_exists(&self, r: &str) -> scion_store::Result<bool> {
            self.0.repo_exists(r).await
        }
        async fn list_repos(&self) -> scion_store::Result<Vec<String>> {
            self.0.list_repos().await
        }
        async fn get_ref(&self, r: &str, n: &str) -> scion_store::Result<Option<scion_store::RefState>> {
            self.0.get_ref(r, n).await
        }
        async fn cas_ref(
            &self,
            _: &str,
            n: &str,
            _: scion_store::RefExpect,
            _: ObjectId,
        ) -> scion_store::Result<scion_store::RefState> {
            Err(scion_store::Error::RefVersionMismatch {
                name: n.to_owned(),
                current: Some(99),
            })
        }
        async fn index_objects(&self, r: &str, o: &[IndexedObject]) -> scion_store::Result<()> {
            self.0.index_objects(r, o).await
        }
        async fn missing_objects(&self, r: &str, ids: &[ObjectId]) -> scion_store::Result<Vec<ObjectId>> {
            self.0.missing_objects(r, ids).await
        }
        async fn object_sizes(
            &self,
            r: &str,
            ids: &[ObjectId],
        ) -> scion_store::Result<std::collections::HashMap<ObjectId, u64>> {
            self.0.object_sizes(r, ids).await
        }
        async fn put_changeset(&self, r: &str, rec: &scion_store::ChangesetRecord) -> scion_store::Result<()> {
            self.0.put_changeset(r, rec).await
        }
        async fn get_changeset(
            &self,
            r: &str,
            id: &ObjectId,
        ) -> scion_store::Result<Option<scion_store::ChangesetRecord>> {
            self.0.get_changeset(r, id).await
        }
        async fn list_refs(&self, r: &str) -> scion_store::Result<Vec<(String, scion_store::RefState)>> {
            self.0.list_refs(r).await
        }
        async fn changesets_below(
            &self,
            r: &str,
            g: u64,
            l: usize,
        ) -> scion_store::Result<Vec<scion_store::ChangesetRecord>> {
            self.0.changesets_below(r, g, l).await
        }
        async fn repair_lineage(
            &self,
            r: &str,
            id: &ObjectId,
            p: &[ObjectId],
            d: u64,
            j: Option<ObjectId>,
        ) -> scion_store::Result<()> {
            self.0.repair_lineage(r, id, p, d, j).await
        }
        async fn index_paths(
            &self,
            r: &str,
            cs: ObjectId,
            g: u64,
            c: &[scion_store::PathChange],
        ) -> scion_store::Result<()> {
            self.0.index_paths(r, cs, g, c).await
        }
        async fn path_history(
            &self,
            r: &str,
            p: &str,
            g: u64,
            l: usize,
        ) -> scion_store::Result<Vec<scion_store::PathHistoryEntry>> {
            self.0.path_history(r, p, g, l).await
        }
        async fn get_derived(&self, r: &str, k: &str, v: u32, key: &[u8]) -> scion_store::Result<Option<Vec<u8>>> {
            self.0.get_derived(r, k, v, key).await
        }
        async fn put_derived(&self, r: &str, k: &str, v: u32, key: &[u8], val: &[u8]) -> scion_store::Result<()> {
            self.0.put_derived(r, k, v, key, val).await
        }
    }
    let fx = Fx::new().await;
    let a = fx.commit(&[], &[]).await;
    let w = Arc::new(ChangesetWriter::new(
        Arc::new(AlwaysMoved(fx.store.clone())),
        fx.store.clone(),
    ));
    let l = Lander::new(w).with_max_attempts(2);
    assert_eq!(l.land(REPO, "main", a, false).await, Err(LandError::Contention));
}

#[test]
fn error_conversions() {
    use scion_merge::MergeError;
    use scion_query::QueryError;
    use scion_tree::TreeError;
    let id = ObjectId::of_blob(b"x");
    assert_eq!(
        LandError::from(scion_store::Error::RepoNotFound("r".into())),
        LandError::RepoNotFound("r".into())
    );
    assert_eq!(
        LandError::from(scion_store::Error::RefVersionMismatch {
            name: "m".into(),
            current: None
        }),
        LandError::Contention
    );
    assert_eq!(
        LandError::from(scion_store::Error::Backend("down".into())),
        LandError::Unavailable("down".into())
    );
    assert!(matches!(
        LandError::from(scion_store::Error::InvalidRepoName("x".into())),
        LandError::Corrupt(_)
    ));
    assert!(matches!(
        LandError::from(TreeError::PathConflict("p".into())),
        LandError::InvalidChangeset { path, .. } if path == "p"
    ));
    assert!(matches!(
        LandError::from(TreeError::BadTree(id, "x".into())),
        LandError::Corrupt(_)
    ));
    assert_eq!(
        LandError::from(TreeError::Storage("s".into())),
        LandError::Unavailable("s".into())
    );
    assert!(matches!(
        LandError::from(MergeError::MissingBlob(id)),
        LandError::Corrupt(_)
    ));
    assert_eq!(
        LandError::from(MergeError::Storage("s".into())),
        LandError::Unavailable("s".into())
    );
    assert_eq!(
        LandError::from(MergeError::Tree(TreeError::Storage("t".into()))),
        LandError::Unavailable("t".into())
    );
    assert_eq!(
        LandError::from(QueryError::NotFound(id)),
        LandError::ObjectsNotUploaded(vec![id])
    );
    assert_eq!(
        LandError::from(QueryError::Store(scion_store::Error::Backend("b".into()))),
        LandError::Unavailable("b".into())
    );
    assert_eq!(
        LandError::from(QueryError::Tree(TreeError::Storage("t".into()))),
        LandError::Unavailable("t".into())
    );
    assert!(matches!(
        LandError::from(QueryError::Corrupt(id, "bad".into())),
        LandError::Corrupt(_)
    ));
    assert!(validate_ref_name("feature/x").is_ok());
    assert_eq!(validate_ref_name(""), Err(LandError::InvalidRef(String::new())));
    // 错误信息
    assert_eq!(
        LandError::StackTooDeep(3).to_string(),
        "stack has more than 3 changesets to rebase"
    );
    assert!(
        LandError::Conflict {
            changeset: id,
            conflicts: vec![]
        }
        .to_string()
        .contains("0 paths")
    );
}
