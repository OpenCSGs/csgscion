//! 存储后端一致性测试套件。每个后端（内存、SQLite、PostgreSQL、YugabyteDB、S3……）
//! 都必须在自己的测试中调用这里的全部函数并通过。
//!
//! 函数在断言失败时 panic，直接用于 `#[tokio::test]`。

use std::sync::Arc;

use bytes::Bytes;
use scion_object::{ObjectId, ObjectKind};

use crate::{
    BlobStore, ChangesetRecord, Error, IndexedObject, MetaStore, PathChange, PathHistoryEntry, RefExpect, RefState,
};

fn cs(n: u8) -> ObjectId {
    ObjectId::hash(ObjectKind::Changeset, &[n])
}

/// MetaStore 的语义：仓库生命周期、ref 的 CAS 与版本号。
pub async fn meta_store<S: MetaStore + ?Sized>(s: &S) {
    // 仓库
    assert_eq!(s.list_repos().await, Ok(vec![]));
    assert_eq!(s.repo_exists("b/two").await, Ok(false));
    s.create_repo("b/two").await.expect("create b/two");
    s.create_repo("a").await.expect("create a");
    assert_eq!(s.repo_exists("b/two").await, Ok(true));
    assert_eq!(s.list_repos().await, Ok(vec!["a".to_string(), "b/two".to_string()]));
    assert_eq!(s.create_repo("a").await, Err(Error::RepoAlreadyExists("a".into())));
    assert!(matches!(s.create_repo("../x").await, Err(Error::InvalidRepoName(_))));

    // 不存在的仓库
    assert_eq!(s.get_ref("nope", "main").await, Err(Error::RepoNotFound("nope".into())));
    assert_eq!(
        s.cas_ref("nope", "main", RefExpect::Absent, cs(1)).await,
        Err(Error::RepoNotFound("nope".into()))
    );

    // 创建 ref
    assert_eq!(s.get_ref("a", "main").await, Ok(None));
    assert_eq!(
        s.cas_ref("a", "main", RefExpect::Version(1), cs(1)).await,
        Err(Error::RefVersionMismatch {
            name: "main".into(),
            current: None
        })
    );
    let v1 = s
        .cas_ref("a", "main", RefExpect::Absent, cs(1))
        .await
        .expect("create ref");
    assert_eq!(
        v1,
        RefState {
            target: cs(1),
            version: 1
        }
    );
    assert_eq!(s.get_ref("a", "main").await, Ok(Some(v1)));

    // 重复创建、版本不符
    assert_eq!(
        s.cas_ref("a", "main", RefExpect::Absent, cs(2)).await,
        Err(Error::RefVersionMismatch {
            name: "main".into(),
            current: Some(1)
        })
    );
    assert_eq!(
        s.cas_ref("a", "main", RefExpect::Version(7), cs(2)).await,
        Err(Error::RefVersionMismatch {
            name: "main".into(),
            current: Some(1)
        })
    );

    // 更新：版本递增；即使 target 不变也递增
    let v2 = s
        .cas_ref("a", "main", RefExpect::Version(1), cs(2))
        .await
        .expect("update");
    assert_eq!(
        v2,
        RefState {
            target: cs(2),
            version: 2
        }
    );
    let v3 = s
        .cas_ref("a", "main", RefExpect::Version(2), cs(2))
        .await
        .expect("same target");
    assert_eq!(v3.version, 3);

    // 仓库之间互相隔离
    assert_eq!(s.get_ref("b/two", "main").await, Ok(None));
    // ref 名字按原样区分
    assert_eq!(s.get_ref("a", "Main").await, Ok(None));
}

/// BlobStore 的语义：全局、幂等写入、读取、存在性。
pub async fn blob_store<S: BlobStore + ?Sized>(s: &S) {
    let a = Bytes::from_static(b"alpha");
    let b = Bytes::from_static(b"");
    let ida = ObjectId::of_blob(&a);
    let idb = ObjectId::of_blob(&b);

    assert_eq!(s.get(&ida).await, Ok(None));
    assert_eq!(s.exists(&ida).await, Ok(false));

    s.put(ida, a.clone()).await.expect("put a");
    s.put(ida, a.clone()).await.expect("put a again is idempotent");
    s.put(idb, b.clone()).await.expect("put empty blob");

    assert_eq!(s.get(&ida).await, Ok(Some(a)));
    assert_eq!(s.get(&idb).await, Ok(Some(b)));
    assert_eq!(s.exists(&ida).await, Ok(true));

    // 字节与 id 不符：拒绝，且不留下任何对象
    let idc = ObjectId::of_blob(b"claimed");
    assert!(matches!(
        s.put(idc, Bytes::from_static(b"actual")).await,
        Err(Error::Backend(_))
    ));
    assert_eq!(s.exists(&idc).await, Ok(false));

    // 大对象（跨越常见缓冲区边界）
    let big = Bytes::from(vec![0x5a; 3 << 20]);
    let idbig = ObjectId::of_blob(&big);
    s.put(idbig, big.clone()).await.expect("put big");
    assert_eq!(s.get(&idbig).await, Ok(Some(big)));
}

/// 对象索引的语义：按仓库隔离、幂等登记、FindMissing。
pub async fn object_index<S: MetaStore + ?Sized>(s: &S) {
    s.create_repo("idx/a").await.expect("create");
    s.create_repo("idx/b").await.expect("create");
    let o = |d: &[u8]| IndexedObject {
        id: ObjectId::of_blob(d),
        size: d.len() as u64,
    };
    let (x, y, z) = (o(b"x"), o(b"y"), o(b"z"));

    assert_eq!(s.missing_objects("idx/a", &[]).await, Ok(vec![]));
    assert_eq!(s.missing_objects("idx/a", &[x.id, y.id]).await, Ok(vec![x.id, y.id]));
    s.index_objects("idx/a", &[x, y]).await.expect("index");
    s.index_objects("idx/a", &[x]).await.expect("index again is idempotent");
    s.index_objects("idx/a", &[]).await.expect("empty batch");
    // 保持顺序、去重
    assert_eq!(
        s.missing_objects("idx/a", &[z.id, x.id, z.id, y.id]).await,
        Ok(vec![z.id])
    );
    // 仓库之间互相隔离
    assert_eq!(s.missing_objects("idx/b", &[x.id]).await, Ok(vec![x.id]));
    // 一批内的重复条目
    s.index_objects("idx/b", &[z, z]).await.expect("duplicates in batch");
    assert_eq!(s.missing_objects("idx/b", &[z.id]).await, Ok(vec![]));

    assert!(matches!(
        s.index_objects("nope", &[x]).await,
        Err(Error::RepoNotFound(_))
    ));
    assert!(matches!(
        s.missing_objects("nope", &[x.id]).await,
        Err(Error::RepoNotFound(_))
    ));
}

/// changeset 记录与 ref 列表的语义。
pub async fn changesets_and_refs<S: MetaStore + ?Sized>(s: &S) {
    s.create_repo("cs/a").await.expect("create");
    s.create_repo("cs/b").await.expect("create");
    let tree = ObjectId::hash(ObjectKind::Tree, b"t");
    let root = ChangesetRecord {
        id: cs(1),
        root_tree: tree,
        parents: vec![],
        generation: 1,
        fp_depth: 0,
        fp_jump: None,
    };
    // 合并提交：多个父提交，顺序必须保留
    let child = ChangesetRecord {
        id: cs(2),
        root_tree: tree,
        parents: vec![cs(1), cs(9), cs(5)],
        generation: u64::from(u32::MAX) + 7,
        fp_depth: 2,
        fp_jump: Some(cs(1)),
    };

    assert_eq!(s.get_changeset("cs/a", &cs(1)).await, Ok(None));
    s.put_changeset("cs/a", &root).await.expect("put root");
    s.put_changeset("cs/a", &child).await.expect("put child");
    assert_eq!(s.get_changeset("cs/a", &cs(1)).await, Ok(Some(root.clone())));
    assert_eq!(s.get_changeset("cs/a", &cs(2)).await, Ok(Some(child.clone())));
    assert_eq!(child.first_parent(), Some(cs(1)));
    assert_eq!(root.first_parent(), None);
    // 幂等：保留第一次写入
    let other = ChangesetRecord {
        root_tree: ObjectId::hash(ObjectKind::Tree, b"other"),
        ..root.clone()
    };
    s.put_changeset("cs/a", &other).await.expect("idempotent put");
    assert_eq!(s.get_changeset("cs/a", &cs(1)).await, Ok(Some(root.clone())));
    // 仓库隔离
    assert_eq!(s.get_changeset("cs/b", &cs(1)).await, Ok(None));
    assert!(matches!(
        s.put_changeset("nope", &root).await,
        Err(Error::RepoNotFound(_))
    ));
    assert!(matches!(
        s.get_changeset("nope", &cs(1)).await,
        Err(Error::RepoNotFound(_))
    ));

    // ref 列表：字节序
    assert_eq!(s.list_refs("cs/a").await, Ok(vec![]));
    for name in ["main", "Zeta", "feature/x"] {
        s.cas_ref("cs/a", name, RefExpect::Absent, cs(1))
            .await
            .expect("create ref");
    }
    s.cas_ref("cs/a", "main", RefExpect::Version(1), cs(2))
        .await
        .expect("update");
    let refs = s.list_refs("cs/a").await.expect("list");
    let names: Vec<_> = refs.iter().map(|(n, _)| n.as_str()).collect();
    assert_eq!(names, ["Zeta", "feature/x", "main"]);
    assert_eq!(
        refs[2].1,
        RefState {
            target: cs(2),
            version: 2
        }
    );
    assert_eq!(s.list_refs("cs/b").await, Ok(vec![]));
    assert!(matches!(s.list_refs("nope").await, Err(Error::RepoNotFound(_))));
}

/// 派生数据：按 (kind, version, key) 存取，幂等，仓库隔离。
pub async fn derived<S: MetaStore + ?Sized>(s: &S) {
    s.create_repo("der/a").await.expect("create");
    s.create_repo("der/b").await.expect("create");
    let big = vec![7u8; 100_000];
    assert_eq!(s.get_derived("der/a", "blame", 1, b"k").await, Ok(None));
    s.put_derived("der/a", "blame", 1, b"k", b"v1").await.expect("put");
    s.put_derived("der/a", "blame", 1, b"k", b"other")
        .await
        .expect("idempotent");
    assert_eq!(s.get_derived("der/a", "blame", 1, b"k").await, Ok(Some(b"v1".to_vec())));
    // 版本、种类、键都参与区分
    assert_eq!(s.get_derived("der/a", "blame", 2, b"k").await, Ok(None));
    assert_eq!(s.get_derived("der/a", "other", 1, b"k").await, Ok(None));
    assert_eq!(s.get_derived("der/a", "blame", 1, b"k2").await, Ok(None));
    assert_eq!(s.get_derived("der/b", "blame", 1, b"k").await, Ok(None));
    // 空键、空值、大值、二进制键
    s.put_derived("der/a", "x", 1, b"", b"").await.expect("empty");
    assert_eq!(s.get_derived("der/a", "x", 1, b"").await, Ok(Some(vec![])));
    s.put_derived("der/a", "x", 1, &[0, 255, 0], &big).await.expect("big");
    assert_eq!(s.get_derived("der/a", "x", 1, &[0, 255, 0]).await, Ok(Some(big)));
    // 批量读取：顺序与输入一致，缺失为 None，重复键各自返回
    let keys = vec![b"k2".to_vec(), b"k".to_vec(), vec![], b"k".to_vec()];
    assert_eq!(
        s.get_derived_many("der/a", "blame", 1, &keys).await,
        Ok(vec![None, Some(b"v1".to_vec()), None, Some(b"v1".to_vec())])
    );
    assert_eq!(s.get_derived_many("der/a", "blame", 1, &[]).await, Ok(vec![]));
    let many: Vec<Vec<u8>> = (0..2000u32).map(|i| i.to_be_bytes().to_vec()).collect();
    for k in many.iter().step_by(7) {
        s.put_derived("der/a", "many", 1, k, k).await.expect("put");
    }
    let got = s.get_derived_many("der/a", "many", 1, &many).await.expect("many");
    for (i, (k, v)) in many.iter().zip(&got).enumerate() {
        assert_eq!(v.as_ref(), (i % 7 == 0).then_some(k), "key {i}");
    }
    assert!(matches!(
        s.get_derived_many("nope", "x", 1, &keys).await,
        Err(Error::RepoNotFound(_))
    ));
    assert!(matches!(
        s.get_derived("nope", "x", 1, b"k").await,
        Err(Error::RepoNotFound(_))
    ));
    assert!(matches!(
        s.put_derived("nope", "x", 1, b"k", b"v").await,
        Err(Error::RepoNotFound(_))
    ));
}

/// 批量读取提交记录与 PathHistory 索引。
pub async fn history_index<S: MetaStore + ?Sized>(s: &S) {
    s.create_repo("hist/a").await.expect("create");
    s.create_repo("hist/b").await.expect("create");
    let tree = ObjectId::hash(ObjectKind::Tree, b"t");
    for g in 1..=5u8 {
        let rec = ChangesetRecord {
            id: cs(g),
            root_tree: tree,
            parents: vec![],
            generation: u64::from(g),
            fp_depth: 0,
            fp_jump: None,
        };
        s.put_changeset("hist/a", &rec).await.expect("put");
    }
    let gens = |v: Vec<ChangesetRecord>| v.into_iter().map(|r| r.generation).collect::<Vec<_>>();
    assert_eq!(
        gens(s.changesets_below("hist/a", 4, 2).await.expect("below")),
        vec![4, 3]
    );
    assert_eq!(
        gens(s.changesets_below("hist/a", 100, 100).await.expect("below")),
        vec![5, 4, 3, 2, 1]
    );
    assert_eq!(
        gens(s.changesets_below("hist/a", 0, 10).await.expect("below")),
        Vec::<u64>::new()
    );
    assert_eq!(s.changesets_below("hist/b", 10, 10).await.expect("below"), vec![]);
    // 大代数（超过 i32）
    let big = ChangesetRecord {
        id: cs(9),
        root_tree: tree,
        parents: vec![cs(5)],
        generation: u64::from(u32::MAX) + 1,
        fp_depth: 0,
        fp_jump: None,
    };
    s.put_changeset("hist/a", &big).await.expect("put");
    assert_eq!(
        s.changesets_below("hist/a", u64::from(u32::MAX) + 1, 1)
            .await
            .expect("below"),
        vec![big]
    );

    let ch = |p: &str, deleted| PathChange {
        path: p.into(),
        deleted,
    };
    s.index_paths("hist/a", cs(1), 1, &[ch("src/a.rs", false), ch("README", false)])
        .await
        .expect("index");
    s.index_paths("hist/a", cs(2), 2, &[ch("src/a.rs", false), ch("src/b/c.rs", false)])
        .await
        .expect("index");
    s.index_paths(
        "hist/a",
        cs(3),
        3,
        &[ch("src/a.rs", true), ch("srcx/d.rs", false), ch("src.txt", false)],
    )
    .await
    .expect("index");
    s.index_paths("hist/a", cs(3), 3, &[ch("src/a.rs", true)])
        .await
        .expect("idempotent");
    s.index_paths("hist/a", cs(4), 4, &[]).await.expect("empty");
    let hist = |v: Vec<PathHistoryEntry>| {
        v.into_iter()
            .map(|e| (e.generation, e.path, e.deleted))
            .collect::<Vec<_>>()
    };
    assert_eq!(
        hist(s.path_history("hist/a", "src/a.rs", 10, 10).await.expect("file")),
        vec![
            (3, "src/a.rs".into(), true),
            (2, "src/a.rs".into(), false),
            (1, "src/a.rs".into(), false)
        ]
    );
    assert_eq!(
        hist(s.path_history("hist/a", "src", 10, 10).await.expect("dir")),
        vec![
            (3, "src/a.rs".into(), true),
            (2, "src/a.rs".into(), false),
            (2, "src/b/c.rs".into(), false),
            (1, "src/a.rs".into(), false)
        ],
        "directory matches descendants only, not src.txt or srcx/"
    );
    assert_eq!(s.path_history("hist/a", "src", 2, 1).await.expect("limit").len(), 1);
    assert_eq!(
        hist(s.path_history("hist/a", "src/a.rs", 2, 10).await.expect("max gen")).len(),
        2
    );
    assert!(s.path_history("hist/a", "nope", 10, 10).await.expect("none").is_empty());
    assert!(
        s.path_history("hist/b", "src", 10, 10)
            .await
            .expect("isolated")
            .is_empty()
    );
    assert!(matches!(
        s.path_history("nope", "src", 1, 1).await,
        Err(Error::RepoNotFound(_))
    ));
    assert!(matches!(
        s.index_paths("nope", cs(1), 1, &[]).await,
        Err(Error::RepoNotFound(_))
    ));
    assert!(matches!(
        s.changesets_below("nope", 1, 1).await,
        Err(Error::RepoNotFound(_))
    ));
}

/// 第一父提交链跳表：线性历史上 fp_skip 的深度与跳跃指针满足 skew-binary 规则。
pub async fn fp_skip_chain<S: MetaStore>(s: &S) {
    s.create_repo("skip").await.expect("create");
    let tree = ObjectId::hash(ObjectKind::Tree, b"t");
    let mut prev: Option<ObjectId> = None;
    let mut recs = Vec::new();
    for n in 0..40u8 {
        let (fp_depth, fp_jump) = crate::fp_skip(s, "skip", prev).await.expect("skip");
        let rec = ChangesetRecord {
            id: cs(100 + n),
            root_tree: tree,
            parents: prev.into_iter().collect(),
            generation: u64::from(n) + 1,
            fp_depth,
            fp_jump,
        };
        s.put_changeset("skip", &rec).await.expect("put");
        prev = Some(rec.id);
        recs.push(rec);
    }
    assert_eq!((recs[0].fp_depth, recs[0].fp_jump), (1, None), "root jumps to itself");
    let depth_of = |id: ObjectId| recs.iter().find(|r| r.id == id).map(|r| r.fp_depth).expect("known");
    for r in &recs[1..] {
        assert_eq!(r.fp_depth, r.generation);
        let j = depth_of(r.fp_jump.expect("jump"));
        assert!(j < r.fp_depth, "jump goes strictly up the chain");
    }
    // 跳跃长度覆盖多个尺度（skew-binary：1, 3, 7, 15, …）
    let longest = recs
        .iter()
        .skip(1)
        .map(|r| r.fp_depth - depth_of(r.fp_jump.expect("jump")))
        .max()
        .expect("max");
    assert!(longest >= 15, "longest jump {longest}");
    // 父提交深度未知：结果也未知
    let legacy = ChangesetRecord {
        id: cs(250),
        root_tree: tree,
        parents: vec![],
        generation: 1,
        fp_depth: 0,
        fp_jump: None,
    };
    s.put_changeset("skip", &legacy).await.expect("put");
    assert_eq!(crate::fp_skip(s, "skip", Some(cs(250))).await, Ok((0, None)));
    // 补写：只在深度未知时生效
    s.set_fp_skip("skip", &cs(250), 1, None).await.expect("backfill");
    assert_eq!(
        s.get_changeset("skip", &cs(250)).await.expect("ok").map(|r| r.fp_depth),
        Some(1)
    );
    s.set_fp_skip("skip", &cs(250), 9, Some(cs(1))).await.expect("no-op");
    let r = s.get_changeset("skip", &cs(250)).await.expect("ok").expect("rec");
    assert_eq!(
        (r.fp_depth, r.fp_jump),
        (1, None),
        "existing values are never overwritten"
    );
    s.set_fp_skip("skip", &cs(251), 1, None)
        .await
        .expect("unknown changeset is a no-op");
    assert!(matches!(
        s.set_fp_skip("nope", &cs(250), 1, None).await,
        Err(Error::RepoNotFound(_))
    ));
    assert!(
        matches!(crate::fp_skip(s, "skip", Some(cs(251))).await, Err(Error::Backend(_))),
        "parent not recorded"
    );
}

/// 并发 CAS：同一版本上的多个竞争者恰好只有一个成功，且版本严格连续。
pub async fn concurrent_cas<S: MetaStore>(s: Arc<S>) {
    const WRITERS: u8 = 16;
    const ROUNDS: u64 = 20;
    s.create_repo("race").await.expect("create repo");
    s.cas_ref("race", "main", RefExpect::Absent, cs(0))
        .await
        .expect("create ref");

    for round in 1..=ROUNDS {
        let mut tasks = Vec::new();
        for w in 0..WRITERS {
            let s = s.clone();
            tasks.push(tokio::spawn(async move {
                s.cas_ref("race", "main", RefExpect::Version(round), cs(w)).await
            }));
        }
        let mut ok = 0;
        for t in tasks {
            match t.await.expect("task") {
                Ok(state) => {
                    assert_eq!(state.version, round + 1);
                    ok += 1;
                }
                Err(Error::RefVersionMismatch { current, .. }) => assert_eq!(current, Some(round + 1)),
                Err(e) => panic!("unexpected error {e}"),
            }
        }
        assert_eq!(ok, 1, "round {round}: exactly one writer must win");
    }
    let last = s.get_ref("race", "main").await.expect("get").expect("exists");
    assert_eq!(last.version, ROUNDS + 1);
}
