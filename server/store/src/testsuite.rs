//! 存储后端一致性测试套件。每个后端（内存、SQLite、PostgreSQL、YugabyteDB、S3……）
//! 都必须在自己的测试中调用这里的全部函数并通过。
//!
//! 函数在断言失败时 panic，直接用于 `#[tokio::test]`。

use std::sync::Arc;

use bytes::Bytes;
use scion_object::{ObjectId, ObjectKind};

use crate::{BlobStore, Error, MetaStore, RefExpect, RefState};

/// [`blob_store`] 使用的仓库名，调用方需事先创建。
pub const REPO: &str = "conformance/repo";

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

/// BlobStore 的语义：幂等写入、读取、FindMissing。
pub async fn blob_store<S: BlobStore + ?Sized>(s: &S) {
    let a = Bytes::from_static(b"alpha");
    let b = Bytes::from_static(b"");
    let ida = ObjectId::of_blob(&a);
    let idb = ObjectId::of_blob(&b);
    let idc = ObjectId::of_blob(b"never stored");

    assert_eq!(s.get(REPO, &ida).await, Ok(None));
    assert_eq!(s.missing(REPO, &[]).await, Ok(vec![]));
    assert_eq!(s.missing(REPO, &[ida, idb]).await, Ok(vec![ida, idb]));

    s.put(REPO, ida, a.clone()).await.expect("put a");
    s.put(REPO, ida, a.clone()).await.expect("put a again is idempotent");
    s.put(REPO, idb, b.clone()).await.expect("put empty blob");

    assert_eq!(s.get(REPO, &ida).await, Ok(Some(a)));
    assert_eq!(s.get(REPO, &idb).await, Ok(Some(b)));
    // 保持顺序、去重
    assert_eq!(s.missing(REPO, &[idc, ida, idc, idb]).await, Ok(vec![idc]));

    // 不存在的仓库
    assert!(matches!(s.get("nope", &ida).await, Err(Error::RepoNotFound(_))));
    assert!(matches!(
        s.put("nope", ida, Bytes::new()).await,
        Err(Error::RepoNotFound(_))
    ));
    assert!(matches!(s.missing("nope", &[ida]).await, Err(Error::RepoNotFound(_))));
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
