//! redaction 端到端：权限、读取返回 REDACTED、合入时不合并 redact 过的内容、撤销、物理删除。

use std::sync::Arc;
use std::time::Duration;

use scion_api::scion_auth::{AuthConfig, Guard, GuardConfig};
use scion_api::{App, ServerConfig, serve};
use scion_client::{Auth, Client, Code};
use scion_proto::v1::admin_service_client::AdminServiceClient;
use scion_proto::v1::{ListRedactionsRequest, PurgeRequest, RedactRequest, UnredactRequest};
use scion_store::{BlobStore, MemStore, MetaStore};
use tokio::net::TcpListener;
use tonic::metadata::MetadataValue;

const POLICY: &str = r#"
[[rule]]
subjects = ["user:root"]
repos = ["*"]
actions = ["admin"]

[[rule]]
subjects = ["user:owner"]
repos = ["mono"]
actions = ["admin"]

[[rule]]
subjects = ["user:dev"]
repos = ["mono"]
actions = ["land"]
"#;

struct Server {
    url: String,
    store: Arc<MemStore>,
    guard: Arc<Guard>,
    _stop: tokio::sync::oneshot::Sender<()>,
}

async fn start() -> Server {
    let store = Arc::new(MemStore::new());
    store.create_repo("mono").await.expect("repo");
    store.create_repo("fork").await.expect("repo");
    let auth = GuardConfig {
        required: true,
        auth: AuthConfig::default(),
        signing_key_pem: None,
    };
    let guard = Guard::build(auth.clone(), store.clone(), Arc::new(|_: &str| None)).expect("guard");
    guard.set_policy(POLICY).await.expect("policy");
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let url = format!("http://{}", listener.local_addr().expect("addr"));
    let app = App {
        config: Arc::new(ServerConfig {
            auth,
            warm_record_cache: false,
            janitor_interval: Duration::from_millis(50),
            ..ServerConfig::default()
        }),
        meta: store.clone(),
        blobs: store.clone(),
    };
    let (tx, rx) = tokio::sync::oneshot::channel::<()>();
    tokio::spawn(serve(listener, app, async {
        let _ = rx.await;
    }));
    Server {
        url,
        store,
        guard,
        _stop: tx,
    }
}

async fn token(s: &Server, subject: &str) -> String {
    s.guard
        .create_token("pat", subject, vec![], "test", None)
        .await
        .expect("token")
        .0
}

async fn client(s: &Server, token: &str) -> Client {
    Client::builder()
        .endpoint(&s.url)
        .repo("mono")
        .auth(Auth::Bearer(token.into()))
        .build()
        .await
        .expect("connect")
}

/// 给每个请求加上 `authorization`。
#[derive(Clone)]
struct Bearer(MetadataValue<tonic::metadata::Ascii>);

impl tonic::service::Interceptor for Bearer {
    fn call(&mut self, mut r: tonic::Request<()>) -> Result<tonic::Request<()>, tonic::Status> {
        r.metadata_mut().insert("authorization", self.0.clone());
        Ok(r)
    }
}

type Admin = AdminServiceClient<tonic::service::interceptor::InterceptedService<tonic::transport::Channel, Bearer>>;

async fn admin(s: &Server, token: &str) -> Admin {
    let channel = tonic::transport::Endpoint::from_shared(s.url.clone())
        .expect("url")
        .connect()
        .await
        .expect("connect");
    let value = MetadataValue::try_from(format!("Bearer {token}")).expect("header");
    AdminServiceClient::with_interceptor(channel, Bearer(value))
}

#[tokio::test(flavor = "multi_thread")]
async fn redaction_lifecycle() {
    let s = start().await;
    let (root_t, owner_t, dev_t) = (
        token(&s, "root").await,
        token(&s, "owner").await,
        token(&s, "dev").await,
    );
    let dev = client(&s, &dev_t).await;
    assert!(dev.has_feature(scion_proto::features::REDACTION));

    let a = dev
        .changeset(None)
        .put("secret.txt", "line 1\npassword=hunter2\nline 3\n")
        .put("README", "hello")
        .create()
        .await
        .expect("create");
    dev.land("main", &a.id).await.expect("land");
    let secret = scion_client::ObjectId::of_blob(b"line 1\npassword=hunter2\nline 3\n");

    // 权限：开发者不能 redact；仓库管理员不能 redact 所有仓库
    let req = |all_repos| RedactRequest {
        repo: "mono".into(),
        ids: vec![],
        paths: vec!["main:secret.txt".into()],
        reason: "leaked credential".into(),
        all_repos,
    };
    let e = admin(&s, &dev_t).await.redact(req(false)).await.expect_err("dev");
    assert_eq!(e.code(), Code::PermissionDenied);
    let e = admin(&s, &owner_t)
        .await
        .redact(req(true))
        .await
        .expect_err("all repos");
    assert_eq!(e.code(), Code::PermissionDenied);

    let resp = admin(&s, &owner_t)
        .await
        .redact(req(false))
        .await
        .expect("redact")
        .into_inner();
    assert!(resp.redaction_id.starts_with("rd_"));
    assert_eq!(resp.redactions.len(), 1);
    let info = &resp.redactions[0];
    assert_eq!(
        (
            info.repo.as_str(),
            info.created_by.as_str(),
            info.active,
            info.id.as_ref()
        ),
        ("mono", "owner", true, secret.to_bytes().as_slice())
    );
    let rid = resp.redaction_id;

    // 读取返回 REDACTED；目录与其它文件不受影响
    let e = dev.get_object(&secret).await.expect_err("get");
    assert!(e.is_redacted(), "{e:?}");
    let e = dev.read_file("main", "secret.txt").await.expect_err("read");
    assert!(e.is_redacted(), "{e:?}");
    assert_eq!(dev.read_file("main", "README").await.expect("readme"), "hello");
    assert_eq!(dev.list_dir("main", "").await.expect("ls").len(), 2);
    let page = dev.list_redacted_objects(0).await.expect("feed");
    assert_eq!(page.objects.len(), 1);
    assert_eq!((page.objects[0].id, page.objects[0].active), (secret, true));
    assert!(!page.more && page.next_seq == page.objects[0].seq);

    // rebase 需要三方合并 redact 过的版本：拒绝，不把内容合并进新提交
    let theirs = dev
        .changeset(Some(a.id))
        .put("secret.txt", "line 1\npassword=hunter2\nline 3 changed\n")
        .create()
        .await
        .expect("theirs");
    let ours = dev
        .changeset(Some(a.id))
        .put("secret.txt", "line 1 changed\npassword=hunter2\nline 3\n")
        .create()
        .await
        .expect("ours");
    dev.land("main", &theirs.id).await.expect("fast-forward");
    let e = dev.land("main", &ours.id).await.expect_err("merge");
    assert!(e.is_redacted(), "{e:?}");

    // 撤销
    let n = admin(&s, &owner_t)
        .await
        .unredact(UnredactRequest {
            redaction_id: rid.clone(),
        })
        .await
        .expect("unredact")
        .into_inner()
        .count;
    assert_eq!(n, 1);
    assert_eq!(
        dev.get_object(&secret).await.expect("readable again").as_ref(),
        b"line 1\npassword=hunter2\nline 3\n"
    );

    // 管理员 redact 所有仓库并立即清除
    s.store
        .index_objects("fork", &[scion_store::IndexedObject { id: secret, size: 31 }])
        .await
        .expect("fork index");
    let mut root = admin(&s, &root_t).await;
    // main 已经前进：按 id 指定最初的版本
    let resp = root
        .redact(RedactRequest {
            ids: vec![bytes::Bytes::copy_from_slice(&secret.to_bytes())],
            paths: vec![],
            ..req(true)
        })
        .await
        .expect("all repos")
        .into_inner();
    assert_eq!(resp.redactions.len(), 2);
    let e = admin(&s, &owner_t)
        .await
        .purge(PurgeRequest {
            redaction_id: resp.redaction_id.clone(),
            grace_seconds: Some(0),
        })
        .await
        .expect_err("fork is not owner's");
    assert_eq!(e.code(), Code::PermissionDenied);
    let purge = root
        .purge(PurgeRequest {
            redaction_id: resp.redaction_id.clone(),
            grace_seconds: Some(0),
        })
        .await
        .expect("purge")
        .into_inner();
    assert_eq!(purge.count, 2);
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while s.store.exists(&secret).await.expect("exists") {
        assert!(tokio::time::Instant::now() < deadline, "janitor never purged");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let listed = root
        .list_redactions(ListRedactionsRequest {
            repo: String::new(),
            redaction_id: resp.redaction_id.clone(),
        })
        .await
        .expect("list")
        .into_inner()
        .redactions;
    assert_eq!(listed.len(), 2);
    assert!(listed.iter().all(|r| r.purged_at_ms > 0 && r.purge_after_ms > 0));
    let e = root
        .unredact(UnredactRequest {
            redaction_id: resp.redaction_id,
        })
        .await
        .expect_err("purged");
    assert_eq!(e.code(), Code::FailedPrecondition);

    // 列出：不限仓库需要系统管理员
    let e = admin(&s, &owner_t)
        .await
        .list_redactions(ListRedactionsRequest::default())
        .await
        .expect_err("owner");
    assert_eq!(e.code(), Code::PermissionDenied);
    let mine = admin(&s, &owner_t)
        .await
        .list_redactions(ListRedactionsRequest {
            repo: "mono".into(),
            redaction_id: String::new(),
        })
        .await
        .expect("own repo")
        .into_inner()
        .redactions;
    assert_eq!(mine.len(), 1, "one record per (repo, object)");
    let e = admin(&s, &owner_t)
        .await
        .unredact(UnredactRequest {
            redaction_id: "rd_missing".into(),
        })
        .await
        .expect_err("missing");
    assert_eq!(e.code(), Code::NotFound);
}
