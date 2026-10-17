//! SDK 与要求认证的服务端：用长期凭据换取访问令牌并自动续期、旧服务端回退为直接发送凭据、device login。

use std::sync::Arc;
use std::time::Duration;

use scion_api::scion_auth::testing::FakeIdp;
use scion_api::scion_auth::{AuthConfig, Guard, GuardConfig};
use scion_api::{App, ServerConfig};
use scion_client::{Auth, Client, DeviceLoginPoll};
use scion_store::{MemStore, MetaStore};
use tokio::net::TcpListener;

struct Server {
    url: String,
    guard: Arc<Guard>,
    _stop: tokio::sync::oneshot::Sender<()>,
}

async fn start(auth_toml: &str, signing_key: bool) -> Server {
    let store = Arc::new(MemStore::new());
    store.create_repo("mono").await.expect("repo");
    let auth = GuardConfig {
        required: true,
        auth: AuthConfig::parse(auth_toml).expect("auth config"),
        signing_key_pem: signing_key.then(scion_api::scion_auth::token::generate_signing_key),
    };
    let guard = Guard::build(auth.clone(), store.clone(), Arc::new(|_: &str| None)).expect("guard");
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let url = format!("http://{}", listener.local_addr().expect("addr"));
    let app = App {
        config: Arc::new(ServerConfig {
            auth,
            ..ServerConfig::default()
        }),
        meta: store.clone(),
        blobs: store,
    };
    let (tx, rx) = tokio::sync::oneshot::channel::<()>();
    tokio::spawn(scion_api::serve(listener, app, async {
        let _ = rx.await;
    }));
    Server { url, guard, _stop: tx }
}

async fn connect(s: &Server, auth: Auth) -> scion_client::Result<Client> {
    Client::builder().endpoint(&s.url).repo("mono").auth(auth).build().await
}

#[tokio::test(flavor = "multi_thread")]
async fn exchange_refreshes_access_tokens() {
    // 访问令牌只有 2 秒：后台在过期前重新换取
    let s = start("[token]\nttl = \"2s\"\n", true).await;
    let (pat, _) = s.guard.create_token("pat", "ci", vec![], "", None).await.expect("pat");
    let c = connect(&s, Auth::Exchange(pat.clone())).await.expect("connect");
    let first = c.token_expires_at_ms().expect("exchanged");
    let me = c.who_am_i().await.expect("whoami");
    assert_eq!((me.subject.as_str(), me.auth_required), ("ci", true));
    assert!(
        me.expires_at_ms.is_some(),
        "authenticated with the short-lived access token"
    );
    tokio::time::sleep(Duration::from_millis(3500)).await;
    let later = c.token_expires_at_ms().expect("still exchanged");
    assert!(later > first, "refreshed in the background");
    c.list_refs()
        .await
        .expect("requests keep working after the first token expired");
    // 凭据无效：连接失败
    let err = connect(&s, Auth::Exchange("scn_pat_nope".into()))
        .await
        .expect_err("invalid");
    assert!(
        matches!(
            err,
            scion_client::Error::Rpc {
                code: tonic::Code::Unauthenticated,
                ..
            }
        ),
        "{err}"
    );
    // 匿名
    let anon = connect(&s, Auth::None).await.expect("connect");
    assert_eq!(anon.who_am_i().await.expect("whoami").subject, "anonymous");
    assert!(anon.list_refs().await.is_err());
}

#[tokio::test(flavor = "multi_thread")]
async fn without_a_signing_key_credentials_are_sent_directly() {
    let s = start("", false).await;
    let (pat, _) = s.guard.create_token("pat", "ci", vec![], "", None).await.expect("pat");
    let c = connect(&s, Auth::Exchange(pat)).await.expect("connect");
    assert_eq!(c.token_expires_at_ms(), None, "server does not issue access tokens");
    assert_eq!(c.who_am_i().await.expect("whoami").subject, "ci");
}

#[tokio::test(flavor = "multi_thread")]
async fn device_login_yields_a_refresh_token() {
    let idp = FakeIdp::start().await;
    let toml = format!(
        "[oidc]\nissuer = \"{}\"\nclient_id = \"scn\"\nsubject_claim = \"preferred_username\"\ngroups_claim = \"groups\"\n",
        idp.base()
    );
    let s = start(&toml, true).await;
    let anon = connect(&s, Auth::None).await.expect("connect");
    assert!(anon.has_feature(scion_proto::features::AUTH_DEVICE));
    let login = anon.start_device_login().await.expect("start");
    assert_eq!(login.user_code, "ABCD-EFGH");
    assert!(login.verification_uri.ends_with("/activate"));
    assert_eq!(
        anon.poll_device_login(&login.device_code).await.expect("poll"),
        DeviceLoginPoll::Pending
    );
    assert_eq!(
        anon.poll_device_login("slow").await.expect("poll"),
        DeviceLoginPoll::SlowDown
    );
    assert_eq!(
        anon.poll_device_login("denied").await.expect("poll"),
        DeviceLoginPoll::Denied
    );
    assert_eq!(
        anon.poll_device_login("expired").await.expect("poll"),
        DeviceLoginPoll::Expired
    );
    idp.approve(&login.device_code);
    let DeviceLoginPoll::Complete { subject, refresh_token } =
        anon.poll_device_login(&login.device_code).await.expect("poll")
    else {
        panic!("expected completion");
    };
    assert_eq!(subject, "alice");
    let c = connect(&s, Auth::Exchange(refresh_token))
        .await
        .expect("refresh token works");
    let me = c.who_am_i().await.expect("whoami");
    assert_eq!(
        (me.subject.as_str(), me.groups.as_slice()),
        ("alice", &["eng".to_string()][..])
    );
}
