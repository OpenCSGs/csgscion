//! RefService：解析、列出与订阅 ref。

use std::collections::{HashMap, HashSet};
use std::pin::Pin;
use std::sync::Arc;

use bytes::Bytes;
use scion_proto::v1::ref_service_server::RefService;
use scion_proto::v1::{
    ListRefsRequest, ListRefsResponse, RefEvent, RefInfo, ResolveRefRequest, ResolveRefResponse, WatchRefsRequest,
    WatchRefsResponse,
};
use scion_store::MetaStore;
use tokio::sync::{broadcast, mpsc};
use tokio_stream::Stream;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};

use crate::service::support::{check_repo, resolve_revision};
use crate::status;
use crate::watch::{RefHub, RefUpdate};

pub type WatchRefsStream = Pin<Box<dyn Stream<Item = Result<WatchRefsResponse, Status>> + Send>>;

pub struct RefSvc {
    meta: Arc<dyn MetaStore>,
    hub: Arc<RefHub>,
}

impl RefSvc {
    pub fn new(meta: Arc<dyn MetaStore>, hub: Arc<RefHub>) -> Self {
        Self { meta, hub }
    }
}

fn event(u: &RefUpdate) -> RefEvent {
    RefEvent {
        name: u.name.clone(),
        changeset_id: Bytes::copy_from_slice(&u.target.to_bytes()),
        root_tree: u
            .root_tree
            .map(|t| Bytes::copy_from_slice(&t.to_bytes()))
            .unwrap_or_default(),
        version: u.version,
    }
}

/// 一个订阅者的过滤状态：只关心的 ref，以及每个 ref 已发送的版本。
struct Filter {
    refs: HashSet<String>,
    sent: HashMap<String, u64>,
}

impl Filter {
    /// 是否需要发送；需要时记下版本。
    fn take(&mut self, u: &RefUpdate) -> bool {
        if !self.refs.is_empty() && !self.refs.contains(&u.name) {
            return false;
        }
        let sent = self.sent.entry(u.name.clone()).or_insert(0);
        if u.version <= *sent {
            return false;
        }
        *sent = u.version;
        true
    }

    fn take_all(&mut self, updates: &[RefUpdate]) -> Vec<RefEvent> {
        updates.iter().filter(|u| self.take(u)).map(event).collect()
    }
}

/// 订阅者的发送循环：通道里的变化逐批发送（同时到达的合并成一条响应），落后太多时重新取快照。
async fn pump(
    hub: Arc<RefHub>,
    repo: String,
    mut filter: Filter,
    mut rx: broadcast::Receiver<RefUpdate>,
    out: mpsc::Sender<Result<WatchRefsResponse, Status>>,
) {
    loop {
        let mut events = Vec::new();
        let first = tokio::select! {
            r = rx.recv() => r,
            () = out.closed() => return,
            () = hub.wait_closed() => return,
        };
        let mut next = Some(first);
        while let Some(r) = next.take() {
            match r {
                Ok(u) => {
                    if filter.take(&u) {
                        events.push(event(&u));
                    }
                }
                Err(broadcast::error::RecvError::Lagged(_)) => match hub.snapshot(&repo).await {
                    Ok(snap) => events.extend(filter.take_all(&snap)),
                    Err(e) => {
                        let _ = out.send(Err(status::from_store(e))).await;
                        return;
                    }
                },
                Err(broadcast::error::RecvError::Closed) => return,
            }
            next = match rx.try_recv() {
                Ok(u) => Some(Ok(u)),
                Err(broadcast::error::TryRecvError::Lagged(n)) => Some(Err(broadcast::error::RecvError::Lagged(n))),
                Err(_) => None,
            };
        }
        if !events.is_empty()
            && out
                .send(Ok(WatchRefsResponse {
                    events,
                    snapshot: false,
                }))
                .await
                .is_err()
        {
            return;
        }
    }
}

#[tonic::async_trait]
impl RefService for RefSvc {
    type WatchRefsStream = WatchRefsStream;

    async fn resolve_ref(&self, request: Request<ResolveRefRequest>) -> Result<Response<ResolveRefResponse>, Status> {
        let req = request.into_inner();
        let state = self
            .meta
            .get_ref(&req.repo, &req.r#ref)
            .await
            .map_err(status::from_store)?
            .ok_or_else(|| status::ref_not_found(&req.r#ref))?;
        let rec = resolve_revision(self.meta.as_ref(), &req.repo, &state.target.to_string()).await?;
        Ok(Response::new(ResolveRefResponse {
            changeset_id: Bytes::copy_from_slice(&rec.id.to_bytes()),
            root_tree: Bytes::copy_from_slice(&rec.root_tree.to_bytes()),
            version: state.version,
        }))
    }

    async fn list_refs(&self, request: Request<ListRefsRequest>) -> Result<Response<ListRefsResponse>, Status> {
        let req = request.into_inner();
        check_repo(self.meta.as_ref(), &req.repo).await?;
        let refs = self.meta.list_refs(&req.repo).await.map_err(status::from_store)?;
        Ok(Response::new(ListRefsResponse {
            refs: refs
                .into_iter()
                .map(|(name, s)| RefInfo {
                    name,
                    changeset_id: Bytes::copy_from_slice(&s.target.to_bytes()),
                    version: s.version,
                })
                .collect(),
        }))
    }

    async fn watch_refs(&self, request: Request<WatchRefsRequest>) -> Result<Response<Self::WatchRefsStream>, Status> {
        let req = request.into_inner();
        check_repo(self.meta.as_ref(), &req.repo).await?;
        // 先订阅再取快照：两者之间的变化会出现在通道里，按版本去重
        let rx = self.hub.subscribe(&req.repo);
        let snap = self.hub.snapshot(&req.repo).await.map_err(status::from_store)?;
        let mut filter = Filter {
            refs: req.refs.into_iter().collect(),
            sent: req.known_versions,
        };
        let events = filter.take_all(&snap);
        let (tx, out) = mpsc::channel(16);
        let _ = tx.send(Ok(WatchRefsResponse { events, snapshot: true })).await;
        tokio::spawn(pump(self.hub.clone(), req.repo, filter, rx, tx));
        Ok(Response::new(Box::pin(ReceiverStream::new(out))))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::service::land::LandSvc;
    use crate::service::write::spec_to_changeset;
    use crate::service::write::tests::{REPO, spec, svc};
    use scion_object::ObjectId;
    use tonic::Code;

    #[tokio::test]
    async fn resolve_and_list() {
        let (store, w) = svc().await;
        let refs = RefSvc::new(
            store.clone(),
            RefHub::new(store.clone(), std::time::Duration::from_secs(3600)),
        );
        let a = w
            .create(REPO, &spec_to_changeset(spec(&[], vec![]), 10).expect("s"))
            .await
            .expect("a");
        LandSvc::new(scion_land::Coordinator::new(w.writer().clone(), Default::default()))
            .land(REPO, "main", a.id, false)
            .await
            .expect("land");

        let r = refs
            .resolve_ref(Request::new(ResolveRefRequest {
                repo: REPO.into(),
                r#ref: "main".into(),
            }))
            .await
            .expect("resolve")
            .into_inner();
        assert_eq!(ObjectId::from_bytes(&r.changeset_id), Ok(a.id));
        assert_eq!(ObjectId::from_bytes(&r.root_tree), Ok(a.root_tree));
        assert_eq!(r.version, 1);

        let l = refs
            .list_refs(Request::new(ListRefsRequest { repo: REPO.into() }))
            .await
            .expect("list")
            .into_inner();
        assert_eq!(l.refs.len(), 1);
        assert_eq!(l.refs[0].name, "main");
    }

    #[tokio::test]
    async fn errors() {
        let (store, _w) = svc().await;
        let refs = RefSvc::new(store.clone(), RefHub::new(store, std::time::Duration::from_secs(3600)));
        let r = refs
            .resolve_ref(Request::new(ResolveRefRequest {
                repo: REPO.into(),
                r#ref: "main".into(),
            }))
            .await;
        assert_eq!(r.map(|_| ()).map_err(|s| s.code()), Err(Code::NotFound));
        let r = refs
            .resolve_ref(Request::new(ResolveRefRequest {
                repo: "x".into(),
                r#ref: "main".into(),
            }))
            .await;
        assert_eq!(r.map(|_| ()).map_err(|s| s.code()), Err(Code::NotFound));
        let r = refs.list_refs(Request::new(ListRefsRequest { repo: "x".into() })).await;
        assert_eq!(r.map(|_| ()).map_err(|s| s.code()), Err(Code::NotFound));
    }

    fn hub(store: Arc<scion_store::MemStore>) -> Arc<RefHub> {
        RefHub::new(store, std::time::Duration::from_secs(3600))
    }

    async fn next(stream: &mut WatchRefsStream) -> WatchRefsResponse {
        use tokio_stream::StreamExt;
        tokio::time::timeout(std::time::Duration::from_secs(5), stream.next())
            .await
            .expect("timely")
            .expect("item")
            .expect("ok")
    }

    #[tokio::test]
    async fn watch_sends_snapshot_then_lands() {
        let (store, w) = svc().await;
        let h = hub(store.clone());
        let refs = RefSvc::new(store.clone(), h.clone());
        let lands = LandSvc::new(scion_land::Coordinator::new(w.writer().clone(), Default::default())).with_ref_hub(h);
        let a = w
            .create(REPO, &spec_to_changeset(spec(&[], vec![]), 10).expect("s"))
            .await
            .expect("a");
        lands.land(REPO, "main", a.id, false).await.expect("main");
        lands.land(REPO, "dev", a.id, false).await.expect("dev");
        lands.land(REPO, "old", a.id, false).await.expect("old");

        // 只订阅 main 与 old；old 的版本客户端已知
        let mut stream = refs
            .watch_refs(Request::new(WatchRefsRequest {
                repo: REPO.into(),
                refs: vec!["main".into(), "old".into()],
                known_versions: HashMap::from([("old".to_string(), 1)]),
            }))
            .await
            .expect("watch")
            .into_inner();
        let snap = next(&mut stream).await;
        assert!(snap.snapshot);
        assert_eq!(snap.events.len(), 1);
        assert_eq!(snap.events[0].name, "main");
        assert_eq!(ObjectId::from_bytes(&snap.events[0].root_tree), Ok(a.root_tree));

        let b = w
            .create(REPO, &spec_to_changeset(spec(&[a.id], vec![]), 11).expect("s"))
            .await
            .expect("b");
        lands.land(REPO, "dev", b.id, false).await.expect("dev, not watched");
        lands.land(REPO, "main", b.id, false).await.expect("main");
        let ev = next(&mut stream).await;
        assert!(!ev.snapshot);
        assert_eq!(ev.events.len(), 1);
        assert_eq!((ev.events[0].name.as_str(), ev.events[0].version), ("main", 2));
        assert_eq!(ObjectId::from_bytes(&ev.events[0].changeset_id), Ok(b.id));

        let missing = refs
            .watch_refs(Request::new(WatchRefsRequest {
                repo: "x".into(),
                ..Default::default()
            }))
            .await;
        assert_eq!(missing.map(|_| ()).map_err(|s| s.code()), Err(Code::NotFound));
    }

    fn update(name: &str, version: u64) -> RefUpdate {
        RefUpdate {
            name: name.into(),
            target: ObjectId::of_blob(name.as_bytes()),
            root_tree: None,
            version,
        }
    }

    #[test]
    fn filter_keeps_newer_versions_of_watched_refs() {
        let mut all = Filter {
            refs: HashSet::new(),
            sent: HashMap::from([("a".to_string(), 2)]),
        };
        assert!(!all.take(&update("a", 2)));
        assert!(all.take(&update("a", 3)));
        assert!(all.take(&update("b", 1)));
        assert!(!all.take(&update("b", 1)));
        let mut some = Filter {
            refs: HashSet::from(["a".to_string()]),
            sent: HashMap::new(),
        };
        assert!(!some.take(&update("b", 1)));
        assert_eq!(some.take_all(&[update("a", 1), update("b", 1)]).len(), 1);
        assert!(event(&update("a", 1)).root_tree.is_empty());
    }

    /// 订阅者落后太多（通道 Lagged）：重新取快照补上；同时到达的变化合并成一条响应。
    #[tokio::test]
    async fn pump_recovers_from_lag_and_batches() {
        let (store, w) = svc().await;
        let h = hub(store.clone());
        let a = w
            .create(REPO, &spec_to_changeset(spec(&[], vec![]), 10).expect("s"))
            .await
            .expect("a");
        store
            .cas_ref(REPO, "main", scion_store::RefExpect::Absent, a.id)
            .await
            .expect("main");
        let (btx, brx) = broadcast::channel(2);
        for v in 1..=4 {
            btx.send(update("x", v)).expect("send");
        }
        let (tx, mut out) = mpsc::channel(4);
        let filter = Filter {
            refs: HashSet::new(),
            sent: HashMap::new(),
        };
        let task = tokio::spawn(pump(h, REPO.into(), filter, brx, tx));
        let resp = out.recv().await.expect("resp").expect("ok");
        // 快照里的 main，加上通道里剩下的 x 的最新版本
        let got: Vec<_> = resp.events.iter().map(|e| (e.name.as_str(), e.version)).collect();
        assert!(got.contains(&("main", 1)), "{got:?}");
        assert!(got.contains(&("x", 4)), "{got:?}");
        // 通道关闭：发送循环结束
        drop(btx);
        task.await.expect("pump ends");

        // 服务端停机：发送循环结束，流随之结束
        let (btx, brx) = broadcast::channel(2);
        let (tx, mut out) = mpsc::channel(1);
        let closing = hub(store.clone());
        closing.close();
        let filter = Filter {
            refs: HashSet::new(),
            sent: HashMap::new(),
        };
        pump(closing, REPO.into(), filter, brx, tx).await;
        assert!(out.recv().await.is_none());
        drop(btx);

        // 客户端断开：发送循环结束
        let (btx, brx) = broadcast::channel(2);
        let (tx, out) = mpsc::channel(1);
        drop(out);
        let filter = Filter {
            refs: HashSet::new(),
            sent: HashMap::new(),
        };
        pump(hub(store.clone()), REPO.into(), filter, brx, tx).await;
        drop(btx);

        // 重新取快照失败：把错误发给客户端
        let (btx, brx) = broadcast::channel(1);
        btx.send(update("x", 1)).expect("send");
        btx.send(update("x", 2)).expect("send");
        let (tx, mut out) = mpsc::channel(1);
        let filter = Filter {
            refs: HashSet::new(),
            sent: HashMap::new(),
        };
        pump(hub(store), "missing".into(), filter, brx, tx).await;
        assert_eq!(
            out.recv().await.expect("err").map(|_| ()).map_err(|s| s.code()),
            Err(Code::NotFound)
        );
    }
}
