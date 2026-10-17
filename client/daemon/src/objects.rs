//! daemon 的 `ObjectService`：本地命中就地应答，未命中按调用者身份回源（经服务端推荐的区域缓存），
//! 同一仓库对同一对象的并发未命中只回源一次。
//!
//! 与服务端一致，对象只对索引中有它的仓库可见：本地缓存按 id 去重，但应答前确认 id 在调用者的仓库里
//! 可见。回源成功的 (服务端, 仓库, id) 记入可见集；命中而可见性未知的，先向服务端 `HasObjects` 确认。

use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;

use bytes::Bytes;
use scion_flight::{Claim, Group};
use scion_object::ObjectId;
use scion_proto::v1::object_service_client::ObjectServiceClient;
use scion_proto::v1::object_service_server::ObjectService;
use scion_proto::v1::{Encoding, GetObjectsRequest, GetObjectsResponse, HasObjectsRequest, HasObjectsResponse};
use tokio_stream::Stream;
use tonic::{Request, Response, Status};

use crate::Daemon;
use crate::upstream::{auth_of, forward, upstream_of};

pub type GetObjectsStream = Pin<Box<dyn Stream<Item = Result<GetObjectsResponse, Status>> + Send>>;

/// (服务端, 仓库, 对象)。
pub type Key = (Arc<str>, Arc<str>, ObjectId);

/// 并发未命中的合并：同一仓库的同一对象只回源一次。
pub type Flight = Group<Key, Result<Bytes, Status>>;

/// 已确认可见的 (服务端, 仓库, 对象)。
pub type Visible = moka::sync::Cache<Key, ()>;

/// 与服务端相同的「对象不存在」错误。
fn object_missing(ids: &[ObjectId]) -> Status {
    let listed: Vec<String> = ids.iter().take(32).map(ToString::to_string).collect();
    scion_client::Error::Rpc {
        code: tonic::Code::NotFound,
        reason: Some(scion_proto::v1::ErrorReason::ObjectMissing),
        message: format!("{} object(s) not found", ids.len()),
        metadata: HashMap::from([
            ("ids".to_string(), listed.join(",")),
            ("count".to_string(), ids.len().to_string()),
        ]),
    }
    .to_status()
}

/// 把一个对象从 `start` 开始切成若干分块。`start == len` 时返回一个空的结尾块。
fn chunk_object(id: &ObjectId, data: Bytes, start: usize, chunk: usize) -> Vec<GetObjectsResponse> {
    let total = data.len();
    let id_bytes = Bytes::copy_from_slice(&id.to_bytes());
    let mut out = Vec::new();
    let mut off = start;
    loop {
        let end = (off + chunk).min(total);
        out.push(GetObjectsResponse {
            id: id_bytes.clone(),
            offset: off as u64,
            data: data.slice(off..end),
            encoding: Encoding::Identity.into(),
            last: end == total,
            total_size: total as u64,
        });
        if end == total {
            return out;
        }
        off = end;
    }
}

pub struct DaemonObjectSvc {
    daemon: Arc<Daemon>,
}

impl DaemonObjectSvc {
    pub fn new(daemon: Arc<Daemon>) -> Self {
        Self { daemon }
    }
}

impl Daemon {
    /// 取一组不重复的对象：本地 → 合并的回源。
    pub(crate) async fn objects(
        &self,
        client: &scion_client::Client,
        upstream: &Arc<str>,
        repo: &Arc<str>,
        ids: &[ObjectId],
    ) -> HashMap<ObjectId, Result<Bytes, Status>> {
        let mut out = HashMap::with_capacity(ids.len());
        let mut leaders = Vec::new();
        let mut followers = Vec::new();
        for id in ids {
            if let Some(b) = self.cas.get(id) {
                self.stats.hit();
                out.insert(*id, Ok(b));
                continue;
            }
            self.stats.miss();
            match self.flight.claim((upstream.clone(), repo.clone(), *id)) {
                Claim::Leader(l) => {
                    // 上一个 leader 可能刚刚写入
                    match self.cas.get(id) {
                        Some(b) => {
                            out.insert(*id, Ok(b.clone()));
                            l.complete(Ok(b));
                        }
                        None => leaders.push(l),
                    }
                }
                Claim::Follower(f) => followers.push((*id, f)),
            }
        }
        if !leaders.is_empty() {
            let want: Vec<ObjectId> = leaders.iter().map(|l| l.key().2).collect();
            let results: Vec<Result<Bytes, Status>> = match client.get_objects(&want).await {
                Ok(v) => v.into_iter().map(Ok).collect(),
                Err(e) if want.len() == 1 => vec![Err(e.to_status())],
                // 整批失败可能只是其中一个对象的问题：逐个重试，不连累其它对象
                Err(_) => {
                    let mut v = Vec::with_capacity(want.len());
                    for id in &want {
                        v.push(client.get_object(id).await.map_err(|e| e.to_status()));
                    }
                    v
                }
            };
            let mut fetched = Vec::new();
            for (l, r) in leaders.into_iter().zip(results) {
                let id = l.key().2;
                if let Ok(b) = &r {
                    self.cas.remember(id, b);
                    // 服务端按这个仓库校验过：可见。先记下再交付，等待者据此免查
                    self.visible.insert(l.key().clone(), ());
                    fetched.push((id, b.clone()));
                }
                l.complete(r.clone());
                out.insert(id, r);
            }
            // 落盘不阻塞应答：对象已在内存层，随后的读取照样命中
            if !fetched.is_empty() {
                let cas = self.cas.clone();
                tokio::task::spawn_blocking(move || {
                    for (id, b) in fetched {
                        if let Err(e) = cas.persist(id, &b) {
                            tracing::warn!(%id, error = %e, "writing the object cache failed");
                        }
                    }
                });
            }
        }
        let mut retry = Vec::new();
        for (id, f) in followers {
            match f.wait().await {
                Some(r) => {
                    out.insert(id, r);
                }
                None => retry.push(id),
            }
        }
        if !retry.is_empty() {
            out.extend(Box::pin(self.objects(client, upstream, repo, &retry)).await);
        }
        out
    }

    /// 确认 `ids` 都在调用者的仓库中可见（`client` 带着仓库与调用者身份）。
    pub(crate) async fn check_visible(
        &self,
        client: &scion_client::Client,
        upstream: &Arc<str>,
        repo: &Arc<str>,
        ids: &[ObjectId],
    ) -> Result<(), Status> {
        let key = |id: ObjectId| (upstream.clone(), repo.clone(), id);
        let unknown: Vec<ObjectId> = ids
            .iter()
            .filter(|id| !self.visible.contains_key(&key(**id)))
            .copied()
            .collect();
        if unknown.is_empty() {
            return Ok(());
        }
        let missing = client.missing_objects(&unknown).await.map_err(|e| e.to_status())?;
        if !missing.is_empty() {
            return Err(object_missing(&missing));
        }
        for id in unknown {
            self.visible.insert(key(id), ());
        }
        Ok(())
    }
}

#[tonic::async_trait]
impl ObjectService for DaemonObjectSvc {
    type GetObjectsStream = GetObjectsStream;

    async fn get_objects(&self, request: Request<GetObjectsRequest>) -> Result<Response<GetObjectsStream>, Status> {
        self.daemon.touch();
        let url = upstream_of(&request)?;
        let auth = auth_of(&request);
        let req = request.into_inner();
        let up = self.daemon.upstreams.get(&url)?;
        let client = up.client().await?.with_repo(req.repo.clone()).with_auth(auth);
        let limits = client.server_info().limits.clone();
        if req.wants.len() > limits.max_batch_objects as usize {
            return Err(Status::invalid_argument(format!(
                "{} objects requested; at most {} per request (max_batch_objects)",
                req.wants.len(),
                limits.max_batch_objects
            )));
        }
        let ids = req
            .wants
            .iter()
            .map(|w| {
                ObjectId::from_bytes(&w.id).map_err(|e| Status::invalid_argument(format!("invalid object id: {e}")))
            })
            .collect::<Result<Vec<_>, _>>()?;
        let mut unique = ids.clone();
        unique.sort_unstable_by_key(|id| id.to_bytes());
        unique.dedup();
        let (upstream, repo): (Arc<str>, Arc<str>) = (Arc::from(url.as_str()), Arc::from(req.repo.as_str()));
        let found = self.daemon.objects(&client, &upstream, &repo, &unique).await;
        let chunk = usize::try_from(limits.max_chunk_bytes).unwrap_or(usize::MAX).max(1);
        let mut parts = Vec::new();
        for (id, want) in ids.iter().zip(&req.wants) {
            let data = match found.get(id) {
                Some(Ok(b)) => b.clone(),
                Some(Err(e)) => return Err(e.clone()),
                None => return Err(Status::internal(format!("object {id} was not resolved"))),
            };
            if want.offset > data.len() as u64 {
                return Err(Status::out_of_range(format!(
                    "offset {} beyond size {} of {id}",
                    want.offset,
                    data.len()
                )));
            }
            parts.extend(chunk_object(id, data, want.offset as usize, chunk).into_iter().map(Ok));
        }
        self.daemon.check_visible(&client, &upstream, &repo, &unique).await?;
        Ok(Response::new(Box::pin(tokio_stream::iter(parts))))
    }

    async fn has_objects(&self, request: Request<HasObjectsRequest>) -> Result<Response<HasObjectsResponse>, Status> {
        self.daemon.touch();
        let up = self.daemon.upstreams.get(&upstream_of(&request)?)?;
        let fwd = forward(&request, request.get_ref().clone());
        let resp = ObjectServiceClient::new(up.raw.clone()).has_objects(fwd).await?;
        Ok(Response::new(resp.into_inner()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn chunks_cover_the_object_from_offset() {
        let id = ObjectId::of_blob(b"0123456789");
        let parts = chunk_object(&id, Bytes::from_static(b"0123456789"), 2, 4);
        let got: Vec<_> = parts.iter().map(|p| (p.offset, p.data.len(), p.last)).collect();
        assert_eq!(got, [(2, 4, false), (6, 4, true)]);
        let end = chunk_object(&id, Bytes::from_static(b"0123456789"), 10, 4);
        assert_eq!((end.len(), end[0].last, end[0].data.len()), (1, true, 0));
    }
}
