//! 写入、合入与查询。

use std::collections::BTreeMap;
use std::time::{SystemTime, UNIX_EPOCH};

use bytes::{Bytes, BytesMut};
use scion_object::{EntryMode, ObjectId};
use scion_proto::v1::{
    self, ChangesetSpec, CreateChangesetRequest, LandRequest, ListDirRequest, ListRefsRequest, ReadFileRequest,
    ResolveRefRequest, UploadBlobsRequest, file_change, land_service_client::LandServiceClient,
    query_service_client::QueryServiceClient, ref_service_client::RefServiceClient,
    write_service_client::WriteServiceClient,
};

use crate::client::Client;
use crate::error::{Error, Result};

/// ref 当前状态。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RefTip {
    pub changeset: ObjectId,
    pub root_tree: ObjectId,
    pub version: u64,
}

/// 创建提交的结果。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CreatedChangeset {
    pub id: ObjectId,
    pub root_tree: ObjectId,
    pub generation: u64,
}

/// 合入结果。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Landed {
    pub tip: ObjectId,
    pub version: u64,
}

/// 目录条目。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DirEntry {
    pub name: String,
    pub mode: EntryMode,
    pub id: ObjectId,
    pub size: u64,
}

/// 提交作者 / 提交者。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Person {
    pub name: String,
    pub email: String,
}

fn bytes_of(id: &ObjectId) -> Bytes {
    Bytes::copy_from_slice(&id.to_bytes())
}

fn id_from(raw: &[u8], what: &str) -> Result<ObjectId> {
    ObjectId::from_bytes(raw).map_err(|e| Error::Protocol(format!("bad {what} in response: {e}")))
}

fn proto_mode(m: EntryMode) -> v1::EntryMode {
    match m {
        EntryMode::File => v1::EntryMode::File,
        EntryMode::Executable => v1::EntryMode::Executable,
        EntryMode::Symlink => v1::EntryMode::Symlink,
        EntryMode::Tree => v1::EntryMode::Tree,
    }
}

fn mode_from(m: i32) -> Result<EntryMode> {
    match v1::EntryMode::try_from(m) {
        Ok(v1::EntryMode::File) => Ok(EntryMode::File),
        Ok(v1::EntryMode::Executable) => Ok(EntryMode::Executable),
        Ok(v1::EntryMode::Symlink) => Ok(EntryMode::Symlink),
        Ok(v1::EntryMode::Tree) => Ok(EntryMode::Tree),
        _ => Err(Error::Protocol(format!("unknown entry mode {m}"))),
    }
}

fn request_id() -> String {
    uuid::Uuid::now_v7().to_string()
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| i64::try_from(d.as_millis()).unwrap_or(i64::MAX))
        .unwrap_or(0)
}

/// 把对象切成上传消息：第一条带 repo，按 `chunk` 分块，最后一块 last=true。
fn upload_messages(repo: &str, id: &ObjectId, data: &Bytes, chunk: usize) -> Vec<UploadBlobsRequest> {
    let chunk = chunk.max(1);
    let mut out = Vec::with_capacity(data.len().div_ceil(chunk).max(1));
    let mut off = 0;
    loop {
        let end = (off + chunk).min(data.len());
        out.push(UploadBlobsRequest {
            repo: repo.to_owned(),
            id: bytes_of(id),
            offset: off as u64,
            data: data.slice(off..end),
            last: end == data.len(),
        });
        if end == data.len() {
            return out;
        }
        off = end;
    }
}

impl Client {
    /// 上传文件内容，只传服务端还没有的。返回各内容的 Blob id（与输入顺序一致）。
    pub async fn upload_blobs(&self, contents: &[Bytes]) -> Result<Vec<ObjectId>> {
        let repo = self.repo()?;
        let ids: Vec<ObjectId> = contents.iter().map(|c| ObjectId::of_blob(c)).collect();
        let missing = self.missing_objects(&ids).await?;
        if missing.is_empty() {
            return Ok(ids);
        }
        let max = self.server_info().limits.max_upload_object_bytes;
        let chunk = usize::try_from(self.server_info().limits.max_chunk_bytes).unwrap_or(usize::MAX);
        let mut msgs = Vec::new();
        let mut queued = std::collections::HashSet::new();
        for (id, data) in ids.iter().zip(contents) {
            if !missing.contains(id) || !queued.insert(*id) {
                continue;
            }
            if max > 0 && data.len() as u64 > max {
                return Err(Error::Protocol(format!(
                    "{id} is {} bytes, above the server limit of {max} bytes",
                    data.len()
                )));
            }
            msgs.extend(upload_messages(&repo, id, data, chunk));
        }
        let mut svc = WriteServiceClient::new(self.channel.clone());
        svc.upload_blobs(self.request(tokio_stream::iter(msgs))?).await?;
        Ok(ids)
    }

    /// 以 `parent`（`None` 表示根提交）为父提交开始构造一个提交。
    pub fn changeset(&self, parent: Option<ObjectId>) -> ChangesetBuilder<'_> {
        ChangesetBuilder {
            client: self,
            parents: parent.into_iter().collect(),
            author: None,
            message: String::new(),
            puts: Vec::new(),
            deletes: Vec::new(),
            extensions: BTreeMap::new(),
        }
    }

    /// 合入：把 `reference` 移到 `changeset`。M1 只允许快进。
    pub async fn land(&self, reference: &str, changeset: &ObjectId) -> Result<Landed> {
        let req = LandRequest {
            repo: self.repo()?,
            request_id: request_id(),
            r#ref: reference.to_owned(),
            changeset_id: bytes_of(changeset),
        };
        let resp = LandServiceClient::new(self.channel.clone())
            .land(self.request(req)?)
            .await?
            .into_inner();
        Ok(Landed {
            tip: id_from(&resp.tip, "tip")?,
            version: resp.ref_version,
        })
    }

    /// 解析 ref；不存在时返回 `Ok(None)`。
    pub async fn resolve_ref(&self, reference: &str) -> Result<Option<RefTip>> {
        let req = ResolveRefRequest {
            repo: self.repo()?,
            r#ref: reference.to_owned(),
        };
        match RefServiceClient::new(self.channel.clone())
            .resolve_ref(self.request(req)?)
            .await
        {
            Ok(r) => {
                let r = r.into_inner();
                Ok(Some(RefTip {
                    changeset: id_from(&r.changeset_id, "changeset id")?,
                    root_tree: id_from(&r.root_tree, "root tree")?,
                    version: r.version,
                }))
            }
            Err(s) => {
                let e = Error::from(s);
                if e.reason() == Some(v1::ErrorReason::RefNotFound) {
                    Ok(None)
                } else {
                    Err(e)
                }
            }
        }
    }

    /// 列出全部 ref（名字、提交、版本），按名字字节序。
    pub async fn list_refs(&self) -> Result<Vec<(String, ObjectId, u64)>> {
        let req = ListRefsRequest { repo: self.repo()? };
        let resp = RefServiceClient::new(self.channel.clone())
            .list_refs(self.request(req)?)
            .await?
            .into_inner();
        resp.refs
            .into_iter()
            .map(|r| Ok((r.name, id_from(&r.changeset_id, "changeset id")?, r.version)))
            .collect()
    }

    /// 读取 `revision`（ref 名或提交 id）下的文件，并校验内容哈希。
    pub async fn read_file(&self, revision: &str, path: &str) -> Result<Bytes> {
        let req = ReadFileRequest {
            repo: self.repo()?,
            revision: revision.to_owned(),
            path: path.to_owned(),
            offset: 0,
        };
        let mut stream = QueryServiceClient::new(self.channel.clone())
            .read_file(self.request(req)?)
            .await?
            .into_inner();
        let mut buf = BytesMut::new();
        let mut blob: Option<ObjectId> = None;
        while let Some(part) = stream.message().await? {
            let id = id_from(&part.blob_id, "blob id")?;
            if blob.is_some_and(|b| b != id) || part.offset != buf.len() as u64 {
                return Err(Error::Protocol(format!("inconsistent ReadFile stream for {path:?}")));
            }
            blob = Some(id);
            buf.extend_from_slice(&part.data);
            if part.last {
                let data = buf.freeze();
                if !id.verify(&data) {
                    return Err(Error::Corrupt(id));
                }
                return Ok(data);
            }
        }
        Err(Error::Protocol(format!(
            "ReadFile stream for {path:?} ended before the last chunk"
        )))
    }

    /// 列出 `revision` 下的目录（`""` 为根目录）。
    pub async fn list_dir(&self, revision: &str, path: &str) -> Result<Vec<DirEntry>> {
        let req = ListDirRequest {
            repo: self.repo()?,
            revision: revision.to_owned(),
            path: path.to_owned(),
        };
        let resp = QueryServiceClient::new(self.channel.clone())
            .list_dir(self.request(req)?)
            .await?
            .into_inner();
        resp.entries
            .into_iter()
            .map(|e| {
                Ok(DirEntry {
                    mode: mode_from(e.mode)?,
                    id: id_from(&e.id, "entry id")?,
                    name: e.name,
                    size: e.size,
                })
            })
            .collect()
    }
}

/// 构造提交：收集改动，`create()` 时先上传缺失的文件内容，再创建提交。
pub struct ChangesetBuilder<'a> {
    client: &'a Client,
    parents: Vec<ObjectId>,
    author: Option<Person>,
    message: String,
    puts: Vec<(String, EntryMode, Bytes)>,
    deletes: Vec<String>,
    extensions: BTreeMap<String, Bytes>,
}

impl ChangesetBuilder<'_> {
    /// 写入普通文件。
    pub fn put(self, path: impl Into<String>, content: impl Into<Bytes>) -> Self {
        self.put_with_mode(path, EntryMode::File, content)
    }

    pub fn put_with_mode(mut self, path: impl Into<String>, mode: EntryMode, content: impl Into<Bytes>) -> Self {
        self.puts.push((path.into(), mode, content.into()));
        self
    }

    pub fn delete(mut self, path: impl Into<String>) -> Self {
        self.deletes.push(path.into());
        self
    }

    /// 追加一个父提交（合并提交）。
    pub fn parent(mut self, parent: ObjectId) -> Self {
        self.parents.push(parent);
        self
    }

    pub fn message(mut self, message: impl Into<String>) -> Self {
        self.message = message.into();
        self
    }

    pub fn author(mut self, name: impl Into<String>, email: impl Into<String>) -> Self {
        self.author = Some(Person {
            name: name.into(),
            email: email.into(),
        });
        self
    }

    pub fn extension(mut self, key: impl Into<String>, value: impl Into<Bytes>) -> Self {
        self.extensions.insert(key.into(), value.into());
        self
    }

    fn spec(&self, ids: &[ObjectId]) -> ChangesetSpec {
        let person = self.author.clone().unwrap_or(Person {
            name: "scion".into(),
            email: "scion@localhost".into(),
        });
        let sig = v1::Signature {
            name: person.name,
            email: person.email,
            time_ms: now_ms(),
            tz_offset_minutes: 0,
        };
        let mut delta: Vec<v1::FileChange> = self
            .puts
            .iter()
            .zip(ids)
            .map(|((path, mode, content), id)| v1::FileChange {
                path: path.clone(),
                op: Some(file_change::Op::Put(v1::PutFile {
                    mode: proto_mode(*mode).into(),
                    id: bytes_of(id),
                    size: content.len() as u64,
                })),
            })
            .collect();
        delta.extend(self.deletes.iter().map(|p| v1::FileChange {
            path: p.clone(),
            op: Some(file_change::Op::Delete(v1::DeleteFile {})),
        }));
        ChangesetSpec {
            parents: self.parents.iter().map(bytes_of).collect(),
            author: Some(sig.clone()),
            committer: Some(sig),
            message: self.message.clone(),
            change_id: Bytes::copy_from_slice(uuid::Uuid::now_v7().as_bytes()),
            delta,
            extensions: self.extensions.clone().into_iter().collect(),
        }
    }

    pub async fn create(self) -> Result<CreatedChangeset> {
        let contents: Vec<Bytes> = self.puts.iter().map(|(_, _, c)| c.clone()).collect();
        let ids = self.client.upload_blobs(&contents).await?;
        let req = CreateChangesetRequest {
            repo: self.client.repo()?,
            request_id: request_id(),
            changeset: Some(self.spec(&ids)),
        };
        let resp = WriteServiceClient::new(self.client.channel.clone())
            .create_changeset(self.client.request(req)?)
            .await?
            .into_inner();
        Ok(CreatedChangeset {
            id: id_from(&resp.changeset_id, "changeset id")?,
            root_tree: id_from(&resp.root_tree, "root tree")?,
            generation: resp.generation,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn upload_messages_split_and_mark_last() {
        let data = Bytes::from_static(b"abcdefg");
        let id = ObjectId::of_blob(&data);
        let m = upload_messages("r", &id, &data, 3);
        assert_eq!(m.iter().map(|x| x.offset).collect::<Vec<_>>(), [0, 3, 6]);
        assert_eq!(m.iter().map(|x| x.last).collect::<Vec<_>>(), [false, false, true]);
        assert!(m.iter().all(|x| x.repo == "r"));
        let empty = upload_messages("r", &ObjectId::of_blob(b""), &Bytes::new(), 0);
        assert_eq!(empty.len(), 1);
        assert!(empty[0].last && empty[0].data.is_empty());
    }

    #[test]
    fn mode_roundtrip() {
        for m in [
            EntryMode::File,
            EntryMode::Executable,
            EntryMode::Symlink,
            EntryMode::Tree,
        ] {
            assert_eq!(mode_from(proto_mode(m).into()).ok(), Some(m));
        }
        assert!(matches!(mode_from(0), Err(Error::Protocol(_))));
        assert!(matches!(mode_from(42), Err(Error::Protocol(_))));
    }

    #[test]
    fn id_from_bad_bytes() {
        assert!(matches!(id_from(b"x", "thing"), Err(Error::Protocol(m)) if m.contains("thing")));
    }

    #[test]
    fn request_ids_are_unique_uuids() {
        let (a, b) = (request_id(), request_id());
        assert_ne!(a, b);
        assert_eq!(a.len(), 36);
        assert!(now_ms() > 1_577_836_800_000);
    }
}
