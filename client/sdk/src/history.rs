//! 历史查询：log、祖先判断、merge-base、diff。

use std::collections::BTreeMap;

use bytes::Bytes;
use scion_object::ObjectId;
use scion_proto::v1::{
    self, BlameRequest, DerivedKind, DiffRequest, GetDerivedRequest, IsAncestorRequest, LogRequest, MergeBaseRequest,
    ResolveRevisionRequest, StatRequest, query_service_client::QueryServiceClient,
};

use crate::client::Client;
use crate::error::{Error, Result};
use crate::ops::{DirEntry, Signature, id_from, mode_from};

/// 历史中的一个提交。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogItem {
    pub id: ObjectId,
    pub parents: Vec<ObjectId>,
    pub author: Signature,
    pub committer: Signature,
    pub message: String,
    pub generation: u64,
    pub root_tree: ObjectId,
    pub extensions: BTreeMap<String, Bytes>,
}

/// 一页历史；`next_page_token` 为 None 表示没有更多。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogPage {
    pub entries: Vec<LogItem>,
    pub next_page_token: Option<String>,
}

/// 一个文件的变化：`old` 为 None 表示新增，`new` 为 None 表示删除。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileChangeInfo {
    pub path: String,
    pub old: Option<DirEntry>,
    pub new: Option<DirEntry>,
}

/// `resolve_revision` 的结果。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Revision {
    pub id: ObjectId,
    pub root_tree: ObjectId,
    pub generation: u64,
    pub parents: Vec<ObjectId>,
}

/// Blame 中出现的一个提交。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BlameCommitInfo {
    pub id: ObjectId,
    pub author: Signature,
    pub summary: String,
}

/// 逐行归属：`lines[i]` 是第 i 行（从 0 开始）在 `commits` 中的下标。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BlameInfo {
    pub blob: ObjectId,
    pub commits: Vec<BlameCommitInfo>,
    pub lines: Vec<usize>,
}

/// 把区间展开为逐行下标，并校验区间首尾相接、下标有效。
fn expand(ranges: &[v1::BlameRange], commits: usize) -> Result<Vec<usize>> {
    let mut lines = Vec::new();
    for r in ranges {
        let idx = r.commit_index as usize;
        if r.start_line as usize != lines.len() + 1 || idx >= commits {
            return Err(Error::Protocol("malformed blame ranges".into()));
        }
        lines.extend(std::iter::repeat_n(idx, r.line_count as usize));
    }
    Ok(lines)
}

fn signature(s: Option<v1::Signature>) -> Result<Signature> {
    let s = s.ok_or_else(|| Error::Protocol("log entry without signature".into()))?;
    Ok(Signature {
        name: s.name,
        email: s.email,
        time_ms: s.time_ms,
        tz_offset_minutes: s.tz_offset_minutes,
    })
}

fn item(e: v1::LogEntry) -> Result<LogItem> {
    Ok(LogItem {
        id: id_from(&e.changeset_id, "changeset id")?,
        parents: e
            .parents
            .iter()
            .map(|p| id_from(p, "parent id"))
            .collect::<Result<_>>()?,
        author: signature(e.author)?,
        committer: signature(e.committer)?,
        message: e.message,
        generation: e.generation,
        root_tree: id_from(&e.root_tree, "root tree")?,
        extensions: e.extensions.into_iter().collect(),
    })
}

fn entry(e: Option<v1::TreeEntry>) -> Result<Option<DirEntry>> {
    e.map(|e| {
        Ok(DirEntry {
            mode: mode_from(e.mode)?,
            id: id_from(&e.id, "entry id")?,
            name: e.name,
            size: e.size,
        })
    })
    .transpose()
}

impl Client {
    fn query(&self) -> QueryServiceClient<tonic::transport::Channel> {
        QueryServiceClient::new(self.channel.clone())
    }

    /// 历史：从 `revision` 开始按代数从新到旧。传入上一页的 `page_token` 继续翻页（此时忽略 revision）。
    pub async fn log(
        &self,
        revision: &str,
        path: Option<&str>,
        page_size: u32,
        page_token: Option<&str>,
    ) -> Result<LogPage> {
        let req = LogRequest {
            repo: self.repo()?,
            revision: revision.to_owned(),
            path: path.unwrap_or_default().to_owned(),
            page_size,
            page_token: page_token.unwrap_or_default().to_owned(),
        };
        let resp = self.query().log(self.request(req)?).await?.into_inner();
        Ok(LogPage {
            entries: resp.entries.into_iter().map(item).collect::<Result<_>>()?,
            next_page_token: Some(resp.next_page_token).filter(|t| !t.is_empty()),
        })
    }

    pub async fn is_ancestor(&self, ancestor: &str, descendant: &str) -> Result<bool> {
        let req = IsAncestorRequest {
            repo: self.repo()?,
            ancestor: ancestor.to_owned(),
            descendant: descendant.to_owned(),
        };
        Ok(self
            .query()
            .is_ancestor(self.request(req)?)
            .await?
            .into_inner()
            .is_ancestor)
    }

    /// 最佳公共祖先，按代数从高到低。
    pub async fn merge_bases(&self, a: &str, b: &str) -> Result<Vec<ObjectId>> {
        let req = MergeBaseRequest {
            repo: self.repo()?,
            a: a.to_owned(),
            b: b.to_owned(),
        };
        let resp = self.query().merge_base(self.request(req)?).await?.into_inner();
        resp.bases.iter().map(|b| id_from(b, "merge base")).collect()
    }

    /// 文件每一行最后被改动的提交（沿第一父提交链追溯）。
    pub async fn blame(&self, revision: &str, path: &str) -> Result<BlameInfo> {
        let req = BlameRequest {
            repo: self.repo()?,
            revision: revision.to_owned(),
            path: path.to_owned(),
        };
        let resp = self.query().blame(self.request(req)?).await?.into_inner();
        let commits = resp
            .commits
            .into_iter()
            .map(|c| {
                Ok(BlameCommitInfo {
                    id: id_from(&c.changeset_id, "changeset id")?,
                    author: signature(c.author)?,
                    summary: c.summary,
                })
            })
            .collect::<Result<Vec<_>>>()?;
        let lines = expand(&resp.ranges, commits.len())?;
        Ok(BlameInfo {
            blob: id_from(&resp.blob_id, "blob id")?,
            commits,
            lines,
        })
    }

    /// 解析版本号：ref 名、提交 id 文本或 40 位 git sha。
    pub async fn resolve_revision(&self, revision: &str) -> Result<Revision> {
        let req = ResolveRevisionRequest {
            repo: self.repo()?,
            revision: revision.to_owned(),
        };
        let r = self.query().resolve_revision(self.request(req)?).await?.into_inner();
        Ok(Revision {
            id: id_from(&r.changeset_id, "changeset id")?,
            root_tree: id_from(&r.root_tree, "root tree")?,
            generation: r.generation,
            parents: r
                .parents
                .iter()
                .map(|p| id_from(p, "parent id"))
                .collect::<Result<_>>()?,
        })
    }

    /// 单个路径的条目；`path` 为空时返回根目录（名字为空、类型为目录）。
    pub async fn stat(&self, revision: &str, path: &str) -> Result<DirEntry> {
        let req = StatRequest {
            repo: self.repo()?,
            revision: revision.to_owned(),
            path: path.to_owned(),
        };
        let r = self.query().stat(self.request(req)?).await?.into_inner();
        entry(r.entry)?.ok_or_else(|| Error::Protocol("Stat returned no entry".into()))
    }

    /// 提交对应的 git sha（40 位十六进制）；不是从 git 导入的提交返回 None。
    pub async fn git_sha(&self, revision: &str) -> Result<Option<String>> {
        let req = GetDerivedRequest {
            repo: self.repo()?,
            revision: revision.to_owned(),
            kind: DerivedKind::GitSha1.into(),
        };
        match self.query().get_derived(self.request(req)?).await?.into_inner().value {
            Some(v1::get_derived_response::Value::GitSha1(sha)) => Ok(Some(hex::encode(sha))),
            None => Ok(None),
            Some(_) => Err(Error::Protocol("GetDerived returned another kind".into())),
        }
    }

    /// 代数与第一父提交链深度（深度 0 表示未知）。
    pub async fn generation(&self, revision: &str) -> Result<(u64, u64)> {
        let req = GetDerivedRequest {
            repo: self.repo()?,
            revision: revision.to_owned(),
            kind: DerivedKind::Generation.into(),
        };
        match self.query().get_derived(self.request(req)?).await?.into_inner().value {
            Some(v1::get_derived_response::Value::Generation(g)) => Ok((g.generation, g.first_parent_depth)),
            _ => Err(Error::Protocol("GetDerived returned no generation".into())),
        }
    }

    /// 两个版本之间的文件差异；`from` 为 None 时相对空树。
    pub async fn diff(&self, from: Option<&str>, to: &str, path: Option<&str>) -> Result<Vec<FileChangeInfo>> {
        let req = DiffRequest {
            repo: self.repo()?,
            from: from.unwrap_or_default().to_owned(),
            to: to.to_owned(),
            path: path.unwrap_or_default().to_owned(),
        };
        let mut stream = self.query().diff(self.request(req)?).await?.into_inner();
        let mut files = Vec::new();
        while let Some(batch) = stream.message().await? {
            files.extend(batch.files);
        }
        files
            .into_iter()
            .map(|f| {
                Ok(FileChangeInfo {
                    old: entry(f.old)?,
                    new: entry(f.new)?,
                    path: f.path,
                })
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sig() -> v1::Signature {
        v1::Signature {
            name: "a".into(),
            email: "e".into(),
            time_ms: 1,
            tz_offset_minutes: 2,
        }
    }

    #[test]
    fn item_converts_and_validates() {
        let id = Bytes::copy_from_slice(&ObjectId::hash(scion_object::ObjectKind::Changeset, b"x").to_bytes());
        let tree = Bytes::copy_from_slice(&ObjectId::hash(scion_object::ObjectKind::Tree, b"t").to_bytes());
        let ok = v1::LogEntry {
            changeset_id: id.clone(),
            parents: vec![id.clone()],
            author: Some(sig()),
            committer: Some(sig()),
            message: "m".into(),
            generation: 3,
            root_tree: tree,
            extensions: [("k".to_string(), Bytes::from_static(b"v"))].into_iter().collect(),
        };
        let it = item(ok.clone()).expect("ok");
        assert_eq!(
            (it.generation, it.parents.len(), it.author.tz_offset_minutes),
            (3, 1, 2)
        );
        assert_eq!(it.extensions["k"].as_ref(), b"v");
        assert!(matches!(
            item(v1::LogEntry {
                author: None,
                ..ok.clone()
            }),
            Err(Error::Protocol(_))
        ));
        assert!(matches!(
            item(v1::LogEntry {
                parents: vec![Bytes::from_static(b"x")],
                ..ok.clone()
            }),
            Err(Error::Protocol(_))
        ));
        assert!(matches!(
            item(v1::LogEntry {
                root_tree: Bytes::new(),
                ..ok
            }),
            Err(Error::Protocol(_))
        ));
    }

    #[test]
    fn expand_validates_ranges() {
        let r = |start, count, idx| v1::BlameRange {
            start_line: start,
            line_count: count,
            commit_index: idx,
        };
        assert_eq!(expand(&[r(1, 2, 0), r(3, 1, 1)], 2).expect("ok"), vec![0, 0, 1]);
        assert_eq!(expand(&[], 0).expect("empty"), Vec::<usize>::new());
        assert!(matches!(expand(&[r(2, 1, 0)], 1), Err(Error::Protocol(_))), "gap");
        assert!(matches!(expand(&[r(1, 1, 3)], 1), Err(Error::Protocol(_))), "bad index");
    }

    #[test]
    fn entry_converts() {
        assert_eq!(entry(None).expect("none"), None);
        let e = v1::TreeEntry {
            name: "f".into(),
            mode: v1::EntryMode::File.into(),
            id: Bytes::copy_from_slice(&ObjectId::of_blob(b"x").to_bytes()),
            size: 1,
        };
        assert_eq!(entry(Some(e.clone())).expect("ok").map(|d| d.name), Some("f".into()));
        assert!(entry(Some(v1::TreeEntry { mode: 0, ..e })).is_err());
    }
}
