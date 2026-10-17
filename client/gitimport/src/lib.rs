//! 把 git 历史导入 Scion（服务端设计文档 §14 M1「git 导入」）。
//!
//! 只通过公开协议写入：每个 git 提交变成一个 Scion 提交，delta 为相对第一父提交的改动。
//! 结果是确定的：作者、提交者、时间、时区、说明原样保留，`change_id` 取自 git sha，
//! git sha 记在扩展字段 `git.sha1` 中。因此重复导入同一历史得到相同的提交 id。

mod git;

use std::collections::HashMap;
use std::path::Path;

use scion_client::{Client, EntryMode, ObjectId, Signature};
use thiserror::Error;

pub use git::{Change, Commit, GitMode, Ident, parse_commit, parse_ident, parse_raw_diff};

/// 记录 git 提交 sha 的扩展字段。
pub const GIT_SHA1_EXTENSION: &str = "git.sha1";

#[derive(Debug, Error)]
pub enum Error {
    #[error("{0}")]
    Git(String),
    #[error("cannot parse git output: {0}")]
    Parse(String),
    #[error("path is not valid UTF-8: {0:?}")]
    NonUtf8Path(String),
    #[error("ref {reference:?} already points to {current}, which is not in the imported first-parent history")]
    Diverged { reference: String, current: ObjectId },
    #[error(transparent)]
    Client(#[from] scion_client::Error),
}

pub type Result<T> = std::result::Result<T, Error>;

/// 导入结果。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Summary {
    /// 导入的提交数（含此前已导入、本次复用的）。
    pub commits: usize,
    /// 因为是子模块而跳过的条目数。
    pub skipped_gitlinks: usize,
    /// git sha → Scion 提交 id。
    pub mapping: HashMap<String, ObjectId>,
    /// 目标 ref 最终指向的提交；git 历史为空时为 None。
    pub tip: Option<ObjectId>,
}

fn signature(i: &Ident) -> Signature {
    Signature {
        name: i.name.clone(),
        email: i.email.clone(),
        time_ms: i.time_ms,
        tz_offset_minutes: i.tz_offset_minutes,
    }
}

fn entry_mode(m: GitMode) -> Option<EntryMode> {
    match m {
        GitMode::File => Some(EntryMode::File),
        GitMode::Executable => Some(EntryMode::Executable),
        GitMode::Symlink => Some(EntryMode::Symlink),
        GitMode::Gitlink => None,
    }
}

/// 由 git sha 推导 change_id 与扩展字段值。
fn sha_bytes(sha: &str) -> Result<([u8; 16], Vec<u8>)> {
    let raw = hex::decode(sha).map_err(|_| Error::Parse(format!("bad commit sha {sha:?}")))?;
    let change_id: [u8; 16] = raw
        .get(..16)
        .and_then(|s| s.try_into().ok())
        .ok_or_else(|| Error::Parse(format!("short sha {sha:?}")))?;
    Ok((change_id, raw))
}

/// 导入 `repo_dir` 中 `git_ref` 可达的全部提交，并把 Scion 的 `target_ref` 快进到最新提交。
///
/// `progress` 在每个提交导入后被调用（已完成数, 总数）。
pub async fn import(
    client: &Client,
    repo_dir: &Path,
    git_ref: &str,
    target_ref: &str,
    mut progress: impl FnMut(usize, usize),
) -> Result<Summary> {
    let repo = git::Repo::open(repo_dir)?;
    let commits = repo.commits(git_ref)?;
    let mut cat = repo.cat_file()?;
    let mut mapping: HashMap<String, ObjectId> = HashMap::with_capacity(commits.len());
    let mut first_parent_of: HashMap<ObjectId, Option<ObjectId>> = HashMap::with_capacity(commits.len());
    let mut skipped_gitlinks = 0;

    for (i, sha) in commits.iter().enumerate() {
        let commit = cat.commit(sha)?;
        let parents: Vec<ObjectId> = commit
            .parents
            .iter()
            .map(|p| {
                mapping
                    .get(p)
                    .copied()
                    .ok_or_else(|| Error::Parse(format!("parent {p} of {sha} not imported yet")))
            })
            .collect::<Result<_>>()?;
        let changes = repo.changes(sha, commit.parents.first().map(String::as_str))?;
        let (change_id, raw_sha) = sha_bytes(sha)?;

        let mut b = client
            .changeset(parents.first().copied())
            .message(commit.message.clone())
            .author_signature(signature(&commit.author))
            .committer_signature(signature(&commit.committer))
            .change_id(change_id)
            .extension(GIT_SHA1_EXTENSION, raw_sha);
        for p in parents.iter().skip(1) {
            b = b.parent(*p);
        }
        for c in &changes {
            b = match &c.new {
                None => b.delete(&c.path),
                Some((mode, blob)) => match entry_mode(*mode) {
                    Some(m) => b.put_with_mode(&c.path, m, cat.blob(blob)?),
                    None => {
                        skipped_gitlinks += 1;
                        b
                    }
                },
            };
        }
        let created = b.create().await?;
        first_parent_of.insert(created.id, parents.first().copied());
        mapping.insert(sha.clone(), created.id);
        progress(i + 1, commits.len());
    }

    let tip = commits.last().and_then(|sha| mapping.get(sha).copied());
    if let Some(tip) = tip {
        land_chain(client, target_ref, tip, &first_parent_of).await?;
    }
    Ok(Summary {
        commits: commits.len(),
        skipped_gitlinks,
        mapping,
        tip,
    })
}

/// 把 ref 快进到 `tip`：ref 当前提交必须在 `tip` 的第一父提交链上（否则 git 历史被改写过）。
async fn land_chain(
    client: &Client,
    target_ref: &str,
    tip: ObjectId,
    first_parent_of: &HashMap<ObjectId, Option<ObjectId>>,
) -> Result<()> {
    let current = client.resolve_ref(target_ref).await?.map(|t| t.changeset);
    let mut chain = Vec::new();
    let mut cur = Some(tip);
    while let Some(c) = cur {
        if Some(c) == current {
            break;
        }
        chain.push(c);
        cur = first_parent_of.get(&c).copied().flatten();
    }
    if let Some(cur_tip) = current {
        if cur.is_none() {
            return Err(Error::Diverged {
                reference: target_ref.to_owned(),
                current: cur_tip,
            });
        }
    }
    if !chain.is_empty() {
        // 只允许快进：导入的历史必须原样进入 ref，期间 ref 被别人移动时报错而不是 rebase
        client.land_fast_forward(target_ref, &tip).await?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sha_bytes_cases() {
        let sha = "0123456789abcdef0123456789abcdef01234567";
        let (cid, raw) = sha_bytes(sha).expect("ok");
        assert_eq!(raw.len(), 20);
        assert_eq!(&cid[..], &raw[..16]);
        assert!(matches!(sha_bytes("zz"), Err(Error::Parse(_))));
        assert!(matches!(sha_bytes("0123"), Err(Error::Parse(_))));
    }

    #[test]
    fn entry_mode_mapping() {
        assert_eq!(entry_mode(GitMode::File), Some(EntryMode::File));
        assert_eq!(entry_mode(GitMode::Executable), Some(EntryMode::Executable));
        assert_eq!(entry_mode(GitMode::Symlink), Some(EntryMode::Symlink));
        assert_eq!(entry_mode(GitMode::Gitlink), None);
    }

    #[test]
    fn signature_copies_fields() {
        let i = Ident {
            name: "n".into(),
            email: "e".into(),
            time_ms: 5,
            tz_offset_minutes: -60,
        };
        let s = signature(&i);
        assert_eq!(
            (s.name.as_str(), s.email.as_str(), s.time_ms, s.tz_offset_minutes),
            ("n", "e", 5, -60)
        );
    }

    #[test]
    fn error_display() {
        let id = ObjectId::hash(scion_object::ObjectKind::Changeset, b"x");
        let e = Error::Diverged {
            reference: "main".into(),
            current: id,
        };
        assert!(e.to_string().contains("not in the imported"));
        assert!(Error::NonUtf8Path("p".into()).to_string().contains("UTF-8"));
        assert!(Error::Parse("x".into()).to_string().contains("parse"));
        assert_eq!(Error::Git("boom".into()).to_string(), "boom");
    }
}
