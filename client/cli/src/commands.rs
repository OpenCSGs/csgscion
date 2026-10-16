//! 各子命令的实现。每个命令返回可渲染为文本或 JSON 的结果。

use std::path::{Path, PathBuf};
use std::time::Duration;

use bytes::Bytes;
use scion_client::{Auth, Client, EntryMode};
use scion_wc::{Status, SyncOutcome, WorkingCopy};
use serde::Serialize;

use crate::output::{CliError, DoctorOut, Render, doctor_out};

pub const DEFAULT_ENDPOINT: &str = "http://127.0.0.1:7080";
const DEFAULT_REF: &str = "main";

/// 命令行给出的全局选项。
#[derive(Debug, Clone, Default)]
pub struct Ctx {
    pub endpoint: Option<String>,
    pub repo: Option<String>,
    pub cwd: PathBuf,
}

impl Ctx {
    /// 当前目录所在的工作副本（如果有）。
    fn working_copy(&self) -> Option<WorkingCopy> {
        WorkingCopy::open(&self.cwd).ok()
    }

    /// 服务端地址：命令行或环境变量 > 工作副本记录 > 默认值。
    fn endpoint_or(&self, wc: Option<&WorkingCopy>) -> String {
        self.endpoint
            .clone()
            .or_else(|| wc.map(|w| w.state().endpoint.clone()))
            .unwrap_or_else(|| DEFAULT_ENDPOINT.to_owned())
    }
}

pub async fn connect(endpoint: &str, repo: Option<&str>) -> Result<Client, CliError> {
    let mut b = Client::builder()
        .endpoint(endpoint)
        .auth(Auth::from_env())
        .client_name("scn")
        .connect_timeout(Duration::from_secs(5));
    if let Some(r) = repo {
        b = b.repo(r);
    }
    Ok(b.build().await?)
}

/// 打开当前工作副本并连接到它记录的服务端。
async fn open_wc(ctx: &Ctx) -> Result<(WorkingCopy, Client), CliError> {
    let wc = WorkingCopy::open(&ctx.cwd)?;
    let client = connect(&ctx.endpoint_or(Some(&wc)), Some(&wc.state().repo)).await?;
    Ok((wc, client))
}

pub async fn doctor(ctx: &Ctx) -> Result<DoctorOut, CliError> {
    let endpoint = ctx.endpoint_or(ctx.working_copy().as_ref());
    let client = connect(&endpoint, None).await?;
    Ok(doctor_out(&endpoint, client.server_info()))
}

/// 解析 `Name <email>`。
pub fn parse_author(s: &str) -> Result<(String, String), CliError> {
    let bad = || CliError::Usage(format!("invalid author {s:?}; expected \"Name <email>\""));
    let (name, rest) = s.split_once('<').ok_or_else(bad)?;
    let email = rest.strip_suffix('>').ok_or_else(bad)?.trim();
    let name = name.trim();
    if name.is_empty() || email.is_empty() {
        return Err(bad());
    }
    Ok((name.to_owned(), email.to_owned()))
}

fn short(id: &str) -> &str {
    &id[..id.len().min(12)]
}

// ---------- clone ----------

#[derive(Debug, Serialize)]
pub struct CloneOut {
    pub schema: &'static str,
    pub repo: String,
    pub path: String,
    #[serde(rename = "ref")]
    pub reference: String,
    pub base: Option<String>,
}

impl Render for CloneOut {
    fn text(&self) -> String {
        match &self.base {
            Some(b) => format!(
                "cloned {} ({} at {}) into {}",
                self.repo,
                self.reference,
                short(b),
                self.path
            ),
            None => format!(
                "cloned empty repository {} into {} ({} does not exist yet)",
                self.repo, self.path, self.reference
            ),
        }
    }
}

/// 默认目录名：仓库名的最后一段。
pub fn default_dir(repo: &str) -> &str {
    repo.rsplit('/').next().unwrap_or(repo)
}

pub async fn clone(ctx: &Ctx, repo: &str, dir: Option<&Path>, reference: &str) -> Result<CloneOut, CliError> {
    let endpoint = ctx.endpoint.clone().unwrap_or_else(|| DEFAULT_ENDPOINT.to_owned());
    let client = connect(&endpoint, Some(repo)).await?;
    let target = ctx.cwd.join(dir.unwrap_or_else(|| Path::new(default_dir(repo))));
    let wc = WorkingCopy::clone(&client, &endpoint, repo, reference, &target).await?;
    Ok(CloneOut {
        schema: "scion.cli.clone/v1",
        repo: repo.to_owned(),
        path: target.display().to_string(),
        reference: reference.to_owned(),
        base: wc.state().base.clone(),
    })
}

// ---------- status ----------

#[derive(Debug, Serialize)]
pub struct StatusOut {
    pub schema: &'static str,
    #[serde(rename = "ref")]
    pub reference: String,
    pub base: Option<String>,
    /// 已提交未合入的提交。
    pub pending: Option<String>,
    pub added: Vec<String>,
    pub modified: Vec<String>,
    pub deleted: Vec<String>,
}

impl Render for StatusOut {
    fn text(&self) -> String {
        let mut lines = Vec::new();
        lines.extend(self.added.iter().map(|p| format!("A {p}")));
        lines.extend(self.modified.iter().map(|p| format!("M {p}")));
        lines.extend(self.deleted.iter().map(|p| format!("D {p}")));
        if lines.is_empty() {
            lines.push("nothing to commit".to_owned());
        }
        if let Some(p) = &self.pending {
            lines.push(format!("committed, not landed: {} (run `scn land`)", short(p)));
        }
        lines.join("\n")
    }
}

pub fn status_out(wc: &WorkingCopy, s: Status) -> StatusOut {
    StatusOut {
        schema: "scion.cli.status/v1",
        reference: wc.state().reference.clone(),
        base: wc.state().base.clone(),
        pending: wc.state().head.clone(),
        added: s.added,
        modified: s.modified,
        deleted: s.deleted,
    }
}

pub fn status(ctx: &Ctx) -> Result<StatusOut, CliError> {
    let wc = WorkingCopy::open(&ctx.cwd)?;
    let s = wc.status()?;
    Ok(status_out(&wc, s))
}

// ---------- commit / land / sync ----------

#[derive(Debug, Serialize)]
pub struct CommitOut {
    pub schema: &'static str,
    /// 没有改动时为 None。
    pub changeset: Option<String>,
    pub generation: Option<u64>,
}

impl Render for CommitOut {
    fn text(&self) -> String {
        match &self.changeset {
            Some(c) => format!("committed {} (run `scn land` to publish)", short(c)),
            None => "nothing to commit".to_owned(),
        }
    }
}

pub async fn commit(ctx: &Ctx, message: &str, author: Option<&str>) -> Result<CommitOut, CliError> {
    let author = author.map(parse_author).transpose()?;
    let (mut wc, client) = open_wc(ctx).await?;
    let created = wc
        .commit(&client, message, author.as_ref().map(|(n, e)| (n.as_str(), e.as_str())))
        .await?;
    Ok(CommitOut {
        schema: "scion.cli.commit/v1",
        changeset: created.map(|c| c.id.to_string()),
        generation: created.map(|c| c.generation),
    })
}

#[derive(Debug, Serialize)]
pub struct LandOut {
    pub schema: &'static str,
    #[serde(rename = "ref")]
    pub reference: String,
    pub tip: String,
    pub version: u64,
}

impl Render for LandOut {
    fn text(&self) -> String {
        format!(
            "landed {} on {} (version {})",
            short(&self.tip),
            self.reference,
            self.version
        )
    }
}

pub async fn land(ctx: &Ctx) -> Result<LandOut, CliError> {
    let (mut wc, client) = open_wc(ctx).await?;
    let landed = wc.land(&client).await?;
    Ok(LandOut {
        schema: "scion.cli.land/v1",
        reference: wc.state().reference.clone(),
        tip: landed.tip.to_string(),
        version: landed.version,
    })
}

#[derive(Debug, Serialize)]
pub struct SyncOut {
    pub schema: &'static str,
    /// up_to_date | updated | rebased
    pub outcome: &'static str,
    pub base: Option<String>,
    pub pending: Option<String>,
    pub written: usize,
    pub removed: usize,
}

impl Render for SyncOut {
    fn text(&self) -> String {
        let base = self.base.as_deref().map(short).unwrap_or("-");
        match self.outcome {
            "up_to_date" => "already up to date".to_owned(),
            "rebased" => format!(
                "rebased your change onto {base} ({} written, {} removed); run `scn land`",
                self.written, self.removed
            ),
            _ => format!("updated to {base} ({} written, {} removed)", self.written, self.removed),
        }
    }
}

pub fn sync_out(wc: &WorkingCopy, o: &SyncOutcome) -> SyncOut {
    let (outcome, written, removed) = match o {
        SyncOutcome::UpToDate => ("up_to_date", 0, 0),
        SyncOutcome::Updated { written, removed, .. } => ("updated", *written, *removed),
        SyncOutcome::Rebased { written, removed, .. } => ("rebased", *written, *removed),
    };
    SyncOut {
        schema: "scion.cli.sync/v1",
        outcome,
        base: wc.state().base.clone(),
        pending: wc.state().head.clone(),
        written,
        removed,
    }
}

pub async fn sync(ctx: &Ctx) -> Result<SyncOut, CliError> {
    let (mut wc, client) = open_wc(ctx).await?;
    let o = wc.sync(&client).await?;
    Ok(sync_out(&wc, &o))
}

// ---------- cat / ls（不需要工作副本） ----------

/// 拆分 `rev:path`；没有 `:` 时整个视为路径，rev 用默认值。
pub fn split_spec<'a>(spec: &'a str, default_rev: &'a str) -> (&'a str, &'a str) {
    match spec.split_once(':') {
        Some((rev, path)) if !rev.is_empty() => (rev, path),
        Some((_, path)) => (default_rev, path),
        None => (default_rev, spec),
    }
}

/// 不需要工作副本的命令：仓库来自 --repo 或当前工作副本。
async fn query_client(ctx: &Ctx) -> Result<(Client, String), CliError> {
    let wc = ctx.working_copy();
    let repo = ctx
        .repo
        .clone()
        .or_else(|| wc.as_ref().map(|w| w.state().repo.clone()))
        .ok_or_else(|| CliError::Usage("--repo is required outside a working copy".into()))?;
    let default_rev = wc
        .as_ref()
        .map_or_else(|| DEFAULT_REF.to_owned(), |w| w.state().reference.clone());
    Ok((connect(&ctx.endpoint_or(wc.as_ref()), Some(&repo)).await?, default_rev))
}

#[derive(Debug, Serialize)]
pub struct CatOut {
    pub schema: &'static str,
    pub revision: String,
    pub path: String,
    pub size: usize,
    /// 内容不是合法 UTF-8 时为 true，此时 `content` 为有损转换结果。
    pub binary: bool,
    pub content: String,
    #[serde(skip)]
    pub data: Bytes,
}

impl Render for CatOut {
    fn text(&self) -> String {
        self.content.clone()
    }
}

pub async fn cat(ctx: &Ctx, spec: &str) -> Result<CatOut, CliError> {
    let (client, default_rev) = query_client(ctx).await?;
    let (rev, path) = split_spec(spec, &default_rev);
    let data = client.read_file(rev, path).await?;
    let binary = std::str::from_utf8(&data).is_err();
    Ok(CatOut {
        schema: "scion.cli.cat/v1",
        revision: rev.to_owned(),
        path: path.to_owned(),
        size: data.len(),
        binary,
        content: String::from_utf8_lossy(&data).into_owned(),
        data,
    })
}

#[derive(Debug, Serialize)]
pub struct LsEntry {
    pub name: String,
    /// file | executable | symlink | dir
    pub kind: &'static str,
    pub size: u64,
    pub id: String,
}

#[derive(Debug, Serialize)]
pub struct LsOut {
    pub schema: &'static str,
    pub revision: String,
    pub path: String,
    pub entries: Vec<LsEntry>,
}

impl Render for LsOut {
    fn text(&self) -> String {
        self.entries
            .iter()
            .map(|e| match e.kind {
                "dir" => format!("{:>10}  {}/", "-", e.name),
                "executable" => format!("{:>10}  {}*", e.size, e.name),
                "symlink" => format!("{:>10}  {}@", e.size, e.name),
                _ => format!("{:>10}  {}", e.size, e.name),
            })
            .collect::<Vec<_>>()
            .join("\n")
    }
}

pub fn kind(m: EntryMode) -> &'static str {
    match m {
        EntryMode::File => "file",
        EntryMode::Executable => "executable",
        EntryMode::Symlink => "symlink",
        EntryMode::Tree => "dir",
    }
}

pub async fn ls(ctx: &Ctx, spec: Option<&str>) -> Result<LsOut, CliError> {
    let (client, default_rev) = query_client(ctx).await?;
    let (rev, path) = split_spec(spec.unwrap_or(""), &default_rev);
    let entries = client.list_dir(rev, path).await?;
    Ok(LsOut {
        schema: "scion.cli.ls/v1",
        revision: rev.to_owned(),
        path: path.to_owned(),
        entries: entries
            .into_iter()
            .map(|e| LsEntry {
                name: e.name,
                kind: kind(e.mode),
                size: e.size,
                id: e.id.to_string(),
            })
            .collect(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_author_cases() {
        assert_eq!(
            parse_author("Ada Lovelace <ada@example.com>").expect("ok"),
            ("Ada Lovelace".into(), "ada@example.com".into())
        );
        for bad in ["Ada", "<a@b>", "Ada <>", "Ada <a@b", ""] {
            assert!(matches!(parse_author(bad), Err(CliError::Usage(_))), "{bad:?}");
        }
    }

    #[test]
    fn split_spec_cases() {
        assert_eq!(split_spec("main:src/a.rs", "x"), ("main", "src/a.rs"));
        assert_eq!(split_spec("src/a.rs", "main"), ("main", "src/a.rs"));
        assert_eq!(split_spec(":src", "main"), ("main", "src"));
        assert_eq!(split_spec("", "main"), ("main", ""));
        assert_eq!(split_spec("c1abc:", "main"), ("c1abc", ""));
    }

    #[test]
    fn default_dir_is_last_segment() {
        assert_eq!(default_dir("opencsg/model"), "model");
        assert_eq!(default_dir("mono"), "mono");
    }

    #[test]
    fn short_truncates() {
        assert_eq!(short("c1abcdefghijklmn"), "c1abcdefghij");
        assert_eq!(short("c1"), "c1");
    }

    #[test]
    fn kind_all() {
        assert_eq!(kind(EntryMode::File), "file");
        assert_eq!(kind(EntryMode::Executable), "executable");
        assert_eq!(kind(EntryMode::Symlink), "symlink");
        assert_eq!(kind(EntryMode::Tree), "dir");
    }

    #[test]
    fn render_texts() {
        let c = CloneOut {
            schema: "s",
            repo: "r".into(),
            path: "/p".into(),
            reference: "main".into(),
            base: None,
        };
        assert!(c.text().contains("empty repository"));
        let c = CloneOut {
            base: Some("c1abcdefghijklmnop".into()),
            ..c
        };
        assert!(c.text().contains("main at c1abcdefghij"));

        let s = StatusOut {
            schema: "s",
            reference: "main".into(),
            base: None,
            pending: None,
            added: vec![],
            modified: vec![],
            deleted: vec![],
        };
        assert_eq!(s.text(), "nothing to commit");
        let s = StatusOut {
            added: vec!["a".into()],
            modified: vec!["m".into()],
            deleted: vec!["d".into()],
            pending: Some("c1pending0000".into()),
            ..s
        };
        assert_eq!(
            s.text(),
            "A a\nM m\nD d\ncommitted, not landed: c1pending000 (run `scn land`)"
        );

        assert_eq!(
            CommitOut {
                schema: "s",
                changeset: None,
                generation: None
            }
            .text(),
            "nothing to commit"
        );
        assert!(
            CommitOut {
                schema: "s",
                changeset: Some("c1x".into()),
                generation: Some(1)
            }
            .text()
            .contains("scn land")
        );
        assert!(
            LandOut {
                schema: "s",
                reference: "main".into(),
                tip: "c1t".into(),
                version: 3
            }
            .text()
            .contains("version 3")
        );

        let mut so = SyncOut {
            schema: "s",
            outcome: "up_to_date",
            base: None,
            pending: None,
            written: 0,
            removed: 0,
        };
        assert_eq!(so.text(), "already up to date");
        so.outcome = "updated";
        assert!(so.text().starts_with("updated to -"));
        so.outcome = "rebased";
        so.base = Some("c1base".into());
        assert!(so.text().contains("rebased your change onto c1base"));

        let ls = LsOut {
            schema: "s",
            revision: "main".into(),
            path: String::new(),
            entries: vec![
                LsEntry {
                    name: "d".into(),
                    kind: "dir",
                    size: 0,
                    id: "t1".into(),
                },
                LsEntry {
                    name: "x".into(),
                    kind: "executable",
                    size: 3,
                    id: "b1".into(),
                },
                LsEntry {
                    name: "l".into(),
                    kind: "symlink",
                    size: 1,
                    id: "b1".into(),
                },
                LsEntry {
                    name: "f".into(),
                    kind: "file",
                    size: 2,
                    id: "b1".into(),
                },
            ],
        };
        let t = ls.text();
        assert!(t.contains("d/") && t.contains("x*") && t.contains("l@") && t.ends_with("f"));

        let cat = CatOut {
            schema: "s",
            revision: "main".into(),
            path: "a".into(),
            size: 2,
            binary: false,
            content: "hi".into(),
            data: Bytes::from_static(b"hi"),
        };
        assert_eq!(cat.text(), "hi");
    }

    #[test]
    fn endpoint_precedence() {
        let ctx = Ctx {
            endpoint: Some("http://flag".into()),
            ..Default::default()
        };
        assert_eq!(ctx.endpoint_or(None), "http://flag");
        assert_eq!(Ctx::default().endpoint_or(None), DEFAULT_ENDPOINT);
    }

    #[tokio::test]
    async fn query_outside_working_copy_needs_repo() {
        let dir = tempfile::tempdir().expect("tempdir");
        let ctx = Ctx {
            cwd: dir.path().to_path_buf(),
            ..Default::default()
        };
        assert!(matches!(cat(&ctx, "main:a").await, Err(CliError::Usage(_))));
        assert!(matches!(ls(&ctx, None).await, Err(CliError::Usage(_))));
        assert!(matches!(
            status(&ctx),
            Err(CliError::Wc(scion_wc::Error::NotAWorkingCopy(_)))
        ));
        assert!(matches!(commit(&ctx, "m", Some("bad")).await, Err(CliError::Usage(_))));
    }
}
