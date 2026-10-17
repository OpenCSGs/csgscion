//! 运维命令：redaction、租约、fsck（直接读写存储，与 `scion server` 使用相同的存储参数）。

use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use scion_api::redaction::Redactor;
use scion_object::ObjectId;
use scion_store::{BlobStore, MetaStore, Redaction};

use crate::fsck::{FsckOptions, FsckReport, fsck};

#[derive(Debug, clap::Args)]
pub struct RedactArgs {
    /// 仓库。
    #[arg(long)]
    pub repo: String,
    /// 要 redact 的 blob id（文本形式 b1…）。
    #[arg(value_name = "ID")]
    pub ids: Vec<String>,
    /// 用 REV:PATH 指定文件，可以重复。
    #[arg(long = "path", value_name = "REV:PATH")]
    pub paths: Vec<String>,
    /// 原因（记录在案）。
    #[arg(long)]
    pub reason: String,
    /// 同时 redact 对象索引中有这些对象的所有仓库。
    #[arg(long)]
    pub all_repos: bool,
    /// 记录的操作人；默认取 $USER。
    #[arg(long, value_name = "NAME")]
    pub by: Option<String>,
}

#[derive(Debug, clap::Args)]
pub struct PurgeArgs {
    pub redaction_id: String,
    /// 宽限期：这段时间之后才物理删除，期间可以 unredact。
    #[arg(long, value_name = "DURATION", default_value = "24h", value_parser = scion_api::scion_auth::config::parse_duration)]
    pub grace: Duration,
    /// 不等宽限期、也不等运行中的服务端：立即删除（不可撤销）。
    #[arg(long, conflicts_with = "grace")]
    pub now: bool,
}

#[derive(Debug, clap::Subcommand)]
pub enum LeaseCmd {
    /// 列出租约（合入 leader 等）。
    Ls,
}

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| i64::try_from(d.as_millis()).unwrap_or(i64::MAX))
        .unwrap_or(0)
}

fn status_err(s: tonic::Status) -> anyhow::Error {
    anyhow::anyhow!("{}", s.message())
}

/// 一条 redaction 记录的展示。
fn describe(r: &Redaction) -> String {
    let state = match (r.active, r.purged_at_ms, r.purge_after_ms) {
        (false, _, _) => "withdrawn".to_string(),
        (true, Some(t), _) => format!("purged at {t}"),
        (true, None, Some(t)) => format!("purge after {t}"),
        (true, None, None) => "redacted".to_string(),
    };
    format!(
        "{}  {}  {}  {}  by {} at {}: {}",
        r.redaction_id, r.repo, r.id, state, r.created_by, r.created_at_ms, r.reason
    )
}

pub async fn redact(
    args: &RedactArgs,
    meta: Arc<dyn MetaStore>,
    blobs: Arc<dyn BlobStore>,
    env: impl Fn(&str) -> Option<String>,
) -> anyhow::Result<Vec<String>> {
    let ids = args
        .ids
        .iter()
        .map(|s| {
            s.parse::<ObjectId>()
                .with_context(|| format!("invalid object id {s:?}"))
        })
        .collect::<anyhow::Result<Vec<_>>>()?;
    let by = args
        .by
        .clone()
        .or_else(|| env("USER"))
        .unwrap_or_else(|| "admin".into());
    let (rid, recs) = Redactor::new(meta, blobs)
        .redact(
            &args.repo,
            &ids,
            &args.paths,
            &args.reason,
            &by,
            args.all_repos,
            now_ms(),
        )
        .await
        .map_err(status_err)?;
    let mut out = vec![format!("redaction {rid}: {} object(s)", recs.len())];
    out.extend(recs.iter().map(describe));
    if recs.is_empty() {
        out.push("(everything was already redacted)".into());
    }
    Ok(out)
}

pub async fn unredact(rid: &str, meta: Arc<dyn MetaStore>, blobs: Arc<dyn BlobStore>) -> anyhow::Result<Vec<String>> {
    let n = Redactor::new(meta, blobs).unredact(rid).await.map_err(status_err)?;
    Ok(vec![format!("withdrew {n} redaction record(s) of {rid}")])
}

pub async fn purge(
    args: &PurgeArgs,
    meta: Arc<dyn MetaStore>,
    blobs: Arc<dyn BlobStore>,
) -> anyhow::Result<Vec<String>> {
    let red = Redactor::new(meta, blobs);
    let grace = if args.now {
        0
    } else {
        i64::try_from(args.grace.as_millis()).unwrap_or(i64::MAX)
    };
    let now = now_ms();
    let (n, after) = red.purge(&args.redaction_id, grace, now).await.map_err(status_err)?;
    let mut out = vec![format!("purge of {n} object(s) scheduled for {after}")];
    if args.now {
        let report = red.run_purges(now).await.map_err(status_err)?;
        out.push(format!("deleted {} object(s)", report.deleted.len()));
        for (id, repos) in report.blocked {
            out.push(format!(
                "kept {id}: still readable in {} (redact it there too, or use --all-repos)",
                repos.join(", ")
            ));
        }
    }
    Ok(out)
}

pub async fn redactions(
    repo: Option<&str>,
    rid: Option<&str>,
    meta: Arc<dyn MetaStore>,
    blobs: Arc<dyn BlobStore>,
) -> anyhow::Result<Vec<String>> {
    let recs = Redactor::new(meta, blobs).list(repo, rid).await.map_err(status_err)?;
    Ok(recs.iter().map(describe).collect())
}

pub async fn lease(cmd: &LeaseCmd, meta: Arc<dyn MetaStore>) -> anyhow::Result<Vec<String>> {
    match cmd {
        LeaseCmd::Ls => {
            let now = now_ms();
            Ok(meta
                .list_leases()
                .await?
                .into_iter()
                .map(|(scope, l)| {
                    let state = if l.expires_at_ms > now {
                        format!("expires in {} ms", l.expires_at_ms - now)
                    } else {
                        "expired".to_string()
                    };
                    format!("{scope}  holder={}  token={}  {state}", l.holder, l.token)
                })
                .collect())
        }
    }
}

/// 返回输出与是否发现问题。
pub async fn run_fsck(
    opts: &FsckOptions,
    meta: Arc<dyn MetaStore>,
    blobs: Arc<dyn BlobStore>,
) -> anyhow::Result<(Vec<String>, bool)> {
    let report: FsckReport = fsck(&meta, &blobs, opts).await?;
    Ok((report.lines(), report.ok()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fsck::tests::fixture;

    fn no_env(_: &str) -> Option<String> {
        None
    }

    #[tokio::test]
    async fn redaction_commands() {
        let fx = fixture().await;
        let args = RedactArgs {
            repo: "r".into(),
            ids: vec![],
            paths: vec!["main~0:docs/secret.txt".into()],
            reason: "leaked password".into(),
            all_repos: false,
            by: None,
        };
        // main~0 不是合法的 revision
        assert!(redact(&args, fx.meta.clone(), fx.blobs.clone(), no_env).await.is_err());
        let first = scion_query::all_records(fx.meta.as_ref(), "r", 16)
            .await
            .expect("records")[0]
            .id;
        let args = RedactArgs {
            paths: vec![format!("{first}:docs/secret.txt")],
            ..args
        };
        let out = redact(&args, fx.meta.clone(), fx.blobs.clone(), |k| {
            (k == "USER").then(|| "ops".into())
        })
        .await
        .expect("redact");
        assert!(
            out[0].starts_with("redaction rd_") && out[0].ends_with("1 object(s)"),
            "{out:?}"
        );
        assert!(out[1].contains("by ops") && out[1].contains("leaked password") && out[1].contains("redacted"));
        let rid = out[0]
            .split_whitespace()
            .nth(1)
            .expect("id")
            .trim_end_matches(':')
            .to_string();
        let again = redact(&args, fx.meta.clone(), fx.blobs.clone(), no_env)
            .await
            .expect("again");
        assert!(again.last().expect("line").contains("already redacted"));
        let bad = RedactArgs {
            ids: vec!["not-an-id".into()],
            paths: vec![],
            ..args
        };
        assert!(redact(&bad, fx.meta.clone(), fx.blobs.clone(), no_env).await.is_err());

        let listed = redactions(Some("r"), None, fx.meta.clone(), fx.blobs.clone())
            .await
            .expect("ls");
        assert_eq!(listed.len(), 1);
        let scheduled = purge(
            &PurgeArgs {
                redaction_id: rid.clone(),
                grace: Duration::from_secs(3600),
                now: false,
            },
            fx.meta.clone(),
            fx.blobs.clone(),
        )
        .await
        .expect("purge");
        assert_eq!(scheduled.len(), 1);
        let listed = redactions(None, Some(&rid), fx.meta.clone(), fx.blobs.clone())
            .await
            .expect("ls");
        assert!(listed[0].contains("purge after"), "{listed:?}");
        assert_eq!(
            unredact(&rid, fx.meta.clone(), fx.blobs.clone())
                .await
                .expect("unredact"),
            [format!("withdrew 1 redaction record(s) of {rid}")]
        );
        let listed = redactions(None, None, fx.meta.clone(), fx.blobs.clone())
            .await
            .expect("ls");
        assert!(listed[0].contains("withdrawn"));
        assert!(unredact("rd_nope", fx.meta.clone(), fx.blobs.clone()).await.is_err());
    }

    #[tokio::test]
    async fn purge_now_deletes_or_explains_why_not() {
        let fx = fixture().await;
        let secret = scion_object::ObjectId::of_blob(b"password=hunter2");
        fx.store.create_repo("fork").await.expect("fork");
        fx.store
            .index_objects("fork", &[scion_store::IndexedObject { id: secret, size: 16 }])
            .await
            .expect("index");
        let red = Redactor::new(fx.meta.clone(), fx.blobs.clone());
        let (rid, _) = red
            .redact("r", &[secret], &[], "leak", "root", false, 1)
            .await
            .expect("redact");
        let args = |rid: &str| PurgeArgs {
            redaction_id: rid.into(),
            grace: Duration::from_secs(1),
            now: true,
        };
        let out = purge(&args(&rid), fx.meta.clone(), fx.blobs.clone())
            .await
            .expect("purge");
        assert!(
            out[1] == "deleted 0 object(s)" && out[2].contains("still readable in fork"),
            "{out:?}"
        );
        let (rid, _) = red
            .redact("r", &[secret], &[], "leak", "root", true, 2)
            .await
            .expect("all");
        let out = purge(&args(&rid), fx.meta.clone(), fx.blobs.clone())
            .await
            .expect("purge");
        assert_eq!(out[1], "deleted 1 object(s)");
        assert!(!fx.blobs.exists(&secret).await.expect("exists"));
        let listed = redactions(Some("fork"), None, fx.meta.clone(), fx.blobs.clone())
            .await
            .expect("ls");
        assert!(listed[0].contains("purged at"), "{listed:?}");
    }

    #[tokio::test]
    async fn lease_and_fsck_commands() {
        let fx = fixture().await;
        assert!(lease(&LeaseCmd::Ls, fx.meta.clone()).await.expect("ls").is_empty());
        fx.meta
            .acquire_lease("land/r/main", "node-a", 60_000, now_ms())
            .await
            .expect("lease");
        fx.meta
            .acquire_lease("land/r/old", "node-b", 1, 0)
            .await
            .expect("lease");
        let out = lease(&LeaseCmd::Ls, fx.meta.clone()).await.expect("ls");
        assert!(
            out[0].starts_with("land/r/main  holder=node-a  token=1  expires in"),
            "{out:?}"
        );
        assert!(out[1].ends_with("expired"), "{out:?}");
        let (lines, ok) = run_fsck(&FsckOptions::default(), fx.meta.clone(), fx.blobs.clone())
            .await
            .expect("fsck");
        assert!(ok && lines.last().is_some_and(|l| l == "no problems found"));
    }
}
