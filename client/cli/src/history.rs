//! `scn log` 与 `scn diff`。

use bytes::Bytes;
use scion_client::{Client, FileChangeInfo, LogItem, ObjectId};
use scion_wc::{Mode, WorkingCopy};
use serde::Serialize;

use crate::commands::{Ctx, query_client, split_spec};
use crate::output::{CliError, Render};

/// Unix 毫秒 + 时区分钟 → `YYYY-MM-DD HH:MM +HHMM`（提交者所在时区的本地时间）。
pub fn format_time(time_ms: i64, tz_offset_minutes: i32) -> String {
    let secs = time_ms.div_euclid(1000) + i64::from(tz_offset_minutes) * 60;
    let (days, rem) = (secs.div_euclid(86_400), secs.rem_euclid(86_400));
    let (y, m, d) = civil_from_days(days);
    let sign = if tz_offset_minutes < 0 { '-' } else { '+' };
    let tz = tz_offset_minutes.unsigned_abs();
    format!(
        "{y:04}-{m:02}-{d:02} {:02}:{:02} {sign}{:02}{:02}",
        rem / 3600,
        rem % 3600 / 60,
        tz / 60,
        tz % 60
    )
}

/// 1970-01-01 起的天数 → 公历 (年, 月, 日)。算法来自 Howard Hinnant 的 `civil_from_days`。
fn civil_from_days(z: i64) -> (i64, u32, u32) {
    let z = z + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let m = if mp < 10 { mp + 3 } else { mp - 9 } as u32;
    let y = yoe + era * 400 + i64::from(m <= 2);
    (y, m, d)
}

fn short(id: &ObjectId) -> String {
    id.to_string()[..12].to_owned()
}

// ---------- log ----------

#[derive(Debug, Serialize)]
pub struct LogLine {
    pub id: String,
    pub parents: Vec<String>,
    pub author: String,
    pub email: String,
    pub time_ms: i64,
    pub tz_offset_minutes: i32,
    pub message: String,
    pub generation: u64,
}

#[derive(Debug, Serialize)]
pub struct LogOut {
    pub schema: &'static str,
    pub revision: String,
    pub path: Option<String>,
    pub entries: Vec<LogLine>,
}

impl Render for LogOut {
    fn text(&self) -> String {
        if self.entries.is_empty() {
            return "no matching changes".to_owned();
        }
        self.entries
            .iter()
            .map(|e| {
                let subject = e.message.lines().next().unwrap_or("");
                let merge = if e.parents.len() > 1 { " (merge)" } else { "" };
                format!(
                    "{}  {}  {}  {subject}{merge}",
                    &e.id[..12.min(e.id.len())],
                    format_time(e.time_ms, e.tz_offset_minutes),
                    e.author
                )
            })
            .collect::<Vec<_>>()
            .join("\n")
    }
}

fn log_line(e: LogItem) -> LogLine {
    LogLine {
        id: e.id.to_string(),
        parents: e.parents.iter().map(ToString::to_string).collect(),
        author: e.author.name,
        email: e.author.email,
        time_ms: e.committer.time_ms,
        tz_offset_minutes: e.committer.tz_offset_minutes,
        message: e.message,
        generation: e.generation,
    }
}

/// 自动翻页直到凑够 `limit` 条或历史结束。
pub async fn log(ctx: &Ctx, revision: Option<&str>, path: Option<&str>, limit: usize) -> Result<LogOut, CliError> {
    let (client, default_rev) = query_client(ctx).await?;
    let revision = revision.map_or(default_rev, str::to_owned);
    let mut entries = Vec::new();
    let mut token: Option<String> = None;
    while entries.len() < limit {
        let want = u32::try_from(limit - entries.len()).unwrap_or(u32::MAX).min(1000);
        let page = client.log(&revision, path, want, token.as_deref()).await?;
        entries.extend(page.entries.into_iter().map(log_line));
        match page.next_page_token {
            Some(t) => token = Some(t),
            None => break,
        }
    }
    entries.truncate(limit);
    Ok(LogOut {
        schema: "scion.cli.log/v1",
        revision,
        path: path.map(str::to_owned),
        entries,
    })
}

// ---------- blame ----------

#[derive(Debug, Serialize)]
pub struct BlameLineOut {
    pub line: usize,
    pub commit: String,
    pub author: String,
    pub time_ms: i64,
    pub tz_offset_minutes: i32,
    pub content: String,
}

#[derive(Debug, Serialize)]
pub struct BlameOut {
    pub schema: &'static str,
    pub revision: String,
    pub path: String,
    pub lines: Vec<BlameLineOut>,
}

impl Render for BlameOut {
    fn text(&self) -> String {
        let width = self.lines.last().map_or(1, |l| l.line.to_string().len());
        self.lines
            .iter()
            .map(|l| {
                let date = &format_time(l.time_ms, l.tz_offset_minutes)[..10];
                let author: String = l.author.chars().take(16).collect();
                format!(
                    "{} ({author:<16} {date}) {:>width$}| {}",
                    &l.commit[..12],
                    l.line,
                    l.content
                )
            })
            .collect::<Vec<_>>()
            .join("\n")
    }
}

/// 按行切分，与服务端一致：保留换行符，末尾没有换行的最后一行也算一行。
fn split_lines(data: &[u8]) -> Vec<&[u8]> {
    data.split_inclusive(|b| *b == b'\n').collect()
}

pub async fn blame(ctx: &Ctx, spec: &str) -> Result<BlameOut, CliError> {
    let (client, default_rev) = query_client(ctx).await?;
    let (rev, path) = split_spec(spec, &default_rev);
    let info = client.blame(rev, path).await?;
    let data = client.get_object(&info.blob).await?;
    let raw = split_lines(&data);
    if raw.len() != info.lines.len() {
        return Err(CliError::Client(scion_client::Error::Protocol(format!(
            "blame covers {} lines but the file has {}",
            info.lines.len(),
            raw.len()
        ))));
    }
    let lines = raw
        .iter()
        .zip(&info.lines)
        .enumerate()
        .map(|(i, (text, idx))| {
            let c = &info.commits[*idx];
            let content = String::from_utf8_lossy(text);
            BlameLineOut {
                line: i + 1,
                commit: c.id.to_string(),
                author: c.author.name.clone(),
                time_ms: c.author.time_ms,
                tz_offset_minutes: c.author.tz_offset_minutes,
                content: content.trim_end_matches(['\n', '\r']).to_owned(),
            }
        })
        .collect();
    Ok(BlameOut {
        schema: "scion.cli.blame/v1",
        revision: rev.to_owned(),
        path: path.to_owned(),
        lines,
    })
}

// ---------- diff ----------

#[derive(Debug, Serialize)]
pub struct DiffFile {
    pub path: String,
    /// A（新增）| M（修改）| D（删除）
    pub status: &'static str,
    pub old_id: Option<String>,
    pub new_id: Option<String>,
    pub old_mode: Option<&'static str>,
    pub new_mode: Option<&'static str>,
    pub binary: bool,
    /// unified diff；`--name-only` 或二进制文件时为 None。
    pub patch: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct DiffOut {
    pub schema: &'static str,
    pub from: Option<String>,
    pub to: String,
    pub files: Vec<DiffFile>,
    #[serde(skip)]
    pub name_only: bool,
}

impl Render for DiffOut {
    fn text(&self) -> String {
        if self.files.is_empty() {
            return "no differences".to_owned();
        }
        if self.name_only {
            return self
                .files
                .iter()
                .map(|f| format!("{} {}", f.status, f.path))
                .collect::<Vec<_>>()
                .join("\n");
        }
        let mut out = String::new();
        for f in &self.files {
            out.push_str(&format!("diff --scion a/{0} b/{0}\n", f.path));
            match (f.old_mode, f.new_mode) {
                (None, Some(m)) => out.push_str(&format!("new file ({m})\n")),
                (Some(m), None) => out.push_str(&format!("deleted file ({m})\n")),
                (Some(a), Some(b)) if a != b => out.push_str(&format!("mode change {a} => {b}\n")),
                _ => {}
            }
            if f.binary {
                out.push_str(&format!("Binary files a/{0} and b/{0} differ\n", f.path));
            } else if let Some(p) = &f.patch {
                out.push_str(p);
            }
        }
        out.trim_end().to_owned()
    }
}

fn mode_name(m: Mode) -> &'static str {
    match m {
        Mode::File => "file",
        Mode::Executable => "executable",
        Mode::Symlink => "symlink",
    }
}

fn entry_mode_name(m: scion_client::EntryMode) -> &'static str {
    crate::commands::kind(m)
}

fn is_binary(b: &[u8]) -> bool {
    b.contains(&0) || std::str::from_utf8(b).is_err()
}

/// 生成 unified diff；任一侧为二进制时返回 (true, None)。
pub fn render_patch(path: &str, old: Option<&[u8]>, new: Option<&[u8]>) -> (bool, Option<String>) {
    let (old, new) = (old.unwrap_or_default(), new.unwrap_or_default());
    if is_binary(old) || is_binary(new) {
        return (true, None);
    }
    let (a, b) = (String::from_utf8_lossy(old), String::from_utf8_lossy(new));
    let diff = similar::TextDiff::from_lines(a.as_ref(), b.as_ref());
    let patch = diff
        .unified_diff()
        .context_radius(3)
        .header(&format!("a/{path}"), &format!("b/{path}"))
        .to_string();
    (false, Some(patch))
}

fn status_of(old: bool, new: bool) -> &'static str {
    match (old, new) {
        (false, _) => "A",
        (_, false) => "D",
        _ => "M",
    }
}

async fn fetch(client: &Client, id: Option<ObjectId>) -> Result<Option<Bytes>, CliError> {
    match id {
        Some(id) => Ok(Some(client.get_object(&id).await?)),
        None => Ok(None),
    }
}

/// 服务端算出文件列表，客户端取内容生成 patch。
async fn remote_files(
    client: &Client,
    changes: Vec<FileChangeInfo>,
    name_only: bool,
) -> Result<Vec<DiffFile>, CliError> {
    let mut files = Vec::with_capacity(changes.len());
    for c in changes {
        let (old_id, new_id) = (c.old.as_ref().map(|e| e.id), c.new.as_ref().map(|e| e.id));
        let (binary, patch) = if name_only {
            (false, None)
        } else {
            let (o, n) = (fetch(client, old_id).await?, fetch(client, new_id).await?);
            render_patch(&c.path, o.as_deref(), n.as_deref())
        };
        files.push(DiffFile {
            status: status_of(c.old.is_some(), c.new.is_some()),
            old_id: old_id.map(|i| i.to_string()),
            new_id: new_id.map(|i| i.to_string()),
            old_mode: c.old.as_ref().map(|e| entry_mode_name(e.mode)),
            new_mode: c.new.as_ref().map(|e| entry_mode_name(e.mode)),
            binary,
            patch,
            path: c.path,
        });
    }
    Ok(files)
}

/// - 不带版本：工作副本里尚未提交的改动；
/// - 一个版本：该提交相对第一个父提交引入的改动；
/// - 两个版本：两者之间的差异。
pub async fn diff(ctx: &Ctx, revs: &[String], path: Option<&str>, name_only: bool) -> Result<DiffOut, CliError> {
    match revs {
        [] => local_diff(ctx, path, name_only).await,
        [rev] => {
            let (client, _) = query_client(ctx).await?;
            let page = client.log(rev, None, 1, None).await?;
            let entry = page
                .entries
                .into_iter()
                .next()
                .ok_or_else(|| CliError::Usage(format!("{rev} has no history")))?;
            let from = entry.parents.first().map(ToString::to_string);
            let to = entry.id.to_string();
            let changes = client.diff(from.as_deref(), &to, path).await?;
            Ok(DiffOut {
                schema: "scion.cli.diff/v1",
                files: remote_files(&client, changes, name_only).await?,
                from,
                to,
                name_only,
            })
        }
        [from, to] => {
            let (client, _) = query_client(ctx).await?;
            let changes = client.diff(Some(from), to, path).await?;
            Ok(DiffOut {
                schema: "scion.cli.diff/v1",
                files: remote_files(&client, changes, name_only).await?,
                from: Some(from.clone()),
                to: to.clone(),
                name_only,
            })
        }
        _ => Err(CliError::Usage("scn diff takes at most two revisions".into())),
    }
}

fn in_path(file: &str, path: Option<&str>) -> bool {
    match path.filter(|p| !p.is_empty()) {
        None => true,
        Some(p) => file == p || file.strip_prefix(p).is_some_and(|rest| rest.starts_with('/')),
    }
}

async fn local_diff(ctx: &Ctx, path: Option<&str>, name_only: bool) -> Result<DiffOut, CliError> {
    let wc = WorkingCopy::open(&ctx.cwd)?;
    let (client, _) = query_client(ctx).await?;
    let mut changes = wc.local_changes()?;
    changes.retain(|c| in_path(&c.path, path));
    changes.sort_by(|a, b| a.path.as_bytes().cmp(b.path.as_bytes()));
    let mut files = Vec::with_capacity(changes.len());
    for c in changes {
        let (binary, patch) = if name_only {
            (false, None)
        } else {
            let old = fetch(&client, c.old.map(|(_, id)| id)).await?;
            render_patch(&c.path, old.as_deref(), c.new.as_ref().map(|(_, b)| b.as_ref()))
        };
        files.push(DiffFile {
            status: status_of(c.old.is_some(), c.new.is_some()),
            old_id: c.old.map(|(_, id)| id.to_string()),
            new_id: c.new.as_ref().map(|(_, b)| ObjectId::of_blob(b).to_string()),
            old_mode: c.old.map(|(m, _)| mode_name(m)),
            new_mode: c.new.as_ref().map(|(m, _)| mode_name(*m)),
            binary,
            patch,
            path: c.path,
        });
    }
    let from = wc.state().current()?.map(|id| short(&id));
    Ok(DiffOut {
        schema: "scion.cli.diff/v1",
        from,
        to: "working copy".to_owned(),
        files,
        name_only,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn format_time_known_values() {
        assert_eq!(format_time(0, 0), "1970-01-01 00:00 +0000");
        assert_eq!(format_time(1_700_000_000_000, 480), "2023-11-15 06:13 +0800");
        assert_eq!(format_time(1_700_000_000_000, -330), "2023-11-14 16:43 -0530");
        assert_eq!(format_time(951_782_400_000, 0), "2000-02-29 00:00 +0000", "leap day");
        assert_eq!(format_time(-1_000, 0), "1969-12-31 23:59 +0000", "before the epoch");
    }

    #[test]
    fn civil_from_days_boundaries() {
        assert_eq!(civil_from_days(0), (1970, 1, 1));
        assert_eq!(civil_from_days(-1), (1969, 12, 31));
        assert_eq!(civil_from_days(10_957), (2000, 1, 1));
        assert_eq!(civil_from_days(11_016), (2000, 2, 29));
    }

    #[test]
    fn render_patch_text_and_binary() {
        let (bin, p) = render_patch("a.txt", Some(b"one\ntwo\n"), Some(b"one\nthree\n"));
        assert!(!bin);
        let p = p.expect("patch");
        assert!(p.contains("--- a/a.txt") && p.contains("+++ b/a.txt"));
        assert!(p.contains("-two") && p.contains("+three"));
        let (bin, p) = render_patch("new.txt", None, Some(b"hi\n"));
        assert!(!bin && p.expect("patch").contains("+hi"));
        assert_eq!(render_patch("x.bin", Some(&[0, 1, 2]), Some(b"t")), (true, None));
        assert_eq!(render_patch("x.bin", None, Some(&[0xff, 0xfe])), (true, None));
    }

    #[test]
    fn status_and_paths() {
        assert_eq!(status_of(false, true), "A");
        assert_eq!(status_of(true, false), "D");
        assert_eq!(status_of(true, true), "M");
        assert!(in_path("src/a.rs", Some("src")));
        assert!(in_path("src", Some("src")));
        assert!(!in_path("srcx/a", Some("src")));
        assert!(in_path("any", None));
        assert!(in_path("any", Some("")));
        assert_eq!(mode_name(Mode::Executable), "executable");
        assert_eq!(mode_name(Mode::Symlink), "symlink");
        assert_eq!(mode_name(Mode::File), "file");
    }

    fn file(status: &'static str, old: Option<&'static str>, new: Option<&'static str>, binary: bool) -> DiffFile {
        DiffFile {
            path: "p".into(),
            status,
            old_id: None,
            new_id: None,
            old_mode: old,
            new_mode: new,
            binary,
            patch: Some("@@ -1 +1 @@\n".into()),
        }
    }

    #[test]
    fn diff_render_text() {
        let out = DiffOut {
            schema: "s",
            from: None,
            to: "t".into(),
            files: vec![],
            name_only: false,
        };
        assert_eq!(out.text(), "no differences");
        let out = DiffOut {
            files: vec![
                file("A", None, Some("file"), false),
                file("D", Some("file"), None, false),
                file("M", Some("file"), Some("executable"), false),
                file("M", Some("file"), Some("file"), true),
            ],
            ..out
        };
        let t = out.text();
        assert!(t.contains("new file (file)"));
        assert!(t.contains("deleted file (file)"));
        assert!(t.contains("mode change file => executable"));
        assert!(t.contains("Binary files a/p and b/p differ"));
        let names = DiffOut { name_only: true, ..out };
        assert_eq!(names.text(), "A p\nD p\nM p\nM p");
    }

    #[test]
    fn split_lines_matches_server() {
        assert_eq!(split_lines(b"a\nb"), vec![&b"a\n"[..], b"b"]);
        assert_eq!(split_lines(b"a\n"), vec![&b"a\n"[..]]);
        assert!(split_lines(b"").is_empty());
    }

    #[test]
    fn blame_render_text() {
        let l = |n, author: &str| BlameLineOut {
            line: n,
            commit: "c1abcdefabcdef0".into(),
            author: author.into(),
            time_ms: 0,
            tz_offset_minutes: 0,
            content: "code".into(),
        };
        let out = BlameOut {
            schema: "s",
            revision: "main".into(),
            path: "f".into(),
            lines: vec![l(1, "Ada"), l(10, "A very long author name")],
        };
        let t = out.text();
        assert!(
            t.starts_with("c1abcdefabcd (Ada              1970-01-01)  1| code"),
            "{t}"
        );
        assert!(t.contains("(A very long auth 1970-01-01) 10| code"), "{t}");
    }

    #[test]
    fn log_render_text() {
        let line = |parents: usize| LogLine {
            id: "c1aaaaaaaaaaaaaaaa".into(),
            parents: vec!["c1p".into(); parents],
            author: "Ada".into(),
            email: "a@x".into(),
            time_ms: 0,
            tz_offset_minutes: 0,
            message: "subject\n\nbody".into(),
            generation: 1,
        };
        let out = LogOut {
            schema: "s",
            revision: "main".into(),
            path: None,
            entries: vec![line(1), line(2)],
        };
        assert_eq!(
            out.text(),
            "c1aaaaaaaaaa  1970-01-01 00:00 +0000  Ada  subject\nc1aaaaaaaaaa  1970-01-01 00:00 +0000  Ada  subject (merge)"
        );
        let empty = LogOut { entries: vec![], ..out };
        assert_eq!(empty.text(), "no matching changes");
    }
}
