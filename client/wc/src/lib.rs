//! Scion 工作副本（客户端设计文档 §5）。
//!
//! M1 范围：完整检出、status、commit、land（快进）、sync（本地无改动时）。
//! 稀疏检出、自动 rebase、操作日志在后续里程碑加入。
//!
//! 工作副本只依赖公开协议：所有数据通过 [`scion_client::Client`] 取得。

mod error;
mod fsutil;
mod state;

use std::collections::BTreeMap;
use std::path::Path;

use bytes::Bytes;
use scion_client::{Client, CreatedChangeset, Landed};
use scion_object::{EntryMode, ObjectId, Tree};

pub use error::{Error, Result};
pub use state::{DirState, FileRecord, Mode, State};

use crate::fsutil::Found;
use crate::state::Paths;

/// 本地改动。
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Status {
    pub added: Vec<String>,
    pub modified: Vec<String>,
    pub deleted: Vec<String>,
}

impl Status {
    pub fn is_clean(&self) -> bool {
        self.added.is_empty() && self.modified.is_empty() && self.deleted.is_empty()
    }
}

/// 一个本地改动文件：`old` 为当前快照中的版本（新增时为 None），`new` 为本地内容（删除时为 None）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LocalChange {
    pub path: String,
    pub old: Option<(Mode, ObjectId)>,
    pub new: Option<(Mode, Bytes)>,
}

/// sync 的结果。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SyncOutcome {
    UpToDate,
    Updated {
        from: Option<ObjectId>,
        to: ObjectId,
        written: usize,
        removed: usize,
    },
    /// 未合入的提交被重新基于 `onto` 创建为 `head`，并把上游改动更新到了本地。
    Rebased {
        onto: ObjectId,
        head: ObjectId,
        written: usize,
        removed: usize,
    },
}

/// 一个待提交的改动（内部使用）。
enum Change {
    Put {
        path: String,
        mode: Mode,
        content: Bytes,
        found_mtime: u64,
    },
    Delete {
        path: String,
    },
}

/// 目录树中的一个文件。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct TreeFile {
    mode: Mode,
    id: ObjectId,
}

pub struct WorkingCopy {
    paths: Paths,
    state: State,
}

/// 每次批量读取的对象数（SDK 还会按服务端上限再分批）。
const FETCH_BATCH: usize = 256;

/// 递归展开目录树，得到 `路径 → 文件`。按层批量读取目录树。
async fn flatten(client: &Client, root: ObjectId) -> Result<BTreeMap<String, TreeFile>> {
    let mut out = BTreeMap::new();
    let mut level = vec![(String::new(), root)];
    while !level.is_empty() {
        let mut next = Vec::new();
        for part in level.chunks(FETCH_BATCH) {
            let ids: Vec<ObjectId> = part.iter().map(|(_, id)| *id).collect();
            let trees = client.get_objects(&ids).await?;
            for ((prefix, id), bytes) in part.iter().zip(trees) {
                let tree = Tree::decode(&bytes).map_err(|e| Error::Corrupt(format!("tree {id}: {e}")))?;
                for e in tree.entries() {
                    let path = if prefix.is_empty() {
                        e.name.clone()
                    } else {
                        format!("{prefix}/{}", e.name)
                    };
                    match Mode::from_entry(e.mode) {
                        Some(mode) => {
                            out.insert(path, TreeFile { mode, id: e.id });
                        }
                        None => next.push((path, e.id)),
                    }
                }
            }
        }
        level = next;
    }
    Ok(out)
}

/// 同时在途的批量读取数（检出时下载与写文件重叠）。
const FETCHES_IN_FLIGHT: usize = 8;

/// 检出：批量下载（多批并发）并在后台线程并行写入新文件，返回每个文件的 dirstate 记录。
async fn checkout_files(
    client: &Client,
    root: &Path,
    files: &BTreeMap<String, TreeFile>,
) -> Result<Vec<(String, FileRecord)>> {
    use futures::StreamExt;
    fsutil::create_parents(root, files.keys().map(String::as_str))?;
    let list: Vec<(String, TreeFile)> = files.iter().map(|(p, f)| (p.clone(), *f)).collect();
    let mut fetches = futures::stream::iter(list.chunks(FETCH_BATCH).map(<[_]>::to_vec))
        .map(|part| async move {
            let ids: Vec<ObjectId> = part.iter().map(|(_, f)| f.id).collect();
            client.get_objects(&ids).await.map(|data| (part, data))
        })
        .buffered(FETCHES_IN_FLIGHT);
    let mut writes = tokio::task::JoinSet::new();
    let mut out = Vec::with_capacity(files.len());
    while let Some(batch) = fetches.next().await {
        let (part, data) = batch?;
        let root = root.to_path_buf();
        writes.spawn_blocking(move || -> Result<Vec<(String, FileRecord)>> {
            part.into_iter()
                .zip(data)
                .map(|((path, f), d)| {
                    let mtime_ns = fsutil::create_file(&root, &path, f.mode, &d)?;
                    Ok((path, record(f.mode, f.id, d.len() as u64, mtime_ns)))
                })
                .collect()
        });
        // 写入跟不上下载时等一等，限制内存中的批数
        while writes.len() >= FETCHES_IN_FLIGHT {
            if let Some(done) = writes.join_next().await {
                out.extend(done.map_err(|e| Error::Corrupt(format!("checkout task: {e}")))??);
            }
        }
    }
    while let Some(done) = writes.join_next().await {
        out.extend(done.map_err(|e| Error::Corrupt(format!("checkout task: {e}")))??);
    }
    Ok(out)
}

impl WorkingCopy {
    /// 把 `repo` 的 `reference` 检出到 `dir`（目录必须不存在或为空）。ref 不存在时得到空的工作副本。
    pub async fn clone(client: &Client, endpoint: &str, repo: &str, reference: &str, dir: &Path) -> Result<Self> {
        if dir.exists() && std::fs::read_dir(dir).map_err(error::io(dir))?.next().is_some() {
            return Err(Error::TargetNotEmpty(dir.to_path_buf()));
        }
        let paths = Paths::new(dir);
        std::fs::create_dir_all(paths.meta_dir()).map_err(error::io(paths.meta_dir()))?;
        let _lock = paths.lock()?;
        let mut state = State::new(endpoint, repo, reference);
        let mut dirstate = DirState::default();
        if let Some(tip) = client.resolve_ref(reference).await? {
            let files = flatten(client, tip.root_tree).await?;
            dirstate.files.extend(checkout_files(client, dir, &files).await?);
            state.base = Some(tip.changeset.to_string());
        }
        paths.save_dirstate(&mut dirstate)?;
        paths.save_state(&state)?;
        Ok(Self { paths, state })
    }

    /// 打开包含 `start` 的工作副本（向上查找 `.scion/`）。
    pub fn open(start: &Path) -> Result<Self> {
        let paths = Paths::discover(start)?;
        let state = paths.load_state()?;
        Ok(Self { paths, state })
    }

    pub fn root(&self) -> &Path {
        &self.paths.root
    }

    pub fn state(&self) -> &State {
        &self.state
    }

    /// 计算相对当前快照的改动，同时返回提交所需的内容。
    fn changes(&self) -> Result<(Status, Vec<Change>)> {
        let dirstate = self.paths.load_dirstate()?;
        let found = fsutil::scan(self.root())?;
        let mut status = Status::default();
        let mut changes = Vec::new();
        for (path, f) in &found {
            match dirstate.files.get(path) {
                None => {
                    let content = fsutil::read_content(self.root(), path, f.mode)?;
                    status.added.push(path.clone());
                    changes.push(Change::Put {
                        path: path.clone(),
                        mode: f.mode,
                        content,
                        found_mtime: f.mtime_ns,
                    });
                }
                Some(rec) if unchanged_by_stat(rec, f, dirstate.written_at_ns) => {}
                Some(rec) => {
                    let content = fsutil::read_content(self.root(), path, f.mode)?;
                    if ObjectId::of_blob(&content) != rec.object_id()? || rec.mode != f.mode {
                        status.modified.push(path.clone());
                        changes.push(Change::Put {
                            path: path.clone(),
                            mode: f.mode,
                            content,
                            found_mtime: f.mtime_ns,
                        });
                    }
                }
            }
        }
        for path in dirstate.files.keys().filter(|p| !found.contains_key(*p)) {
            status.deleted.push(path.clone());
            changes.push(Change::Delete { path: path.clone() });
        }
        Ok((status, changes))
    }

    pub fn status(&self) -> Result<Status> {
        Ok(self.changes()?.0)
    }

    /// 本地改动的明细（供 diff 使用）：每个改动文件在当前快照中的版本，以及本地的新内容。
    pub fn local_changes(&self) -> Result<Vec<LocalChange>> {
        let (_, changes) = self.changes()?;
        let dirstate = self.paths.load_dirstate()?;
        let old_of = |path: &str| -> Result<Option<(Mode, ObjectId)>> {
            dirstate
                .files
                .get(path)
                .map(|r| Ok((r.mode, r.object_id()?)))
                .transpose()
        };
        changes
            .into_iter()
            .map(|c| match c {
                Change::Put {
                    path, mode, content, ..
                } => Ok(LocalChange {
                    old: old_of(&path)?,
                    new: Some((mode, content)),
                    path,
                }),
                Change::Delete { path } => Ok(LocalChange {
                    old: old_of(&path)?,
                    new: None,
                    path,
                }),
            })
            .collect()
    }

    /// 把本地改动创建为提交（尚未合入）。没有改动时返回 `None`。
    pub async fn commit(
        &mut self,
        client: &Client,
        message: &str,
        author: Option<(&str, &str)>,
    ) -> Result<Option<CreatedChangeset>> {
        let _lock = self.paths.lock()?;
        let (_, changes) = self.changes()?;
        if changes.is_empty() {
            return Ok(None);
        }
        let mut b = client.changeset(self.state.current()?).message(message);
        if let Some((name, email)) = author {
            b = b.author(name, email);
        }
        for c in &changes {
            b = match c {
                Change::Put {
                    path, mode, content, ..
                } => b.put_with_mode(path, mode.to_entry(), content.clone()),
                Change::Delete { path } => b.delete(path),
            };
        }
        let created = b.create().await?;

        let mut dirstate = self.paths.load_dirstate()?;
        for c in changes {
            match c {
                Change::Put {
                    path,
                    mode,
                    content,
                    found_mtime,
                } => {
                    let id = ObjectId::of_blob(&content);
                    dirstate
                        .files
                        .insert(path, record(mode, id, content.len() as u64, found_mtime));
                }
                Change::Delete { path } => {
                    dirstate.files.remove(&path);
                }
            }
        }
        // 先写 dirstate 再写 state：崩溃时最多「已提交的改动在本地显示为未提交」，重新提交得到同一个 id
        self.paths.save_dirstate(&mut dirstate)?;
        self.state.head = Some(created.id.to_string());
        self.state.head_message = Some(message.to_owned());
        self.state.head_author = author.map(|(n, e)| (n.to_owned(), e.to_owned()));
        self.paths.save_state(&self.state)?;
        Ok(Some(created))
    }

    /// 合入已提交的改动。ref 已前进时由服务端 rebase，随后把上游改动更新到工作区。
    ///
    /// 有未提交的改动时只允许快进：rebase 后要把上游改动写入工作区，不能和本地未提交的改动交织。
    pub async fn land(&mut self, client: &Client) -> Result<Landed> {
        let _lock = self.paths.lock()?;
        let head = match &self.state.head {
            Some(h) => h
                .parse::<ObjectId>()
                .map_err(|e| Error::Corrupt(format!("head: {e}")))?,
            None => return Err(Error::NothingToLand),
        };
        let landed = if self.changes()?.0.is_clean() {
            client.land(&self.state.reference, &head).await?
        } else {
            client.land_fast_forward(&self.state.reference, &head).await?
        };
        if landed.tip != head {
            // ref 上包含了别人的改动（rebase 或提交早已在历史中）：工作区从 head 更新到新的 tip
            let from = flatten(client, client.root_tree(&head.to_string()).await?).await?;
            let to = flatten(client, client.root_tree(&landed.tip.to_string()).await?).await?;
            let mut dirstate = self.paths.load_dirstate()?;
            self.apply(client, &mut dirstate, &diff(&from, &to)).await?;
            self.paths.save_dirstate(&mut dirstate)?;
        }
        self.state.base = Some(landed.tip.to_string());
        self.clear_head();
        self.paths.save_state(&self.state)?;
        Ok(landed)
    }

    fn clear_head(&mut self) {
        self.state.head = None;
        self.state.head_message = None;
        self.state.head_author = None;
    }

    /// 更新到 ref 的最新提交。要求本地没有未提交的改动。
    ///
    /// 有已提交未合入的改动且 ref 已前进时，在客户端做文件级 rebase：双方改了同一路径且结果不同则报
    /// [`Error::Conflict`]，否则把本地改动基于最新提交重新创建，并把上游改动更新到本地。
    pub async fn sync(&mut self, client: &Client) -> Result<SyncOutcome> {
        let _lock = self.paths.lock()?;
        if !self.changes()?.0.is_clean() {
            return Err(Error::Dirty);
        }
        let Some(tip) = client.resolve_ref(&self.state.reference).await? else {
            return match self.state.base {
                None => Ok(SyncOutcome::UpToDate),
                Some(_) => Err(Error::RefNotFound(self.state.reference.clone())),
            };
        };
        if self.state.head.is_some() {
            if self.state.base.as_deref() == Some(tip.changeset.to_string().as_str()) {
                return Ok(SyncOutcome::UpToDate);
            }
            return self.rebase(client, tip.changeset, tip.root_tree).await;
        }
        let from = self.state.current()?;
        if from == Some(tip.changeset) {
            return Ok(SyncOutcome::UpToDate);
        }
        let target = flatten(client, tip.root_tree).await?;
        let mut dirstate = self.paths.load_dirstate()?;
        let delta = diff(&snapshot(&dirstate)?, &target);
        let (written, removed) = self.apply(client, &mut dirstate, &delta).await?;
        self.paths.save_dirstate(&mut dirstate)?;
        self.state.base = Some(tip.changeset.to_string());
        self.paths.save_state(&self.state)?;
        Ok(SyncOutcome::Updated {
            from,
            to: tip.changeset,
            written,
            removed,
        })
    }

    /// 把一组改动写到工作区并同步 dirstate，返回 (写入数, 删除数)。
    async fn apply(
        &self,
        client: &Client,
        dirstate: &mut DirState,
        delta: &BTreeMap<String, Option<TreeFile>>,
    ) -> Result<(usize, usize)> {
        let (mut written, mut removed) = (0, 0);
        let entries: Vec<_> = delta.iter().collect();
        // 按原顺序处理（删除与写入的先后影响文件/目录互换），每段的文件内容一次批量读取
        for part in entries.chunks(FETCH_BATCH) {
            let ids: Vec<ObjectId> = part.iter().filter_map(|(_, c)| c.as_ref().map(|f| f.id)).collect();
            let mut contents = client.get_objects(&ids).await?.into_iter();
            for (path, change) in part {
                self.apply_one(
                    dirstate,
                    path,
                    change.as_ref(),
                    &mut contents,
                    &mut written,
                    &mut removed,
                )?;
            }
        }
        Ok((written, removed))
    }

    fn apply_one(
        &self,
        dirstate: &mut DirState,
        path: &str,
        change: Option<&TreeFile>,
        contents: &mut impl Iterator<Item = bytes::Bytes>,
        written: &mut usize,
        removed: &mut usize,
    ) -> Result<()> {
        {
            match change {
                Some(f) => {
                    let data = contents
                        .next()
                        .ok_or_else(|| Error::Corrupt(format!("no content fetched for {path}")))?;
                    let mtime_ns = fsutil::write_file(self.root(), path, f.mode, &data)?;
                    dirstate
                        .files
                        .insert(path.to_owned(), record(f.mode, f.id, data.len() as u64, mtime_ns));
                    *written += 1;
                }
                None => {
                    fsutil::remove_file(self.root(), path)?;
                    dirstate.files.remove(path);
                    *removed += 1;
                }
            }
        }
        Ok(())
    }

    async fn rebase(&mut self, client: &Client, onto: ObjectId, onto_root: ObjectId) -> Result<SyncOutcome> {
        let base_files = match &self.state.base {
            Some(b) => flatten(client, client.root_tree(b).await?).await?,
            None => BTreeMap::new(),
        };
        let mut dirstate = self.paths.load_dirstate()?;
        let mine = diff(&base_files, &snapshot(&dirstate)?);
        let theirs = diff(&base_files, &flatten(client, onto_root).await?);
        let conflicts: Vec<String> = mine
            .iter()
            .filter(|(p, v)| theirs.get(*p).is_some_and(|t| t != *v))
            .map(|(p, _)| p.clone())
            .collect();
        if !conflicts.is_empty() {
            return Err(Error::Conflict(conflicts));
        }
        // 双方完全相同的改动不必重复提交
        let mine: BTreeMap<_, _> = mine.into_iter().filter(|(p, _)| !theirs.contains_key(p)).collect();

        let new_head = if mine.is_empty() {
            None
        } else {
            let message = self.state.head_message.clone().unwrap_or_default();
            let mut b = client.changeset(Some(onto)).message(message);
            if let Some((name, email)) = &self.state.head_author {
                b = b.author(name.clone(), email.clone());
            }
            for (path, change) in &mine {
                b = match change {
                    Some(f) => b.put_with_mode(
                        path,
                        f.mode.to_entry(),
                        fsutil::read_content(self.root(), path, f.mode)?,
                    ),
                    None => b.delete(path),
                };
            }
            Some(b.create().await?.id)
        };
        let (written, removed) = self.apply(client, &mut dirstate, &theirs).await?;
        self.paths.save_dirstate(&mut dirstate)?;
        self.state.base = Some(onto.to_string());
        match new_head {
            Some(h) => self.state.head = Some(h.to_string()),
            None => self.clear_head(),
        }
        self.paths.save_state(&self.state)?;
        Ok(match new_head {
            Some(head) => SyncOutcome::Rebased {
                onto,
                head,
                written,
                removed,
            },
            None => SyncOutcome::Updated {
                from: None,
                to: onto,
                written,
                removed,
            },
        })
    }
}

/// dirstate 中的快照（路径 → 文件）。
fn snapshot(d: &DirState) -> Result<BTreeMap<String, TreeFile>> {
    d.files
        .iter()
        .map(|(p, r)| {
            Ok((
                p.clone(),
                TreeFile {
                    mode: r.mode,
                    id: r.object_id()?,
                },
            ))
        })
        .collect()
}

/// 从 `from` 到 `to` 的改动：`Some` 为新增或修改后的文件，`None` 为删除。
fn diff(from: &BTreeMap<String, TreeFile>, to: &BTreeMap<String, TreeFile>) -> BTreeMap<String, Option<TreeFile>> {
    let mut out = BTreeMap::new();
    for (p, f) in to {
        if from.get(p) != Some(f) {
            out.insert(p.clone(), Some(*f));
        }
    }
    for p in from.keys().filter(|p| !to.contains_key(*p)) {
        out.insert(p.clone(), None);
    }
    out
}

fn record(mode: Mode, id: ObjectId, size: u64, mtime_ns: u64) -> FileRecord {
    FileRecord {
        mode,
        id: id.to_string(),
        size,
        mtime_ns,
    }
}

/// 只看元数据就能确定文件没改：类型、大小、修改时间都一致，且修改时间早于 dirstate 写入时间。
/// 修改时间不早于写入时间的文件可能在同一时间粒度内被改过（racy），必须重新计算哈希。
fn unchanged_by_stat(rec: &FileRecord, f: &Found, written_at_ns: u64) -> bool {
    rec.mode == f.mode && rec.size == f.size && rec.mtime_ns == f.mtime_ns && f.mtime_ns < written_at_ns
}

/// 方便 CLI：把 [`EntryMode`] 转成文件类型（目录返回 `None`）。
pub fn file_mode(m: EntryMode) -> Option<Mode> {
    Mode::from_entry(m)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rec(size: u64, mtime_ns: u64) -> FileRecord {
        record(Mode::File, ObjectId::of_blob(b"x"), size, mtime_ns)
    }

    #[test]
    fn unchanged_by_stat_rules() {
        let f = |size, mtime_ns| Found {
            mode: Mode::File,
            size,
            mtime_ns,
        };
        assert!(unchanged_by_stat(&rec(1, 100), &f(1, 100), 200));
        assert!(!unchanged_by_stat(&rec(1, 100), &f(2, 100), 200), "size differs");
        assert!(!unchanged_by_stat(&rec(1, 100), &f(1, 101), 200), "mtime differs");
        assert!(
            !unchanged_by_stat(&rec(1, 200), &f(1, 200), 200),
            "racy: mtime == written_at"
        );
        let mut r = rec(1, 100);
        r.mode = Mode::Executable;
        assert!(!unchanged_by_stat(&r, &f(1, 100), 200), "mode differs");
    }

    #[test]
    fn diff_cases() {
        let f = |c: &[u8]| TreeFile {
            mode: Mode::File,
            id: ObjectId::of_blob(c),
        };
        let from = BTreeMap::from([
            ("same".to_string(), f(b"s")),
            ("mod".to_string(), f(b"1")),
            ("del".to_string(), f(b"d")),
        ]);
        let mut to = BTreeMap::from([
            ("same".to_string(), f(b"s")),
            ("mod".to_string(), f(b"2")),
            ("new".to_string(), f(b"n")),
        ]);
        let d = diff(&from, &to);
        assert_eq!(d.keys().collect::<Vec<_>>(), ["del", "mod", "new"]);
        assert_eq!(d["del"], None);
        assert_eq!(d["mod"], Some(f(b"2")));
        to.insert(
            "same".into(),
            TreeFile {
                mode: Mode::Executable,
                id: ObjectId::of_blob(b"s"),
            },
        );
        assert!(diff(&from, &to).contains_key("same"), "mode change counts");
        assert!(diff(&from, &from).is_empty());
    }

    #[test]
    fn snapshot_rejects_corrupt_ids() {
        let mut d = DirState::default();
        d.files.insert(
            "a".into(),
            FileRecord {
                mode: Mode::File,
                id: "bad".into(),
                size: 0,
                mtime_ns: 0,
            },
        );
        assert!(matches!(snapshot(&d), Err(Error::Corrupt(_))));
    }

    #[test]
    fn status_is_clean() {
        assert!(Status::default().is_clean());
        assert!(
            !Status {
                added: vec!["a".into()],
                ..Default::default()
            }
            .is_clean()
        );
        assert!(
            !Status {
                deleted: vec!["a".into()],
                ..Default::default()
            }
            .is_clean()
        );
        assert!(
            !Status {
                modified: vec!["a".into()],
                ..Default::default()
            }
            .is_clean()
        );
    }

    #[test]
    fn file_mode_maps_entries() {
        assert_eq!(file_mode(EntryMode::Executable), Some(Mode::Executable));
        assert_eq!(file_mode(EntryMode::Tree), None);
    }
}
