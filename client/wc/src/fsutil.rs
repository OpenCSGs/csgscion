//! 工作区文件系统操作。

use std::collections::BTreeMap;
use std::fs::Metadata;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use bytes::Bytes;

use crate::error::{Error, Result, io};
use crate::state::{DIR, Mode};

pub(crate) fn now_ns() -> u64 {
    to_ns(SystemTime::now())
}

fn to_ns(t: SystemTime) -> u64 {
    t.duration_since(UNIX_EPOCH)
        .map(|d| u64::try_from(d.as_nanos()).unwrap_or(u64::MAX))
        .unwrap_or(0)
}

pub(crate) fn mtime_ns(meta: &Metadata) -> u64 {
    meta.modified().map(to_ns).unwrap_or(0)
}

/// 工作区中一个文件的类型与元数据（符号链接取链接本身的元数据）。
#[derive(Debug)]
pub(crate) struct Found {
    pub mode: Mode,
    pub size: u64,
    pub mtime_ns: u64,
}

fn mode_of(meta: &Metadata) -> Option<Mode> {
    let ft = meta.file_type();
    if ft.is_symlink() {
        return Some(Mode::Symlink);
    }
    if !ft.is_file() {
        return None;
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        if meta.permissions().mode() & 0o111 != 0 {
            return Some(Mode::Executable);
        }
    }
    Some(Mode::File)
}

/// 扫描工作区，返回 `相对路径（/ 分隔）→ 文件信息`。跳过 `.scion/`、目录本身和特殊文件。
pub(crate) fn scan(root: &Path) -> Result<BTreeMap<String, Found>> {
    let mut out = BTreeMap::new();
    let mut stack: Vec<(PathBuf, String)> = vec![(root.to_path_buf(), String::new())];
    while let Some((dir, prefix)) = stack.pop() {
        for entry in std::fs::read_dir(&dir).map_err(io(&dir))? {
            let entry = entry.map_err(io(&dir))?;
            let name = entry.file_name();
            let name = name
                .to_str()
                .ok_or_else(|| Error::Corrupt(format!("non UTF-8 file name in {}", dir.display())))?
                .to_owned();
            if prefix.is_empty() && name == DIR {
                continue;
            }
            let rel = if prefix.is_empty() {
                name.clone()
            } else {
                format!("{prefix}/{name}")
            };
            let path = entry.path();
            let meta = std::fs::symlink_metadata(&path).map_err(io(&path))?;
            if meta.is_dir() {
                stack.push((path, rel));
            } else if let Some(mode) = mode_of(&meta) {
                out.insert(
                    rel,
                    Found {
                        mode,
                        size: meta.len(),
                        mtime_ns: mtime_ns(&meta),
                    },
                );
            }
        }
    }
    Ok(out)
}

fn path_of(root: &Path, rel: &str) -> PathBuf {
    rel.split('/').fold(root.to_path_buf(), |p, part| p.join(part))
}

/// 读取文件内容：符号链接返回链接目标的字节。
pub(crate) fn read_content(root: &Path, rel: &str, mode: Mode) -> Result<Bytes> {
    let path = path_of(root, rel);
    match mode {
        Mode::Symlink => {
            let target = std::fs::read_link(&path).map_err(io(&path))?;
            let s = target
                .to_str()
                .ok_or_else(|| Error::Corrupt(format!("non UTF-8 symlink {}", path.display())))?;
            Ok(Bytes::copy_from_slice(s.as_bytes()))
        }
        Mode::File | Mode::Executable => Ok(Bytes::from(std::fs::read(&path).map_err(io(&path))?)),
    }
}

/// 写入文件并设置类型；返回写入后的修改时间。已有的同名文件、链接或目录会被替换。
pub(crate) fn write_file(root: &Path, rel: &str, mode: Mode, data: &[u8]) -> Result<u64> {
    let path = path_of(root, rel);
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).map_err(io(parent))?;
    }
    if let Ok(meta) = std::fs::symlink_metadata(&path) {
        if meta.is_dir() {
            std::fs::remove_dir_all(&path).map_err(io(&path))?;
        } else {
            std::fs::remove_file(&path).map_err(io(&path))?;
        }
    }
    match mode {
        Mode::Symlink => {
            let target =
                std::str::from_utf8(data).map_err(|_| Error::Corrupt(format!("symlink {rel} target is not UTF-8")))?;
            #[cfg(unix)]
            std::os::unix::fs::symlink(target, &path).map_err(io(&path))?;
            #[cfg(not(unix))]
            std::fs::write(&path, target).map_err(io(&path))?;
        }
        Mode::File | Mode::Executable => {
            std::fs::write(&path, data).map_err(io(&path))?;
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                let bits = if mode == Mode::Executable { 0o755 } else { 0o644 };
                std::fs::set_permissions(&path, std::fs::Permissions::from_mode(bits)).map_err(io(&path))?;
            }
        }
    }
    let meta = std::fs::symlink_metadata(&path).map_err(io(&path))?;
    Ok(mtime_ns(&meta))
}

/// 在空目录里新建文件（检出用的快速路径）：不检查已有文件，上级目录由调用方事先建好，权限在创建
/// 时设置（受 umask 影响，与 git 相同），修改时间从打开的文件读取。已存在时报错。
pub(crate) fn create_file(root: &Path, rel: &str, mode: Mode, data: &[u8]) -> Result<u64> {
    use std::io::Write;
    let path = path_of(root, rel);
    if mode == Mode::Symlink {
        return write_file(root, rel, mode, data);
    }
    let mut opts = std::fs::OpenOptions::new();
    opts.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        opts.mode(if mode == Mode::Executable { 0o755 } else { 0o644 });
    }
    let mut f = opts.open(&path).map_err(io(&path))?;
    f.write_all(data).map_err(io(&path))?;
    let meta = f.metadata().map_err(io(&path))?;
    Ok(mtime_ns(&meta))
}

/// 建好一组文件的全部上级目录。
pub(crate) fn create_parents<'a>(root: &Path, rels: impl IntoIterator<Item = &'a str>) -> Result<()> {
    let mut dirs: Vec<&str> = rels
        .into_iter()
        .filter_map(|r| r.rsplit_once('/').map(|(d, _)| d))
        .collect();
    dirs.sort_unstable();
    dirs.dedup();
    for d in dirs {
        let path = path_of(root, d);
        std::fs::create_dir_all(&path).map_err(io(&path))?;
    }
    Ok(())
}

/// 删除文件，并逐级删除因此变空的上级目录（不删 root）。
pub(crate) fn remove_file(root: &Path, rel: &str) -> Result<()> {
    let path = path_of(root, rel);
    match std::fs::remove_file(&path) {
        Ok(()) => {}
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        Err(e) => return Err(io(&path)(e)),
    }
    let mut dir = path.parent();
    while let Some(d) = dir {
        if d == root || std::fs::remove_dir(d).is_err() {
            break; // 非空或已到根目录
        }
        dir = d.parent();
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn now_and_to_ns() {
        assert!(now_ns() > 1_577_836_800_000_000_000);
        assert_eq!(to_ns(UNIX_EPOCH), 0);
    }

    #[test]
    fn write_scan_read_roundtrip() {
        let d = tempfile::tempdir().expect("tempdir");
        let r = d.path();
        write_file(r, "a/b.txt", Mode::File, b"hello").expect("file");
        write_file(r, "run.sh", Mode::Executable, b"#!/bin/sh").expect("exec");
        write_file(r, "link", Mode::Symlink, b"a/b.txt").expect("link");
        std::fs::create_dir(r.join(DIR)).expect("meta dir");
        std::fs::write(r.join(DIR).join("state.json"), b"{}").expect("ignored");
        let found = scan(r).expect("scan");
        assert_eq!(found.keys().collect::<Vec<_>>(), ["a/b.txt", "link", "run.sh"]);
        assert_eq!(found["a/b.txt"].mode, Mode::File);
        assert_eq!(found["a/b.txt"].size, 5);
        assert_eq!(found["link"].mode, Mode::Symlink);
        assert_eq!(found["run.sh"].mode, Mode::Executable);
        assert_eq!(read_content(r, "a/b.txt", Mode::File).expect("read").as_ref(), b"hello");
        assert_eq!(
            read_content(r, "link", Mode::Symlink).expect("read").as_ref(),
            b"a/b.txt"
        );
    }

    #[test]
    fn write_file_replaces_existing_kinds() {
        let d = tempfile::tempdir().expect("tempdir");
        let r = d.path();
        write_file(r, "x", Mode::Symlink, b"target").expect("link");
        write_file(r, "x", Mode::File, b"now a file").expect("replace link");
        assert_eq!(scan(r).expect("scan")["x"].mode, Mode::File);
        write_file(r, "dir/inner", Mode::File, b"i").expect("dir");
        write_file(r, "dir", Mode::File, b"file replaces dir").expect("replace dir");
        assert_eq!(scan(r).expect("scan").keys().collect::<Vec<_>>(), ["dir", "x"]);
        assert!(matches!(
            write_file(r, "bad", Mode::Symlink, &[0xff]),
            Err(Error::Corrupt(_))
        ));
    }

    #[test]
    fn create_file_fast_path() {
        let d = tempfile::tempdir().expect("tempdir");
        let r = d.path();
        create_parents(r, ["a/b/c.txt", "a/d.txt", "top.txt", "a/b/e.sh"]).expect("dirs");
        assert!(r.join("a/b").is_dir());
        create_file(r, "a/b/c.txt", Mode::File, b"c").expect("file");
        create_file(r, "a/b/e.sh", Mode::Executable, b"#!/bin/sh").expect("exec");
        create_file(r, "top.txt", Mode::Symlink, b"a/d.txt").expect("link");
        let found = scan(r).expect("scan");
        assert_eq!(found["a/b/c.txt"].mode, Mode::File);
        assert_eq!(found["a/b/e.sh"].mode, Mode::Executable);
        assert_eq!(found["top.txt"].mode, Mode::Symlink);
        assert!(
            create_file(r, "a/b/c.txt", Mode::File, b"again").is_err(),
            "never overwrites"
        );
        assert!(
            create_file(r, "missing/x", Mode::File, b"x").is_err(),
            "parents must exist"
        );
    }

    #[test]
    fn remove_file_prunes_empty_dirs() {
        let d = tempfile::tempdir().expect("tempdir");
        let r = d.path();
        write_file(r, "a/b/c", Mode::File, b"c").expect("c");
        write_file(r, "a/keep", Mode::File, b"k").expect("keep");
        remove_file(r, "a/b/c").expect("rm");
        assert!(!r.join("a/b").exists());
        assert!(r.join("a/keep").exists());
        remove_file(r, "a/keep").expect("rm");
        assert!(!r.join("a").exists());
        assert!(r.exists());
        remove_file(r, "never/existed").expect("missing is fine");
    }

    #[test]
    fn read_and_scan_errors() {
        let d = tempfile::tempdir().expect("tempdir");
        assert!(matches!(
            read_content(d.path(), "nope", Mode::File),
            Err(Error::Io { .. })
        ));
        assert!(matches!(
            read_content(d.path(), "nope", Mode::Symlink),
            Err(Error::Io { .. })
        ));
        assert!(matches!(scan(&d.path().join("missing")), Err(Error::Io { .. })));
    }

    #[cfg(unix)]
    #[test]
    fn scan_skips_special_files() {
        let d = tempfile::tempdir().expect("tempdir");
        let sock = d.path().join("s.sock");
        let _l = std::os::unix::net::UnixListener::bind(&sock).expect("socket");
        assert!(scan(d.path()).expect("scan").is_empty());
    }

    #[cfg(unix)]
    #[test]
    fn non_utf8_names_rejected() {
        use std::os::unix::ffi::OsStrExt;
        let d = tempfile::tempdir().expect("tempdir");
        let name = std::ffi::OsStr::from_bytes(&[0x66, 0xff]);
        // 部分文件系统（如 APFS）不允许非 UTF-8 文件名，此时跳过
        if std::fs::write(d.path().join(name), b"x").is_ok() {
            assert!(matches!(scan(d.path()), Err(Error::Corrupt(_))));
        }
    }
}
