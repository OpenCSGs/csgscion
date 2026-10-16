//! 工作副本错误。

use std::path::PathBuf;

use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("not a scion working copy (no .scion directory in {0} or its parents)")]
    NotAWorkingCopy(PathBuf),
    #[error("{0} already exists and is not empty")]
    TargetNotEmpty(PathBuf),
    #[error("working copy is locked by another scn process ({0}); remove it if that process is gone")]
    Locked(PathBuf),
    #[error("you have uncommitted changes; commit them first")]
    Dirty,
    #[error("nothing to land")]
    NothingToLand,
    /// 本地未合入的改动与 ref 上的新提交改了同样的文件。
    #[error("conflict: both you and upstream changed {}", .0.join(", "))]
    Conflict(Vec<String>),
    #[error("ref {0:?} does not exist on the server")]
    RefNotFound(String),
    #[error("{path}: {source}")]
    Io {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("corrupt working copy state: {0}")]
    Corrupt(String),
    #[error(transparent)]
    Client(#[from] scion_client::Error),
}

pub type Result<T> = std::result::Result<T, Error>;

/// 给 IO 错误附上路径。
pub(crate) fn io(path: impl Into<PathBuf>) -> impl FnOnce(std::io::Error) -> Error {
    let path = path.into();
    move |source| Error::Io { path, source }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn display_messages() {
        assert!(Error::NotAWorkingCopy("/x".into()).to_string().contains(".scion"));
        assert!(Error::Locked("/x/.scion/lock".into()).to_string().contains("locked"));
        assert!(Error::Dirty.to_string().contains("commit"));
        assert_eq!(Error::NothingToLand.to_string(), "nothing to land");
        assert_eq!(
            Error::Conflict(vec!["a".into(), "b".into()]).to_string(),
            "conflict: both you and upstream changed a, b"
        );
        assert!(Error::RefNotFound("main".into()).to_string().contains("main"));
        assert!(Error::TargetNotEmpty("/d".into()).to_string().contains("not empty"));
        assert!(Error::Corrupt("x".into()).to_string().contains("corrupt"));
        let e = io("/p")(std::io::Error::other("boom"));
        assert_eq!(e.to_string(), "/p: boom");
    }
}
