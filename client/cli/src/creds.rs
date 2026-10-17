//! 登录凭据的保存（客户端设计文档 §3）：按服务端地址保存长期凭据（刷新令牌或 PAT）。
//!
//! macOS 上存进系统钥匙串；其它平台（以及钥匙串不可用时）存进权限为 0600 的文件
//! `$XDG_CONFIG_HOME/scion/credentials.json`（默认 `~/.config/scion/credentials.json`）。
//! `SCION_CREDENTIALS_FILE` 指定文件路径时总是使用文件。

use std::collections::BTreeMap;
use std::path::PathBuf;

/// 钥匙串条目的服务名。
#[cfg(target_os = "macos")]
const KEYRING_SERVICE: &str = "scion";

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Backend {
    /// 系统钥匙串（macOS），文件作为后备。
    Keyring(PathBuf),
    File(PathBuf),
}

/// 凭据存储的位置。
pub fn backend(env: impl Fn(&str) -> Option<String>) -> Backend {
    if let Some(f) = env("SCION_CREDENTIALS_FILE").filter(|f| !f.is_empty()) {
        return Backend::File(PathBuf::from(f));
    }
    let config = env("XDG_CONFIG_HOME")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from(env("HOME").unwrap_or_else(|| ".".into())).join(".config"));
    let file = config.join("scion").join("credentials.json");
    if cfg!(target_os = "macos") {
        Backend::Keyring(file)
    } else {
        Backend::File(file)
    }
}

fn key(endpoint: &str) -> String {
    endpoint.trim_end_matches('/').to_owned()
}

fn read_file(path: &PathBuf) -> std::io::Result<BTreeMap<String, String>> {
    match std::fs::read(path) {
        Ok(b) => serde_json::from_slice(&b).map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e)),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(BTreeMap::new()),
        Err(e) => Err(e),
    }
}

fn write_file(path: &PathBuf, map: &BTreeMap<String, String>) -> std::io::Result<()> {
    if let Some(dir) = path.parent() {
        std::fs::create_dir_all(dir)?;
    }
    let tmp = path.with_extension("tmp");
    let body = serde_json::to_vec_pretty(map).map_err(std::io::Error::other)?;
    let mut opts = std::fs::OpenOptions::new();
    opts.write(true).create(true).truncate(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        opts.mode(0o600);
    }
    {
        use std::io::Write;
        let mut f = opts.open(&tmp)?;
        f.write_all(&body)?;
    }
    std::fs::rename(&tmp, path)
}

#[cfg(target_os = "macos")]
fn keyring_entry(endpoint: &str) -> Option<keyring::Entry> {
    keyring::Entry::new(KEYRING_SERVICE, &key(endpoint)).ok()
}

/// 读取某个服务端的凭据。
pub fn load(b: &Backend, endpoint: &str) -> Option<String> {
    match b {
        #[cfg(target_os = "macos")]
        Backend::Keyring(file) => keyring_entry(endpoint)
            .and_then(|e| e.get_password().ok())
            .or_else(|| read_file(file).ok()?.remove(&key(endpoint))),
        #[cfg(not(target_os = "macos"))]
        Backend::Keyring(file) => read_file(file).ok()?.remove(&key(endpoint)),
        Backend::File(file) => read_file(file).ok()?.remove(&key(endpoint)),
    }
}

/// 保存凭据，返回保存的位置（用于提示）。
pub fn store(b: &Backend, endpoint: &str, secret: &str) -> std::io::Result<String> {
    let file = match b {
        Backend::Keyring(file) => {
            #[cfg(target_os = "macos")]
            if keyring_entry(endpoint).is_some_and(|e| e.set_password(secret).is_ok()) {
                return Ok("the system keychain".into());
            }
            file
        }
        Backend::File(file) => file,
    };
    let mut map = read_file(file)?;
    map.insert(key(endpoint), secret.to_owned());
    write_file(file, &map)?;
    Ok(file.display().to_string())
}

/// 删除凭据，返回是否存在。
pub fn remove(b: &Backend, endpoint: &str) -> std::io::Result<bool> {
    let (file, mut found) = match b {
        Backend::Keyring(file) => {
            #[cfg(target_os = "macos")]
            let found = keyring_entry(endpoint).is_some_and(|e| e.delete_credential().is_ok());
            #[cfg(not(target_os = "macos"))]
            let found = false;
            (file, found)
        }
        Backend::File(file) => (file, false),
    };
    let mut map = read_file(file)?;
    if map.remove(&key(endpoint)).is_some() {
        write_file(file, &map)?;
        found = true;
    }
    Ok(found)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backend_selection() {
        let env = |vars: &'static [(&'static str, &'static str)]| {
            move |k: &str| vars.iter().find(|(n, _)| *n == k).map(|(_, v)| (*v).to_string())
        };
        assert_eq!(
            backend(env(&[("SCION_CREDENTIALS_FILE", "/tmp/c.json")])),
            Backend::File("/tmp/c.json".into())
        );
        let default = backend(env(&[("HOME", "/home/a")]));
        let file = match &default {
            Backend::Keyring(f) | Backend::File(f) => f.clone(),
        };
        assert_eq!(file, PathBuf::from("/home/a/.config/scion/credentials.json"));
        assert_eq!(cfg!(target_os = "macos"), matches!(default, Backend::Keyring(_)));
        let xdg = backend(env(&[("XDG_CONFIG_HOME", "/x"), ("SCION_CREDENTIALS_FILE", "")]));
        assert!(
            matches!(xdg, Backend::Keyring(f) | Backend::File(f) if f == std::path::Path::new("/x/scion/credentials.json"))
        );
    }

    #[test]
    fn file_store_round_trip_with_private_permissions() {
        let dir = tempfile::tempdir().expect("tempdir");
        let b = Backend::File(dir.path().join("sub/credentials.json"));
        assert_eq!(load(&b, "http://a"), None);
        assert!(!remove(&b, "http://a").expect("remove"));
        store(&b, "http://a/", "secret-a").expect("store");
        store(&b, "http://b", "secret-b").expect("store");
        assert_eq!(
            load(&b, "http://a").as_deref(),
            Some("secret-a"),
            "trailing slash ignored"
        );
        assert!(remove(&b, "http://a").expect("remove"));
        assert_eq!(load(&b, "http://a"), None);
        assert_eq!(load(&b, "http://b").as_deref(), Some("secret-b"));
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let Backend::File(f) = &b else { unreachable!() };
            let mode = std::fs::metadata(f).expect("meta").permissions().mode() & 0o777;
            assert_eq!(mode, 0o600);
        }
        // 文件损坏：读不到，写入报错
        let Backend::File(f) = &b else { unreachable!() };
        std::fs::write(f, b"not json").expect("corrupt");
        assert_eq!(load(&b, "http://b"), None);
        assert!(store(&b, "http://c", "x").is_err());
    }
}
