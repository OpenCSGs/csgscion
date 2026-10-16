//! GitSha 派生数据：git 提交 sha → Scion 提交 id（服务端设计文档 §8「GitSha」）。
//!
//! 从 git 导入的提交在扩展字段 `git.sha1` 中记录原始 git sha（20 字节）。创建提交时把反向映射
//! 写成派生数据，于是所有接受版本号的接口都能直接使用 40 位 git sha。正向（提交 → git sha）
//! 直接读提交对象的扩展字段即可。

use scion_object::{Changeset, ObjectId};
use scion_store::MetaStore;

use crate::Result;

/// 记录 git 提交 sha 的扩展字段（SPEC.md §5）。
pub const GIT_SHA1_EXTENSION: &str = "git.sha1";
pub const GITSHA_KIND: &str = "git-sha1";
pub const GITSHA_VERSION: u32 = 1;

/// 40 位小写十六进制的 git sha。
pub fn parse_git_sha(s: &str) -> Option<[u8; 20]> {
    if s.len() != 40 || !s.bytes().all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b)) {
        return None;
    }
    let mut out = [0u8; 20];
    hex::decode_to_slice(s, &mut out).ok()?;
    Some(out)
}

/// 提交中记录的 git sha（扩展字段存在且恰好 20 字节时）。
pub fn git_sha_of(cs: &Changeset) -> Option<[u8; 20]> {
    cs.extensions
        .get(GIT_SHA1_EXTENSION)
        .and_then(|v| v.as_slice().try_into().ok())
}

/// 若提交带有 git sha，登记反向映射（第一次写入为准）。
pub async fn index_git_sha(meta: &dyn MetaStore, repo: &str, id: ObjectId, cs: &Changeset) -> Result<()> {
    if let Some(sha) = git_sha_of(cs) {
        meta.put_derived(repo, GITSHA_KIND, GITSHA_VERSION, &sha, &id.to_bytes())
            .await?;
    }
    Ok(())
}

/// git sha → Scion 提交 id。
pub async fn lookup_git_sha(meta: &dyn MetaStore, repo: &str, sha: &[u8; 20]) -> Result<Option<ObjectId>> {
    Ok(meta
        .get_derived(repo, GITSHA_KIND, GITSHA_VERSION, sha)
        .await?
        .and_then(|v| ObjectId::from_bytes(&v).ok()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use scion_object::Signature;
    use scion_store::MemStore;
    use std::collections::BTreeMap;

    fn cs_with(ext: Option<Vec<u8>>) -> Changeset {
        let sig = Signature {
            name: "a".into(),
            email: "a@x".into(),
            time_ms: 0,
            tz_offset_minutes: 0,
        };
        let mut extensions = BTreeMap::new();
        if let Some(v) = ext {
            extensions.insert(GIT_SHA1_EXTENSION.to_string(), v);
        }
        Changeset::new(vec![], sig.clone(), sig, "m", [0; 16], vec![], extensions).expect("valid")
    }

    #[test]
    fn parse_git_sha_cases() {
        let sha = "0123456789abcdef0123456789abcdef01234567";
        assert_eq!(parse_git_sha(sha).map(|b| b[0]), Some(0x01));
        assert_eq!(parse_git_sha(&sha.to_uppercase()), None, "uppercase rejected");
        assert_eq!(parse_git_sha(&sha[..39]), None);
        assert_eq!(parse_git_sha("main"), None);
        assert_eq!(parse_git_sha("g123456789abcdef0123456789abcdef01234567"), None);
    }

    #[test]
    fn git_sha_of_requires_20_bytes() {
        assert_eq!(git_sha_of(&cs_with(Some(vec![7; 20]))), Some([7; 20]));
        assert_eq!(git_sha_of(&cs_with(Some(vec![7; 19]))), None);
        assert_eq!(git_sha_of(&cs_with(None)), None);
    }

    #[tokio::test]
    async fn index_and_lookup() {
        let s = MemStore::new();
        s.create_repo("r").await.expect("repo");
        let cs = cs_with(Some(vec![9; 20]));
        let id = cs.id();
        index_git_sha(&s, "r", id, &cs).await.expect("index");
        assert_eq!(lookup_git_sha(&s, "r", &[9; 20]).await, Ok(Some(id)));
        assert_eq!(lookup_git_sha(&s, "r", &[8; 20]).await, Ok(None));
        // 没有 git sha 的提交：什么都不写
        let plain = cs_with(None);
        index_git_sha(&s, "r", plain.id(), &plain).await.expect("noop");
        // 损坏的映射值被忽略
        s.put_derived("r", GITSHA_KIND, GITSHA_VERSION, &[1; 20], b"bad")
            .await
            .expect("put");
        assert_eq!(lookup_git_sha(&s, "r", &[1; 20]).await, Ok(None));
    }
}
