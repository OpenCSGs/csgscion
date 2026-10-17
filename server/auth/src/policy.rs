//! 访问控制：主体、动作与 ACL 策略（服务端设计文档 §9）。
//!
//! 策略是一组「允许」规则（TOML），没有任何规则允许的请求被拒绝：
//!
//! ```toml
//! [[rule]]
//! subjects = ["group:eng", "user:alice"]   # 也可以是 "*"（任何已认证的身份）或 "anonymous"
//! repos    = ["mono", "models/*"]          # 仓库名，`*` 匹配任意字符
//! actions  = ["write"]                     # read < write < land < admin，高的包含低的
//! refs     = ["main", "release/*"]         # 可选，只约束 land
//! paths    = ["src", "docs"]               # 可选，目录前缀；不写表示整个仓库
//! ```
//!
//! - 路径规则只对按路径的请求生效（读文件、列目录、创建提交时的每个改动路径）。不按路径的请求
//!   （按 id 读对象、合入）只有不带 `paths` 的规则才允许，读对象因此需要整个仓库的读权限。
//! - 系统管理（策略、令牌）需要 `repos` 含 `"*"` 且 `actions` 含 `"admin"` 的规则。

use serde::Deserialize;

/// 权限动作，高的包含低的。
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Action {
    Read = 1,
    Write = 2,
    Land = 3,
    Admin = 4,
}

impl Action {
    pub fn as_str(self) -> &'static str {
        match self {
            Action::Read => "read",
            Action::Write => "write",
            Action::Land => "land",
            Action::Admin => "admin",
        }
    }

    pub fn parse(s: &str) -> Option<Self> {
        match s.trim().to_ascii_lowercase().as_str() {
            "read" => Some(Action::Read),
            "write" => Some(Action::Write),
            "land" => Some(Action::Land),
            "admin" => Some(Action::Admin),
            _ => None,
        }
    }
}

/// 令牌本身携带的授权（例如 starhub 签发的 JWT 中的 `repos` claim）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Grant {
    /// 仓库名，可含 `*`。
    pub repo: String,
    pub action: Action,
}

/// 请求方的身份。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Principal {
    /// 未认证时为 `anonymous`。
    pub subject: String,
    pub groups: Vec<String>,
    pub grants: Vec<Grant>,
    pub expires_at_ms: Option<i64>,
    pub authenticated: bool,
}

impl Principal {
    pub fn anonymous() -> Self {
        Self {
            subject: "anonymous".into(),
            groups: Vec::new(),
            grants: Vec::new(),
            expires_at_ms: None,
            authenticated: false,
        }
    }

    pub fn user(subject: impl Into<String>, groups: Vec<String>) -> Self {
        Self {
            subject: subject.into(),
            groups,
            grants: Vec::new(),
            expires_at_ms: None,
            authenticated: true,
        }
    }

    /// 是否匹配规则中的一个主体写法。
    fn matches(&self, pattern: &str) -> bool {
        match pattern {
            "*" => self.authenticated,
            "anonymous" => !self.authenticated,
            p => {
                if let Some(u) = p.strip_prefix("user:") {
                    self.authenticated && self.subject == u
                } else if let Some(g) = p.strip_prefix("group:") {
                    self.authenticated && self.groups.iter().any(|x| x == g)
                } else {
                    false
                }
            }
        }
    }
}

/// `*` 匹配任意字符序列（包括 `/`）的通配。
pub fn glob(pattern: &str, s: &str) -> bool {
    let parts: Vec<&str> = pattern.split('*').collect();
    if parts.len() == 1 {
        return pattern == s;
    }
    let (first, last) = (parts[0], parts[parts.len() - 1]);
    if !s.starts_with(first) || s.len() < first.len() + last.len() || !s.ends_with(last) {
        return false;
    }
    let mut rest = &s[first.len()..s.len() - last.len()];
    for mid in &parts[1..parts.len() - 1] {
        match rest.find(mid) {
            Some(i) => rest = &rest[i + mid.len()..],
            None => return false,
        }
    }
    true
}

/// 把规则中的目录前缀规范化：去掉首尾的 `/`。
fn norm_prefix(p: &str) -> &str {
    p.trim_matches('/')
}

/// `path` 是否在目录前缀 `prefix` 之内（等于它或在它下面）。空前缀包含一切。
pub fn path_within(path: &str, prefix: &str) -> bool {
    let (path, prefix) = (path.trim_matches('/'), norm_prefix(prefix));
    prefix.is_empty() || path == prefix || path.strip_prefix(prefix).is_some_and(|r| r.starts_with('/'))
}

/// 一次权限检查。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Request<'a> {
    pub repo: &'a str,
    pub action: Action,
    /// 合入的目标 ref。
    pub reference: Option<&'a str>,
    /// 按路径的请求的路径；`None` 表示不按路径（需要整个仓库的权限）。
    pub path: Option<&'a str>,
}

impl<'a> Request<'a> {
    pub fn repo(repo: &'a str, action: Action) -> Self {
        Self {
            repo,
            action,
            reference: None,
            path: None,
        }
    }

    pub fn with_ref(mut self, reference: &'a str) -> Self {
        self.reference = Some(reference);
        self
    }

    pub fn with_path(mut self, path: &'a str) -> Self {
        self.path = Some(path);
        self
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawRule {
    subjects: Vec<String>,
    repos: Vec<String>,
    actions: Vec<String>,
    #[serde(default)]
    refs: Vec<String>,
    #[serde(default)]
    paths: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawPolicy {
    #[serde(default)]
    rule: Vec<RawRule>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct Rule {
    subjects: Vec<String>,
    repos: Vec<String>,
    /// 规则允许的最高动作。
    action: Action,
    refs: Vec<String>,
    paths: Vec<String>,
}

impl Rule {
    fn applies(&self, p: &Principal, req: &Request<'_>) -> bool {
        self.subjects.iter().any(|s| p.matches(s))
            && self.repos.iter().any(|r| glob(r, req.repo))
            && self.action >= req.action
            && (self.refs.is_empty() || req.reference.is_some_and(|r| self.refs.iter().any(|g| glob(g, r))))
            && (self.paths.is_empty()
                || req
                    .path
                    .is_some_and(|path| self.paths.iter().any(|x| path_within(path, x))))
    }
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum PolicyError {
    #[error("invalid policy: {0}")]
    Syntax(String),
    #[error("rule {index}: {message}")]
    Rule { index: usize, message: String },
}

/// 解析好的策略。
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Policy {
    rules: Vec<Rule>,
}

impl Policy {
    pub fn parse(text: &str) -> Result<Self, PolicyError> {
        let raw: RawPolicy = toml::from_str(text).map_err(|e| PolicyError::Syntax(e.to_string()))?;
        let mut rules = Vec::with_capacity(raw.rule.len());
        for (index, r) in raw.rule.into_iter().enumerate() {
            let bad = |message: String| PolicyError::Rule { index, message };
            if r.subjects.is_empty() || r.repos.is_empty() || r.actions.is_empty() {
                return Err(bad("subjects, repos and actions must not be empty".into()));
            }
            for s in &r.subjects {
                let known = s == "*" || s == "anonymous" || s.starts_with("user:") || s.starts_with("group:");
                if !known {
                    return Err(bad(format!(
                        "unknown subject {s:?}; use user:NAME, group:NAME, * or anonymous"
                    )));
                }
            }
            let mut action = Action::Read;
            for a in &r.actions {
                action = action.max(
                    Action::parse(a)
                        .ok_or_else(|| bad(format!("unknown action {a:?}; use read, write, land or admin")))?,
                );
            }
            rules.push(Rule {
                subjects: r.subjects,
                repos: r.repos,
                action,
                refs: r.refs,
                paths: r.paths,
            });
        }
        Ok(Self { rules })
    }

    pub fn len(&self) -> usize {
        self.rules.len()
    }

    pub fn is_empty(&self) -> bool {
        self.rules.is_empty()
    }

    /// 是否有规则允许这个请求。
    pub fn allows(&self, p: &Principal, req: &Request<'_>) -> bool {
        self.rules.iter().any(|r| r.applies(p, req))
    }

    /// 列目录时 `dir` 下的条目 `path` 是否可以显示：它本身可读，或者是某个可读路径的上级目录。
    pub fn path_visible(&self, p: &Principal, repo: &str, path: &str) -> bool {
        let req = Request::repo(repo, Action::Read).with_path(path);
        if self.allows(p, &req) {
            return true;
        }
        let path = path.trim_matches('/');
        self.rules.iter().any(|r| {
            r.subjects.iter().any(|s| p.matches(s))
                && r.repos.iter().any(|g| glob(g, repo))
                && r.paths
                    .iter()
                    .any(|x| path_within(norm_prefix(x), path) && !path.is_empty())
        }) || path.is_empty()
    }

    /// 在仓库里是否有 `action`（不论 ref 和路径的限制）。用于不涉及具体路径、但限定路径的身份也需要的
    /// 操作：上传对象、读取 ref、浏览历史。
    pub fn allows_any(&self, p: &Principal, repo: &str, action: Action) -> bool {
        self.rules.iter().any(|r| {
            r.action >= action && r.subjects.iter().any(|s| p.matches(s)) && r.repos.iter().any(|g| glob(g, repo))
        })
    }

    /// 在仓库里是否有任何读权限（可能只限某些路径）。
    pub fn any_read(&self, p: &Principal, repo: &str) -> bool {
        self.rules
            .iter()
            .any(|r| r.subjects.iter().any(|s| p.matches(s)) && r.repos.iter().any(|g| glob(g, repo)))
    }

    /// 是否有系统管理权限（策略、令牌）。
    pub fn is_system_admin(&self, p: &Principal) -> bool {
        self.rules.iter().any(|r| {
            r.action == Action::Admin
                && r.repos.iter().any(|x| x == "*")
                && r.paths.is_empty()
                && r.refs.is_empty()
                && r.subjects.iter().any(|s| p.matches(s))
        })
    }
}

/// 令牌携带的授权是否允许请求（只授予整个仓库，不细分 ref 和路径）。
pub fn grants_allow(p: &Principal, req: &Request<'_>) -> bool {
    p.grants
        .iter()
        .any(|g| g.action >= req.action && glob(&g.repo, req.repo))
}

#[cfg(test)]
mod tests {
    use super::*;

    const POLICY: &str = r#"
[[rule]]
subjects = ["*"]
repos = ["public/*"]
actions = ["read"]

[[rule]]
subjects = ["anonymous"]
repos = ["open"]
actions = ["read"]

[[rule]]
subjects = ["group:eng"]
repos = ["mono"]
actions = ["write", "read"]

[[rule]]
subjects = ["user:rel"]
repos = ["mono"]
actions = ["land"]
refs = ["release/*"]

[[rule]]
subjects = ["user:contractor"]
repos = ["mono"]
actions = ["write"]
paths = ["src/ui/", "docs"]

[[rule]]
subjects = ["group:admins"]
repos = ["*"]
actions = ["admin"]
"#;

    fn eng() -> Principal {
        Principal::user("bob", vec!["eng".into()])
    }

    #[test]
    fn actions_parse_and_order() {
        for a in [Action::Read, Action::Write, Action::Land, Action::Admin] {
            assert_eq!(Action::parse(a.as_str()), Some(a));
        }
        assert_eq!(Action::parse(" LAND "), Some(Action::Land));
        assert_eq!(Action::parse("delete"), None);
        assert!(Action::Admin > Action::Land && Action::Land > Action::Write && Action::Write > Action::Read);
    }

    #[test]
    fn glob_matching() {
        assert!(glob("mono", "mono"));
        assert!(!glob("mono", "mono2"));
        assert!(glob("*", "anything/at/all"));
        assert!(glob("models/*", "models/a/b"));
        assert!(!glob("models/*", "datasets/a"));
        assert!(glob("a*c*e", "abcde"));
        assert!(!glob("a*c*e", "abde"));
        assert!(glob("release/*", "release/1.0"));
        assert!(!glob("ab*ba", "aba"), "prefix and suffix must not overlap");
    }

    #[test]
    fn path_prefixes() {
        assert!(path_within("src/ui/a.rs", "src/ui/"));
        assert!(path_within("src/ui", "/src/ui"));
        assert!(!path_within("src/uikit/a.rs", "src/ui"));
        assert!(path_within("anything", ""));
        assert!(!path_within("src", "src/ui"));
    }

    #[test]
    fn rules_grant_by_subject_repo_action_ref_and_path() {
        let p = Policy::parse(POLICY).expect("parse");
        assert_eq!((p.len(), p.is_empty()), (6, false));
        let bob = eng();
        assert!(p.allows(&bob, &Request::repo("mono", Action::Read)));
        assert!(p.allows(&bob, &Request::repo("mono", Action::Write)));
        assert!(
            !p.allows(&bob, &Request::repo("mono", Action::Land)),
            "write does not imply land"
        );
        assert!(!p.allows(&bob, &Request::repo("other", Action::Read)));
        assert!(
            p.allows(&bob, &Request::repo("public/x", Action::Read)),
            "any authenticated user"
        );
        let anon = Principal::anonymous();
        assert!(
            !p.allows(&anon, &Request::repo("public/x", Action::Read)),
            "* excludes anonymous"
        );
        assert!(p.allows(&anon, &Request::repo("open", Action::Read)));
        assert!(
            !p.allows(&bob, &Request::repo("open", Action::Read)),
            "anonymous rule only matches anonymous"
        );

        // ref 约束
        let rel = Principal::user("rel", vec![]);
        assert!(p.allows(&rel, &Request::repo("mono", Action::Land).with_ref("release/2")));
        assert!(!p.allows(&rel, &Request::repo("mono", Action::Land).with_ref("main")));
        assert!(
            !p.allows(&rel, &Request::repo("mono", Action::Land)),
            "ref-scoped rule needs a ref"
        );
        assert!(p.allows(&rel, &Request::repo("mono", Action::Write).with_ref("release/2")));

        // 路径约束：只对按路径的请求生效
        let c = Principal::user("contractor", vec![]);
        assert!(p.allows(&c, &Request::repo("mono", Action::Write).with_path("src/ui/button.rs")));
        assert!(p.allows(&c, &Request::repo("mono", Action::Read).with_path("docs/x.md")));
        assert!(!p.allows(&c, &Request::repo("mono", Action::Write).with_path("src/core.rs")));
        assert!(
            !p.allows(&c, &Request::repo("mono", Action::Read)),
            "whole-repo read needs an unrestricted rule"
        );

        // 系统管理
        let admin = Principal::user("root", vec!["admins".into()]);
        assert!(p.is_system_admin(&admin));
        assert!(p.allows(&admin, &Request::repo("anything", Action::Land).with_ref("main")));
        assert!(!p.is_system_admin(&bob));
    }

    #[test]
    fn directory_listing_shows_paths_leading_to_allowed_subtrees() {
        let p = Policy::parse(POLICY).expect("parse");
        let c = Principal::user("contractor", vec![]);
        assert!(p.path_visible(&c, "mono", ""), "root");
        assert!(p.path_visible(&c, "mono", "src"), "ancestor of src/ui");
        assert!(p.path_visible(&c, "mono", "src/ui"));
        assert!(p.path_visible(&c, "mono", "src/ui/x"));
        assert!(!p.path_visible(&c, "mono", "src/core"));
        assert!(!p.path_visible(&c, "mono", "secret"));
        assert!(
            p.path_visible(&eng(), "mono", "secret"),
            "unrestricted reader sees everything"
        );
        assert!(!p.path_visible(&Principal::user("x", vec![]), "mono", "src"));
        assert!(p.any_read(&c, "mono") && !p.any_read(&c, "other"));
        assert!(p.allows_any(&c, "mono", Action::Write));
        assert!(!p.allows_any(&c, "mono", Action::Land));
        let rel = Principal::user("rel", vec![]);
        assert!(p.allows_any(&rel, "mono", Action::Land), "ref restriction ignored");
    }

    #[test]
    fn token_grants() {
        let mut p = Principal::user("s", vec![]);
        p.grants = vec![Grant {
            repo: "models/*".into(),
            action: Action::Write,
        }];
        assert!(grants_allow(&p, &Request::repo("models/a", Action::Read)));
        assert!(grants_allow(&p, &Request::repo("models/a", Action::Write)));
        assert!(!grants_allow(&p, &Request::repo("models/a", Action::Land)));
        assert!(!grants_allow(&p, &Request::repo("other", Action::Read)));
    }

    #[test]
    fn invalid_policies_are_rejected() {
        assert!(matches!(Policy::parse("not toml ["), Err(PolicyError::Syntax(_))));
        assert!(matches!(
            Policy::parse("[[rule]]\nsubjects=[]\nrepos=[\"a\"]\nactions=[\"read\"]"),
            Err(PolicyError::Rule { index: 0, .. })
        ));
        let bad_subject = "[[rule]]\nsubjects=[\"alice\"]\nrepos=[\"a\"]\nactions=[\"read\"]";
        assert!(
            Policy::parse(bad_subject)
                .expect_err("bad subject")
                .to_string()
                .contains("unknown subject")
        );
        let bad_action = "[[rule]]\nsubjects=[\"*\"]\nrepos=[\"a\"]\nactions=[\"own\"]";
        assert!(
            Policy::parse(bad_action)
                .expect_err("bad action")
                .to_string()
                .contains("unknown action")
        );
        let unknown_field = "[[rule]]\nsubjects=[\"*\"]\nrepos=[\"a\"]\nactions=[\"read\"]\nmode=1";
        assert!(matches!(Policy::parse(unknown_field), Err(PolicyError::Syntax(_))));
        assert_eq!(Policy::parse("").expect("empty").len(), 0);
    }
}
