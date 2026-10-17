//! `cargo xtask <task>`：仓库维护任务。
//!
//! - `layers`：检查 crate 依赖方向（服务端设计文档 §5.1、客户端设计文档 §0）。

use std::collections::BTreeMap;
use std::path::Path;
use std::process::ExitCode;

use cargo_metadata::{DependencyKind, MetadataCommand};

/// crate 所属区域，由其所在的顶层目录决定。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Area {
    /// 服务端与客户端共用（只能依赖 common）。
    Common,
    /// 服务端（可依赖 common 与更低层的 server crate）。
    Server,
    /// 客户端（可依赖 common 与 client；**禁止**依赖 server）。
    Client,
    /// 开发工具，不参与检查。
    Tool,
}

fn area_of(rel_dir: &Path) -> Option<Area> {
    match rel_dir.components().next()?.as_os_str().to_str()? {
        "common" => Some(Area::Common),
        "server" => Some(Area::Server),
        "client" => Some(Area::Client),
        "xtask" => Some(Area::Tool),
        _ => None,
    }
}

/// 服务端内部分层：只能依赖层级更低的 crate。新增服务端 crate 时在这里登记。
fn server_rank(name: &str) -> Option<u32> {
    Some(match name {
        "scion-store" => 1,
        "scion-store-sql" | "scion-store-blob" | "scion-cache" => 2,
        "scion-tree" => 3,
        "scion-merge" | "scion-derive" | "scion-query" => 4,
        "scion-land" => 5,
        "scion-api" => 6,
        "scion" => 7,
        _ => return None,
    })
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct Krate {
    name: String,
    area: Area,
    /// 普通依赖中的 workspace 内 crate（dev / build 依赖不检查）。
    deps: Vec<String>,
}

/// 返回全部违规描述；为空表示通过。
fn check(crates: &[Krate]) -> Vec<String> {
    let by_name: BTreeMap<&str, &Krate> = crates.iter().map(|k| (k.name.as_str(), k)).collect();
    let mut errors = Vec::new();
    for k in crates {
        if k.area == Area::Server && server_rank(&k.name).is_none() {
            errors.push(format!(
                "{}: server crate has no layer; register it in xtask server_rank()",
                k.name
            ));
        }
        for d in &k.deps {
            let Some(dep) = by_name.get(d.as_str()) else { continue };
            let ok = match (k.area, dep.area) {
                (Area::Tool, _) => true,
                (_, Area::Tool) => false,
                (Area::Common, Area::Common) => true,
                (Area::Common, _) => false,
                (Area::Client, Area::Common | Area::Client) => true,
                (Area::Client, Area::Server) => false,
                (Area::Server, Area::Common) => true,
                (Area::Server, Area::Client) => false,
                (Area::Server, Area::Server) => match (server_rank(&k.name), server_rank(d)) {
                    (Some(a), Some(b)) => b < a,
                    _ => true, // 未登记的已在上面报错
                },
            };
            if !ok {
                errors.push(format!(
                    "{} ({:?}) must not depend on {} ({:?})",
                    k.name, k.area, d, dep.area
                ));
            }
        }
    }
    errors
}

fn load_workspace() -> anyhow::Result<Vec<Krate>> {
    let meta = MetadataCommand::new().no_deps().exec()?;
    let root = meta.workspace_root.as_std_path().to_path_buf();
    let names: Vec<String> = meta.workspace_packages().iter().map(|p| p.name.to_string()).collect();
    let mut out = Vec::new();
    for p in meta.workspace_packages() {
        let dir = p
            .manifest_path
            .parent()
            .map(|d| d.as_std_path().to_path_buf())
            .unwrap_or_default();
        let rel = dir.strip_prefix(&root).unwrap_or(&dir);
        let area = area_of(rel).ok_or_else(|| {
            anyhow::anyhow!(
                "{} lives in {}, outside common/ server/ client/ xtask/",
                p.name,
                rel.display()
            )
        })?;
        let deps = p
            .dependencies
            .iter()
            .filter(|d| d.kind == DependencyKind::Normal && names.contains(&d.name))
            .map(|d| d.name.clone())
            .collect();
        out.push(Krate {
            name: p.name.to_string(),
            area,
            deps,
        });
    }
    Ok(out)
}

fn main() -> ExitCode {
    let task = std::env::args().nth(1).unwrap_or_default();
    match task.as_str() {
        "layers" => match load_workspace() {
            Ok(crates) => {
                let errors = check(&crates);
                if errors.is_empty() {
                    println!("layers: ok ({} crates)", crates.len());
                    ExitCode::SUCCESS
                } else {
                    for e in errors {
                        eprintln!("layers: {e}");
                    }
                    ExitCode::FAILURE
                }
            }
            Err(e) => {
                eprintln!("layers: {e:#}");
                ExitCode::FAILURE
            }
        },
        _ => {
            eprintln!("usage: cargo xtask layers");
            ExitCode::from(2)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn k(name: &str, area: Area, deps: &[&str]) -> Krate {
        Krate {
            name: name.into(),
            area,
            deps: deps.iter().map(|s| s.to_string()).collect(),
        }
    }

    fn base() -> Vec<Krate> {
        vec![
            k("scion-proto", Area::Common, &[]),
            k("scion-object", Area::Common, &[]),
            k("scion-store", Area::Server, &["scion-object"]),
            k(
                "scion-api",
                Area::Server,
                &["scion-proto", "scion-object", "scion-store"],
            ),
            k("scion", Area::Server, &["scion-api", "scion-store"]),
            k("scion-client", Area::Client, &["scion-proto", "scion-object"]),
            k("scion-cli", Area::Client, &["scion-client", "scion-proto"]),
            k("xtask", Area::Tool, &[]),
        ]
    }

    #[test]
    fn area_of_top_level_dirs() {
        assert_eq!(area_of(Path::new("common/object")), Some(Area::Common));
        assert_eq!(area_of(Path::new("server/api")), Some(Area::Server));
        assert_eq!(area_of(Path::new("client/sdk")), Some(Area::Client));
        assert_eq!(area_of(Path::new("xtask")), Some(Area::Tool));
        assert_eq!(area_of(Path::new("crates/x")), None);
        assert_eq!(area_of(Path::new("")), None);
    }

    #[test]
    fn server_rank_orders_layers() {
        assert!(server_rank("scion-store") < server_rank("scion-api"));
        assert!(server_rank("scion-api") < server_rank("scion"));
        assert_eq!(server_rank("unknown"), None);
    }

    #[test]
    fn check_current_layout_passes() {
        assert_eq!(check(&base()), Vec::<String>::new());
    }

    #[test]
    fn check_client_depending_on_server_fails() {
        let mut c = base();
        c.push(k("scion-wc", Area::Client, &["scion-store"]));
        let errs = check(&c);
        assert_eq!(errs.len(), 1);
        assert!(errs[0].contains("scion-wc"));
    }

    #[test]
    fn check_common_depending_on_others_fails() {
        let mut c = base();
        c[1].deps.push("scion-store".into());
        c[0].deps.push("scion-client".into());
        assert_eq!(check(&c).len(), 2);
    }

    #[test]
    fn check_server_upward_dependency_fails() {
        let mut c = base();
        c[2].deps.push("scion-api".into()); // store -> api
        assert_eq!(check(&c).len(), 1);
    }

    #[test]
    fn check_server_depending_on_client_or_tool_fails() {
        let mut c = base();
        c[3].deps.push("scion-client".into());
        c[3].deps.push("xtask".into());
        assert_eq!(check(&c).len(), 2);
    }

    #[test]
    fn check_unregistered_server_crate_fails() {
        let mut c = base();
        c.push(k("scion-new", Area::Server, &["scion-object"]));
        let errs = check(&c);
        assert!(errs.iter().any(|e| e.contains("register")));
    }

    #[test]
    fn check_ignores_external_deps_and_tools() {
        let mut c = base();
        c[2].deps.push("serde".into());
        c[7].deps.push("scion-api".into());
        assert!(check(&c).is_empty());
    }

    #[test]
    fn real_workspace_passes() {
        let crates = load_workspace().expect("cargo metadata");
        assert!(crates.iter().any(|k| k.name == "scion-object"));
        assert_eq!(check(&crates), Vec::<String>::new());
    }
}
