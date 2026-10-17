//! Scion 的认证与鉴权（服务端设计文档 §3.7、§9）。

pub mod config;
pub mod external;
pub mod guard;
pub mod oidc;
pub mod policy;
pub mod remote;
pub mod token;

#[cfg(test)]
mod testing;

pub use config::AuthConfig;
pub use guard::{AuthError, Guard, GuardConfig, POLICY_KEY};
pub use policy::{Action, Grant, Policy, Principal, Request};
