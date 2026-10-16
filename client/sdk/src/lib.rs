//! Scion 官方 Rust SDK。
//!
//! 只依赖公开协议（`scion-proto`）和对象模型（`scion-object`）：SDK 能做到的，
//! 任何语言照着协议也能做到。
//!
//! ```no_run
//! # async fn demo() -> scion_client::Result<()> {
//! use scion_client::{Auth, Client};
//! let c = Client::builder()
//!     .endpoint("http://127.0.0.1:7080")
//!     .repo("mono")
//!     .auth(Auth::from_env())
//!     .build()
//!     .await?;
//! println!("server protocol {}", c.server_info().protocol_version);
//! # Ok(()) }
//! ```

mod client;
mod error;
mod info;
mod ops;

pub use client::{Auth, Client, ClientBuilder};
pub use error::{Error, Result};
pub use info::{Limits, ProtocolVersion, ReadEndpoint, Role, ServerInfo};
pub use ops::{ChangesetBuilder, CreatedChangeset, DirEntry, Landed, Person, RefTip};
pub use scion_object::{EntryMode, ObjectId, ObjectKind};
/// gRPC 状态码，出现在 [`Error::Rpc`] 中。
pub use tonic::Code;
