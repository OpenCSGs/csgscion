//! Scion 对象模型：ObjectId、规范编码，以及 Tree / Changeset / ChunkList。
//!
//! 本 crate 只做纯计算，不做任何 IO；格式定义见仓库根目录的 `SPEC.md`。
//! 服务端与客户端共用，客户端只允许依赖它和 `scion-proto`。

mod changeset;
mod chunklist;
mod codec;
mod error;
mod id;
mod path;
mod tree;

pub use changeset::{Changeset, FileChange, FileOp, MAX_TZ_OFFSET_MINUTES, Signature};
pub use chunklist::{Chunk, ChunkList};
pub use error::{Error, Result};
pub use id::{DIGEST_LEN, HashAlgo, ID_LEN, ObjectId, ObjectKind};
pub use path::{MAX_NAME_LEN, MAX_PATH_LEN, validate_name, validate_path};
pub use tree::{EntryMode, Tree, TreeEntry};

/// 规范编码的格式版本（SPEC.md §3）。
pub const FORMAT_VERSION: u8 = 0x01;
