//! Tree（SPEC.md §4）。

use crate::codec::{Reader, put_id, put_str, put_uvarint};
use crate::error::{Error, Result};
use crate::id::{ObjectId, ObjectKind};
use crate::path::validate_name;

/// Tree 条目的类型。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum EntryMode {
    File = 0x01,
    Executable = 0x02,
    Symlink = 0x03,
    Tree = 0x04,
}

impl EntryMode {
    pub fn from_u8(v: u8) -> Result<Self> {
        match v {
            0x01 => Ok(EntryMode::File),
            0x02 => Ok(EntryMode::Executable),
            0x03 => Ok(EntryMode::Symlink),
            0x04 => Ok(EntryMode::Tree),
            other => Err(Error::UnknownMode(other)),
        }
    }

    /// 该 mode 的条目所引用对象应有的 kind。
    pub fn object_kind(self) -> ObjectKind {
        match self {
            EntryMode::Tree => ObjectKind::Tree,
            EntryMode::File | EntryMode::Executable | EntryMode::Symlink => ObjectKind::Blob,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TreeEntry {
    pub name: String,
    pub mode: EntryMode,
    pub id: ObjectId,
    /// 文件字节数；子目录为 0。
    pub size: u64,
}

impl TreeEntry {
    fn validate(&self) -> Result<()> {
        validate_name(&self.name)?;
        if self.id.kind() != self.mode.object_kind() {
            return Err(Error::IdKindMismatch("tree entry mode"));
        }
        if self.mode == EntryMode::Tree && self.size != 0 {
            return Err(Error::NonzeroTreeSize(self.name.clone()));
        }
        Ok(())
    }
}

/// 一个目录的快照。条目始终按名字字节序严格升序。
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Tree {
    entries: Vec<TreeEntry>,
}

const KIND: u8 = ObjectKind::Tree as u8;

impl Tree {
    /// 由任意顺序的条目构造 Tree：自动排序，并校验名字、mode 与 id 是否匹配、是否重名。
    ///
    /// ```
    /// use scion_object::{EntryMode, ObjectId, Tree, TreeEntry};
    /// let tree = Tree::new(vec![TreeEntry {
    ///     name: "README".into(),
    ///     mode: EntryMode::File,
    ///     id: ObjectId::of_blob(b"hi"),
    ///     size: 2,
    /// }])?;
    /// assert_eq!(Tree::decode(&tree.encode())?, tree);
    /// # Ok::<(), scion_object::Error>(())
    /// ```
    pub fn new(mut entries: Vec<TreeEntry>) -> Result<Self> {
        entries.sort_by(|a, b| a.name.as_bytes().cmp(b.name.as_bytes()));
        let tree = Self { entries };
        tree.validate()?;
        Ok(tree)
    }

    pub fn entries(&self) -> &[TreeEntry] {
        &self.entries
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// 按名字查找条目（二分查找）。
    pub fn get(&self, name: &str) -> Option<&TreeEntry> {
        self.entries
            .binary_search_by(|e| e.name.as_bytes().cmp(name.as_bytes()))
            .ok()
            .map(|i| &self.entries[i])
    }

    fn validate(&self) -> Result<()> {
        for e in &self.entries {
            e.validate()?;
        }
        let sorted = self
            .entries
            .windows(2)
            .all(|w| w[0].name.as_bytes() < w[1].name.as_bytes());
        if !sorted {
            return Err(Error::NotSorted("tree entries"));
        }
        Ok(())
    }

    /// 规范编码。
    pub fn encode(&self) -> Vec<u8> {
        let mut out = vec![KIND, crate::FORMAT_VERSION];
        put_uvarint(&mut out, self.entries.len() as u64);
        for e in &self.entries {
            put_str(&mut out, &e.name);
            out.push(e.mode as u8);
            put_id(&mut out, &e.id);
            put_uvarint(&mut out, e.size);
        }
        out
    }

    /// 严格解码：输入必须是规范编码，否则报错。
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let mut r = Reader::new(bytes);
        r.header(KIND)?;
        let n = r.count()?;
        let mut entries = Vec::with_capacity(n);
        for _ in 0..n {
            let name = r.string()?;
            let mode = EntryMode::from_u8(r.u8()?)?;
            let id = r.id()?;
            let size = r.uvarint()?;
            entries.push(TreeEntry { name, mode, id, size });
        }
        r.finish()?;
        let tree = Self { entries };
        tree.validate()?;
        Ok(tree)
    }

    pub fn id(&self) -> ObjectId {
        ObjectId::hash(ObjectKind::Tree, &self.encode())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    fn file(name: &str, content: &[u8]) -> TreeEntry {
        TreeEntry {
            name: name.into(),
            mode: EntryMode::File,
            id: ObjectId::of_blob(content),
            size: content.len() as u64,
        }
    }

    fn dir(name: &str) -> TreeEntry {
        TreeEntry {
            name: name.into(),
            mode: EntryMode::Tree,
            id: Tree::default().id(),
            size: 0,
        }
    }

    #[test]
    fn entry_mode_from_u8_all() {
        for m in [
            EntryMode::File,
            EntryMode::Executable,
            EntryMode::Symlink,
            EntryMode::Tree,
        ] {
            assert_eq!(EntryMode::from_u8(m as u8), Ok(m));
        }
        assert_eq!(EntryMode::from_u8(0), Err(Error::UnknownMode(0)));
        assert_eq!(EntryMode::from_u8(5), Err(Error::UnknownMode(5)));
    }

    #[test]
    fn entry_mode_object_kind() {
        assert_eq!(EntryMode::Tree.object_kind(), ObjectKind::Tree);
        assert_eq!(EntryMode::Symlink.object_kind(), ObjectKind::Blob);
        assert_eq!(EntryMode::Executable.object_kind(), ObjectKind::Blob);
    }

    #[test]
    fn empty_tree_encoding() {
        let t = Tree::default();
        assert!(t.is_empty());
        assert_eq!(t.encode(), [0x02, 0x01, 0x00]);
        assert_eq!(Tree::decode(&[0x02, 0x01, 0x00]), Ok(t));
    }

    #[test]
    fn new_sorts_by_bytes() {
        let t = Tree::new(vec![file("b", b"1"), file("B", b"2"), file("a", b"3")]).expect("valid");
        let names: Vec<_> = t.entries().iter().map(|e| e.name.as_str()).collect();
        assert_eq!(names, ["B", "a", "b"]);
    }

    #[test]
    fn new_duplicate_name_rejected() {
        let r = Tree::new(vec![file("a", b"1"), file("a", b"2")]);
        assert_eq!(r, Err(Error::NotSorted("tree entries")));
    }

    #[test]
    fn new_invalid_name_rejected() {
        assert!(matches!(Tree::new(vec![file("a/b", b"")]), Err(Error::InvalidName(_))));
    }

    #[test]
    fn new_mode_kind_mismatch_rejected() {
        let mut e = file("a", b"");
        e.mode = EntryMode::Tree;
        e.size = 0;
        assert_eq!(Tree::new(vec![e]), Err(Error::IdKindMismatch("tree entry mode")));
    }

    #[test]
    fn new_dir_nonzero_size_rejected() {
        let mut d = dir("d");
        d.size = 1;
        assert_eq!(Tree::new(vec![d]), Err(Error::NonzeroTreeSize("d".into())));
    }

    #[test]
    fn get_finds_present_and_absent() {
        let t = Tree::new(vec![file("a", b""), dir("src"), file("z", b"")]).expect("valid");
        assert_eq!(t.get("src").map(|e| e.mode), Some(EntryMode::Tree));
        assert!(t.get("nope").is_none());
        assert!(Tree::default().get("a").is_none());
    }

    #[test]
    fn id_changes_with_content() {
        let a = Tree::new(vec![file("a", b"1")]).expect("valid");
        let b = Tree::new(vec![file("a", b"2")]).expect("valid");
        assert_ne!(a.id(), b.id());
        assert_eq!(a.id().kind(), ObjectKind::Tree);
        assert!(a.id().verify(&a.encode()));
    }

    #[test]
    fn decode_unsorted_rejected() {
        let mut bytes = vec![0x02, 0x01, 0x02];
        for name in ["b", "a"] {
            put_str(&mut bytes, name);
            bytes.push(0x01);
            put_id(&mut bytes, &ObjectId::of_blob(b""));
            bytes.push(0x00);
        }
        assert_eq!(Tree::decode(&bytes), Err(Error::NotSorted("tree entries")));
    }

    #[test]
    fn decode_wrong_header_rejected() {
        assert_eq!(
            Tree::decode(&[0x03, 0x01, 0x00]),
            Err(Error::WrongKind { expected: 2, found: 3 })
        );
        assert_eq!(Tree::decode(&[0x02, 0x09, 0x00]), Err(Error::UnsupportedVersion(9)));
    }

    #[test]
    fn decode_trailing_bytes_rejected() {
        assert_eq!(Tree::decode(&[0x02, 0x01, 0x00, 0x00]), Err(Error::TrailingBytes(1)));
    }

    #[test]
    fn decode_truncated_rejected() {
        let t = Tree::new(vec![file("a", b"x")]).expect("valid");
        let bytes = t.encode();
        for cut in 0..bytes.len() {
            assert!(Tree::decode(&bytes[..cut]).is_err(), "cut at {cut}");
        }
    }

    #[test]
    fn decode_unknown_mode_rejected() {
        let mut bytes = vec![0x02, 0x01, 0x01];
        put_str(&mut bytes, "a");
        bytes.push(0x09);
        assert_eq!(Tree::decode(&bytes), Err(Error::UnknownMode(9)));
    }

    fn arb_entry() -> impl Strategy<Value = TreeEntry> {
        (
            "[a-zA-Z0-9._-]{1,12}".prop_filter("dot names", |s| s != "." && s != ".."),
            prop_oneof![
                Just(EntryMode::File),
                Just(EntryMode::Executable),
                Just(EntryMode::Symlink),
                Just(EntryMode::Tree)
            ],
            any::<[u8; 32]>(),
            any::<u64>(),
        )
            .prop_map(|(name, mode, digest, size)| TreeEntry {
                name,
                mode,
                id: ObjectId::new(crate::HashAlgo::Blake3_256, mode.object_kind(), digest),
                size: if mode == EntryMode::Tree { 0 } else { size },
            })
    }

    proptest! {
        #[test]
        fn roundtrip_any(entries in proptest::collection::vec(arb_entry(), 0..20)) {
            let mut seen = std::collections::HashSet::new();
            let entries: Vec<_> = entries.into_iter().filter(|e| seen.insert(e.name.clone())).collect();
            let t = Tree::new(entries).expect("unique valid entries");
            let bytes = t.encode();
            prop_assert_eq!(Tree::decode(&bytes), Ok(t.clone()));
            // 编码唯一：重新编码得到相同字节
            prop_assert_eq!(Tree::decode(&bytes).map(|x| x.encode()), Ok(bytes));
        }

        #[test]
        fn decode_never_panics(b in proptest::collection::vec(any::<u8>(), 0..256)) {
            let _ = Tree::decode(&b);
        }
    }
}
