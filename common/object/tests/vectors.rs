//! Golden 测试向量（SPEC.md §9）。
//!
//! - `cargo test -p scion-object --test vectors`：读取 `testdata/vectors/objects.json` 逐条校验。
//! - `SCION_BLESS=1 cargo test -p scion-object --test vectors`：由下方 `build_vectors` 重新生成该文件。
//!   向量一旦发布就不能改变，重新生成后 `git diff` 必须为空，除非是新增向量。

use std::collections::BTreeMap;
use std::path::PathBuf;

use scion_object::{
    Changeset, Chunk, ChunkList, EntryMode, Error, FileChange, FileOp, ObjectId, ObjectKind, Signature, Tree, TreeEntry,
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

#[derive(Debug, Serialize, Deserialize)]
struct VectorFile {
    valid: Vec<ValidVector>,
    invalid: Vec<InvalidVector>,
}

#[derive(Debug, Serialize, Deserialize)]
struct ValidVector {
    name: String,
    kind: String,
    input: Value,
    encoding: String,
    id: String,
}

#[derive(Debug, Serialize, Deserialize)]
struct InvalidVector {
    name: String,
    kind: String,
    encoding: String,
    error: String,
}

fn vectors_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../testdata/vectors/objects.json")
}

// ---------- 结构 <-> JSON ----------

fn mode_str(m: EntryMode) -> &'static str {
    match m {
        EntryMode::File => "file",
        EntryMode::Executable => "executable",
        EntryMode::Symlink => "symlink",
        EntryMode::Tree => "tree",
    }
}

fn mode_from(s: &str) -> EntryMode {
    match s {
        "file" => EntryMode::File,
        "executable" => EntryMode::Executable,
        "symlink" => EntryMode::Symlink,
        "tree" => EntryMode::Tree,
        other => panic!("unknown mode {other}"),
    }
}

fn id_from(v: &Value) -> ObjectId {
    v.as_str().expect("id string").parse().expect("valid id")
}

fn u64_from(v: &Value) -> u64 {
    v.as_u64().expect("u64")
}

fn sig_json(s: &Signature) -> Value {
    json!({"name": s.name, "email": s.email, "time_ms": s.time_ms, "tz_offset_minutes": s.tz_offset_minutes})
}

fn sig_from(v: &Value) -> Signature {
    Signature {
        name: v["name"].as_str().expect("name").into(),
        email: v["email"].as_str().expect("email").into(),
        time_ms: v["time_ms"].as_i64().expect("time_ms"),
        tz_offset_minutes: v["tz_offset_minutes"].as_i64().expect("tz") as i32,
    }
}

fn tree_json(t: &Tree) -> Value {
    let entries: Vec<_> = t
        .entries()
        .iter()
        .map(|e| json!({"name": e.name, "mode": mode_str(e.mode), "id": e.id.to_string(), "size": e.size}))
        .collect();
    json!({ "entries": entries })
}

fn tree_from(v: &Value) -> Tree {
    let entries = v["entries"]
        .as_array()
        .expect("entries")
        .iter()
        .map(|e| TreeEntry {
            name: e["name"].as_str().expect("name").into(),
            mode: mode_from(e["mode"].as_str().expect("mode")),
            id: id_from(&e["id"]),
            size: u64_from(&e["size"]),
        })
        .collect();
    Tree::new(entries).expect("valid tree input")
}

fn changeset_json(c: &Changeset) -> Value {
    let delta: Vec<_> = c
        .delta
        .iter()
        .map(|ch| match &ch.op {
            FileOp::Put { mode, id, size } => json!({
                "path": ch.path, "op": "put", "mode": mode_str(*mode), "id": id.to_string(), "size": size
            }),
            FileOp::Delete => json!({"path": ch.path, "op": "delete"}),
        })
        .collect();
    let ext: BTreeMap<_, _> = c.extensions.iter().map(|(k, v)| (k.clone(), hex::encode(v))).collect();
    json!({
        "parents": c.parents.iter().map(ToString::to_string).collect::<Vec<_>>(),
        "author": sig_json(&c.author),
        "committer": sig_json(&c.committer),
        "message": c.message,
        "change_id": hex::encode(c.change_id),
        "delta": delta,
        "extensions": ext,
    })
}

fn changeset_from(v: &Value) -> Changeset {
    let parents = v["parents"].as_array().expect("parents").iter().map(id_from).collect();
    let mut change_id = [0u8; 16];
    hex::decode_to_slice(v["change_id"].as_str().expect("change_id"), &mut change_id).expect("hex");
    let delta = v["delta"]
        .as_array()
        .expect("delta")
        .iter()
        .map(|d| {
            let path = d["path"].as_str().expect("path");
            match d["op"].as_str().expect("op") {
                "put" => FileChange::put(
                    path,
                    mode_from(d["mode"].as_str().expect("mode")),
                    id_from(&d["id"]),
                    u64_from(&d["size"]),
                ),
                "delete" => FileChange::delete(path),
                other => panic!("unknown op {other}"),
            }
        })
        .collect();
    let extensions = v["extensions"]
        .as_object()
        .expect("extensions")
        .iter()
        .map(|(k, val)| (k.clone(), hex::decode(val.as_str().expect("hex")).expect("hex")))
        .collect();
    Changeset::new(
        parents,
        sig_from(&v["author"]),
        sig_from(&v["committer"]),
        v["message"].as_str().expect("message"),
        change_id,
        delta,
        extensions,
    )
    .expect("valid changeset input")
}

fn chunk_list_json(l: &ChunkList) -> Value {
    let chunks: Vec<_> = l
        .chunks()
        .iter()
        .map(|c| json!({"id": c.id.to_string(), "size": c.size}))
        .collect();
    json!({ "chunks": chunks })
}

fn chunk_list_from(v: &Value) -> ChunkList {
    let chunks = v["chunks"]
        .as_array()
        .expect("chunks")
        .iter()
        .map(|c| Chunk {
            id: id_from(&c["id"]),
            size: u64_from(&c["size"]),
        })
        .collect();
    ChunkList::new(chunks).expect("valid chunk list input")
}

/// 由结构化输入得到 (规范编码, id)。
fn encode_input(kind: &str, input: &Value) -> (Vec<u8>, ObjectId) {
    match kind {
        "blob" => {
            let data = hex::decode(input["data_hex"].as_str().expect("data_hex")).expect("hex");
            let id = ObjectId::of_blob(&data);
            (data, id)
        }
        "tree" => {
            let t = tree_from(input);
            (t.encode(), t.id())
        }
        "changeset" => {
            let c = changeset_from(input);
            (c.encode(), c.id())
        }
        "chunk_list" => {
            let l = chunk_list_from(input);
            (l.encode(), l.id())
        }
        other => panic!("unknown kind {other}"),
    }
}

/// 解码并重新转成 JSON 输入（用于检查解码结果与输入一致）。
fn decode_to_json(kind: &str, bytes: &[u8]) -> Result<Value, Error> {
    match kind {
        "blob" => Ok(json!({ "data_hex": hex::encode(bytes) })),
        "tree" => Tree::decode(bytes).map(|t| tree_json(&t)),
        "changeset" => Changeset::decode(bytes).map(|c| changeset_json(&c)),
        "chunk_list" => ChunkList::decode(bytes).map(|l| chunk_list_json(&l)),
        other => panic!("unknown kind {other}"),
    }
}

// ---------- 向量定义（仅 bless 时使用） ----------

fn sig(name: &str, time_ms: i64, tz: i32) -> Signature {
    Signature {
        name: name.into(),
        email: format!("{name}@example.com"),
        time_ms,
        tz_offset_minutes: tz,
    }
}

fn build_vectors() -> VectorFile {
    let mut valid = Vec::new();
    let mut push = |name: &str, kind: &str, input: Value| {
        let (bytes, id) = encode_input(kind, &input);
        valid.push(ValidVector {
            name: name.into(),
            kind: kind.into(),
            input,
            encoding: hex::encode(bytes),
            id: id.to_string(),
        });
    };

    // Blob
    push("blob_empty", "blob", json!({"data_hex": ""}));
    push("blob_hello", "blob", json!({"data_hex": hex::encode("hello\n")}));
    let bin: Vec<u8> = (0u8..=255).collect();
    push("blob_all_bytes", "blob", json!({"data_hex": hex::encode(&bin)}));

    // Tree
    let readme = ObjectId::of_blob(b"hello\n");
    let run = ObjectId::of_blob(b"#!/bin/sh\necho hi\n");
    let empty_tree = Tree::default();
    push("tree_empty", "tree", tree_json(&empty_tree));
    let single = Tree::new(vec![TreeEntry {
        name: "README".into(),
        mode: EntryMode::File,
        id: readme,
        size: 6,
    }])
    .expect("valid");
    push("tree_single_file", "tree", tree_json(&single));
    let src = Tree::new(vec![TreeEntry {
        name: "main.rs".into(),
        mode: EntryMode::File,
        id: readme,
        size: 6,
    }])
    .expect("valid");
    let mixed = Tree::new(vec![
        TreeEntry {
            name: "src".into(),
            mode: EntryMode::Tree,
            id: src.id(),
            size: 0,
        },
        TreeEntry {
            name: "README".into(),
            mode: EntryMode::File,
            id: readme,
            size: 6,
        },
        TreeEntry {
            name: "run.sh".into(),
            mode: EntryMode::Executable,
            id: run,
            size: 18,
        },
        TreeEntry {
            name: "latest".into(),
            mode: EntryMode::Symlink,
            id: ObjectId::of_blob(b"run.sh"),
            size: 6,
        },
        TreeEntry {
            name: "模型.md".into(),
            mode: EntryMode::File,
            id: readme,
            size: 6,
        },
        TreeEntry {
            name: "Zeta".into(),
            mode: EntryMode::File,
            id: readme,
            size: 6,
        },
    ])
    .expect("valid");
    push("tree_mixed_modes_byte_order", "tree", tree_json(&mixed));
    let big = Tree::new(vec![TreeEntry {
        name: "weights.bin".into(),
        mode: EntryMode::File,
        id: ObjectId::of_blob(b"pretend"),
        size: 70_000_000_000,
    }])
    .expect("valid");
    push("tree_large_size_varint", "tree", tree_json(&big));

    // Changeset
    let root = Changeset::new(
        vec![],
        sig("ada", 1_760_000_000_000, 480),
        sig("ada", 1_760_000_000_000, 480),
        "init\n",
        [0x11; 16],
        vec![
            FileChange::put("README", EntryMode::File, readme, 6),
            FileChange::put("src/main.rs", EntryMode::File, readme, 6),
        ],
        BTreeMap::new(),
    )
    .expect("valid");
    push("changeset_root", "changeset", changeset_json(&root));
    let child = Changeset::new(
        vec![root.id()],
        sig("bob", 1_760_000_100_000, -300),
        sig("ci-bot", 1_760_000_200_000, 0),
        "fix: 修复\n\nbody",
        [0x22; 16],
        vec![
            FileChange::delete("README"),
            FileChange::put("bin/run.sh", EntryMode::Executable, run, 18),
            FileChange::put("latest", EntryMode::Symlink, ObjectId::of_blob(b"bin/run.sh"), 10),
        ],
        BTreeMap::from([
            ("git.sha1".to_string(), vec![0xab; 20]),
            ("agent".to_string(), b"claude".to_vec()),
        ]),
    )
    .expect("valid");
    push("changeset_child_with_extensions", "changeset", changeset_json(&child));
    let merge = Changeset::new(
        vec![child.id(), root.id()],
        sig("ada", -1, -1440),
        sig("ada", i64::MAX, 1440),
        "",
        [0x00; 16],
        vec![],
        BTreeMap::new(),
    )
    .expect("valid");
    push("changeset_merge_extremes", "changeset", changeset_json(&merge));

    // ChunkList
    let cl = ChunkList::new(vec![
        Chunk {
            id: ObjectId::of_blob(b"part-1"),
            size: 8_388_608,
        },
        Chunk {
            id: ObjectId::of_blob(b"part-2"),
            size: 1_000,
        },
    ])
    .expect("valid");
    push("chunk_list_two_chunks", "chunk_list", chunk_list_json(&cl));

    // ---- 非法输入：由合法编码变形得到，并断言错误码与意图一致 ----
    let tree_enc = single.encode();
    let cs_enc = root.encode();
    let mut invalid = Vec::new();
    let mut bad = |name: &str, kind: &str, bytes: Vec<u8>, error: &str| {
        let got = decode_to_json(kind, &bytes).expect_err("must be rejected").code();
        assert_eq!(got, error, "vector {name}");
        invalid.push(InvalidVector {
            name: name.into(),
            kind: kind.into(),
            encoding: hex::encode(bytes),
            error: error.into(),
        });
    };
    bad("tree_empty_input", "tree", vec![], "unexpected_eof");
    bad(
        "tree_truncated",
        "tree",
        tree_enc[..tree_enc.len() - 1].to_vec(),
        "unexpected_eof",
    );
    bad(
        "tree_trailing_byte",
        "tree",
        [tree_enc.clone(), vec![0]].concat(),
        "trailing_bytes",
    );
    bad("tree_wrong_kind", "tree", vec![0x03, 0x01, 0x00], "wrong_kind");
    bad("tree_version_2", "tree", vec![0x02, 0x02, 0x00], "unsupported_version");
    bad(
        "tree_count_non_minimal",
        "tree",
        vec![0x02, 0x01, 0x80, 0x00],
        "varint_non_minimal",
    );
    bad(
        "tree_count_overflow",
        "tree",
        vec![0x02, 0x01, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x02],
        "varint_overflow",
    );
    bad(
        "tree_count_exceeds_input",
        "tree",
        vec![0x02, 0x01, 0x05, 0x00],
        "length_out_of_range",
    );
    let entry = |name: &[u8], mode: u8, id: ObjectId, size: u8| {
        let mut b = vec![name.len() as u8];
        b.extend_from_slice(name);
        b.push(mode);
        b.extend_from_slice(&id.to_bytes());
        b.push(size);
        b
    };
    let tree_of = |entries: Vec<Vec<u8>>| [vec![0x02, 0x01, entries.len() as u8], entries.concat()].concat();
    bad(
        "tree_unsorted",
        "tree",
        tree_of(vec![entry(b"b", 1, readme, 6), entry(b"a", 1, readme, 6)]),
        "not_sorted",
    );
    bad(
        "tree_duplicate_name",
        "tree",
        tree_of(vec![entry(b"a", 1, readme, 6), entry(b"a", 1, readme, 6)]),
        "not_sorted",
    );
    bad(
        "tree_name_with_slash",
        "tree",
        tree_of(vec![entry(b"a/b", 1, readme, 6)]),
        "invalid_name",
    );
    bad(
        "tree_name_dotdot",
        "tree",
        tree_of(vec![entry(b"..", 1, readme, 6)]),
        "invalid_name",
    );
    bad(
        "tree_name_empty",
        "tree",
        tree_of(vec![entry(b"", 1, readme, 6)]),
        "invalid_name",
    );
    bad(
        "tree_name_invalid_utf8",
        "tree",
        tree_of(vec![entry(&[0xc3, 0x28], 1, readme, 6)]),
        "invalid_utf8",
    );
    bad(
        "tree_unknown_mode",
        "tree",
        tree_of(vec![entry(b"a", 9, readme, 6)]),
        "unknown_mode",
    );
    bad(
        "tree_dir_points_to_blob",
        "tree",
        tree_of(vec![entry(b"d", 4, readme, 0)]),
        "id_kind_mismatch",
    );
    bad(
        "tree_dir_nonzero_size",
        "tree",
        tree_of(vec![entry(b"d", 4, empty_tree.id(), 1)]),
        "nonzero_tree_size",
    );
    let mut unknown_algo = entry(b"a", 1, readme, 6);
    unknown_algo[3] = 0x07; // 名字占 2 字节，mode 1 字节，随后是 id 的 algo 字节
    bad("tree_unknown_algo", "tree", tree_of(vec![unknown_algo]), "unknown_algo");
    let mut unknown_kind = entry(b"a", 1, readme, 6);
    unknown_kind[4] = 0x04;
    bad("tree_unknown_kind", "tree", tree_of(vec![unknown_kind]), "unknown_kind");

    let mut dup_parent = root.clone();
    dup_parent.parents = vec![child.id(), child.id()];
    bad(
        "changeset_duplicate_parent",
        "changeset",
        dup_parent.encode(),
        "duplicate_parent",
    );
    let mut tree_parent = root.clone();
    tree_parent.parents = vec![empty_tree.id()];
    bad(
        "changeset_parent_is_tree",
        "changeset",
        tree_parent.encode(),
        "id_kind_mismatch",
    );
    let mut unsorted = root.clone();
    unsorted.delta.reverse();
    bad("changeset_delta_unsorted", "changeset", unsorted.encode(), "not_sorted");
    let mut bad_path = root.clone();
    bad_path.delta = vec![FileChange::delete("a//b")];
    bad("changeset_invalid_path", "changeset", bad_path.encode(), "invalid_path");
    let mut put_dir = root.clone();
    put_dir.delta = vec![FileChange::put("d", EntryMode::Tree, empty_tree.id(), 0)];
    bad("changeset_put_tree_mode", "changeset", put_dir.encode(), "unknown_mode");
    let mut tz = root.clone();
    tz.author.tz_offset_minutes = 1441;
    bad(
        "changeset_tz_out_of_range",
        "changeset",
        tz.encode(),
        "invalid_tz_offset",
    );
    let mut empty_key = root.clone();
    empty_key.extensions.insert(String::new(), vec![1]);
    bad(
        "changeset_empty_extension_key",
        "changeset",
        empty_key.encode(),
        "invalid_extension_key",
    );
    let mut bad_op = root.clone();
    bad_op.delta = vec![FileChange::delete("a")];
    let mut bytes = bad_op.encode();
    let n = bytes.len();
    bytes[n - 2] = 0x07; // 结尾为 op(0x02) + extensions 个数(0x00)
    bad("changeset_unknown_op", "changeset", bytes, "unknown_op");
    bad(
        "changeset_trailing_byte",
        "changeset",
        [cs_enc.clone(), vec![0]].concat(),
        "trailing_bytes",
    );

    let cl_enc = cl.encode();
    let mut wrong_total = cl_enc.clone();
    wrong_total[2] ^= 0x01;
    bad(
        "chunk_list_total_mismatch",
        "chunk_list",
        wrong_total,
        "invalid_chunk_list",
    );
    bad(
        "chunk_list_no_chunks",
        "chunk_list",
        vec![0x05, 0x01, 0x00, 0x00],
        "invalid_chunk_list",
    );

    VectorFile { valid, invalid }
}

// ---------- 测试 ----------

#[test]
fn golden_vectors() {
    let path = vectors_path();
    if std::env::var_os("SCION_BLESS").is_some() {
        let file = build_vectors();
        let text = serde_json::to_string_pretty(&file).expect("serialize") + "\n";
        std::fs::write(&path, text).expect("write vectors");
    }
    let text = std::fs::read_to_string(&path)
        .unwrap_or_else(|e| panic!("read {}: {e}; run with SCION_BLESS=1 to generate", path.display()));
    let file: VectorFile = serde_json::from_str(&text).expect("parse vectors");
    assert!(!file.valid.is_empty() && !file.invalid.is_empty());

    for v in &file.valid {
        let (bytes, id) = encode_input(&v.kind, &v.input);
        assert_eq!(hex::encode(&bytes), v.encoding, "encoding mismatch: {}", v.name);
        assert_eq!(id.to_string(), v.id, "id mismatch: {}", v.name);
        let decoded = decode_to_json(&v.kind, &bytes).unwrap_or_else(|e| panic!("{}: {e}", v.name));
        assert_eq!(decoded, v.input, "decode mismatch: {}", v.name);
        let expected_kind = match v.kind.as_str() {
            "blob" => ObjectKind::Blob,
            "tree" => ObjectKind::Tree,
            "changeset" => ObjectKind::Changeset,
            _ => ObjectKind::ChunkList,
        };
        assert_eq!(id.kind(), expected_kind, "{}", v.name);
        assert!(id.verify(&bytes), "{}", v.name);
    }

    for v in &file.invalid {
        let bytes = hex::decode(&v.encoding).expect("hex");
        let err = decode_to_json(&v.kind, &bytes).expect_err(&v.name);
        assert_eq!(err.code(), v.error, "error code mismatch: {}", v.name);
    }
}

#[test]
fn golden_vectors_file_matches_builder() {
    // 防止有人手改 JSON 或改了构造逻辑却没有重新生成
    if std::env::var_os("SCION_BLESS").is_some() {
        return; // 正在重新生成，由 golden_vectors 写文件
    }
    let built = serde_json::to_value(build_vectors()).expect("serialize");
    let text = std::fs::read_to_string(vectors_path()).expect("read vectors");
    let on_disk: Value = serde_json::from_str(&text).expect("parse");
    assert_eq!(
        built, on_disk,
        "vectors out of date; run with SCION_BLESS=1 and review the diff"
    );
}
