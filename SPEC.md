# Scion 对象模型与规范编码（v1）

本文是 Scion 对象格式的**唯一权威定义**。任何语言的实现只要遵循本文，并通过 `testdata/vectors/` 下的全部 golden 测试向量，就能与官方实现计算出完全相同的对象 ID。

- 普通客户端只需要用到 §1 的 ObjectId 和 §1.2 的「下载后校验」，不必实现完整编解码。
- 规范编码是**唯一**的：同一个逻辑对象只有一种合法字节序列；解码器必须拒绝任何其它写法（§6）。

---

## 1. ObjectId

二进制形式固定 34 字节：

```
algo (u8) | kind (u8) | digest (32 字节)
```

| algo | 名称 | digest 计算 |
|---|---|---|
| `0x01` | BLAKE3-256 | BLAKE3 默认模式，输出 32 字节 |

| kind | 名称 | 文本前缀 | digest 输入 |
|---|---|---|---|
| `0x01` | Blob | `b` | 文件原始字节（不含任何头部） |
| `0x02` | Tree | `t` | Tree 的规范编码（§4） |
| `0x03` | Changeset | `c` | Changeset 的规范编码（§5） |
| `0x05` | ChunkList | `l` | ChunkList 的规范编码（§7），仅服务端存储内部使用 |

其它取值保留，解码时必须拒绝。

### 1.1 文本形式

```
<前缀字母><algo 十进制><64 位小写十六进制 digest>
```

例：空文件的 Blob ID 为 `b1af1349b9f5f9a1a6a0404dea36dcc9499bcb25c9adc112b7cc9a93cae41f3262`。

解析时必须拒绝：大写十六进制、长度不对、未知前缀或 algo。

### 1.2 下载后校验

对任意 kind，都满足 `digest = BLAKE3(对象字节)`，其中「对象字节」对 Blob 是文件内容，对其它 kind 是规范编码。客户端收到对象后**必须**按此校验，不一致即视为损坏，不得信任任何缓存层。

---

## 2. 基本类型

| 记号 | 编码 |
|---|---|
| `u8` | 1 字节 |
| `uvarint` | 无符号 LEB128，最多 10 字节，值域 u64 |
| `svarint` | 有符号整数先做 ZigZag（`(n << 1) ^ (n >> 63)`），再按 `uvarint` 编码 |
| `bytes` | `uvarint` 长度 + 原始字节 |
| `string` | 同 `bytes`，内容必须是合法 UTF-8 |
| `id` | 34 字节 ObjectId 二进制形式，无长度前缀 |
| `list<T>` | `uvarint` 元素个数 + 依次排列的元素 |

**varint 必须是最短编码**：除了值 0 编码为单字节 `0x00`，最后一个字节不能是 `0x00`（例如 `0x80 0x00` 非法）。超过 10 字节，或第 10 字节大于 `0x01`（溢出 u64）都非法。

---

## 3. 对象头

Tree、Changeset、ChunkList 的编码都以 2 字节头部开始：

```
kind (u8) | format_version (u8)
```

v1 中 `format_version` 固定为 `0x01`。头部的 kind 必须与解码时期望的 kind 一致。

---

## 4. Tree

```
header        = 0x02 0x01
entries       = list<Entry>

Entry:
  name        string
  mode        u8
  id          id
  size        uvarint
```

| mode | 含义 | id 的 kind | size |
|---|---|---|---|
| `0x01` | 普通文件 | Blob | 文件字节数 |
| `0x02` | 可执行文件 | Blob | 文件字节数 |
| `0x03` | 符号链接（内容是链接目标） | Blob | 目标字节数 |
| `0x04` | 子目录 | Tree | 必须为 0 |

约束：
- 条目按 `name` 的**字节序严格升序**排列，不允许重名。
- `name` 必须满足 §8 的文件名规则。
- 空 Tree（0 个条目）合法，用于表示空仓库的根目录。

---

## 5. Changeset

```
header        = 0x03 0x01
parents       list<id>            # 有序，第一个是 first parent；kind 必须是 Changeset
author        Signature
committer     Signature
message       string
change_id     16 字节（无长度前缀）
delta         list<FileChange>
extensions    list<Extension>

Signature:
  name        string
  email       string
  time_ms     svarint             # Unix 毫秒
  tz_offset   svarint             # 相对 UTC 的分钟数，范围 [-1440, 1440]

FileChange:
  path        string
  op          u8
  若 op = 0x01（Put）:
    mode      u8                  # 只允许 0x01 / 0x02 / 0x03，不允许 0x04
    id        id                  # kind 必须是 Blob
    size      uvarint
  若 op = 0x02（Delete）:
    （无后续字段）

Extension:
  key         string              # 非空
  value       bytes
```

约束：
- `parents` 中不允许重复 ID。0 个 parent 表示根提交。
- `delta` 按 `path` 的字节序严格升序排列，不允许重复路径；`path` 必须满足 §8 的路径规则。
- `delta` 只描述**相对 first parent 的改动**。目录是隐式的：Put 一个文件会自动创建其上级目录，目录变空时自动消失。「同一个 delta 里路径冲突」（例如同时 Put `a` 和 `a/b`）属于语义错误，由建树阶段报告，不在编码层检查。
- `extensions` 按 `key` 的字节序严格升序排列，不允许重复。未知 key 必须原样保留。
- 保留的 extension key：`git.sha1`，值为 20 字节的 git 提交 sha，由 git 导入写入。服务端据此建立 git sha → 提交 id 的映射，所有接受版本号的接口都可以使用 40 位小写十六进制的 git sha。
- Changeset 的 ID **不包含** root tree。root tree 是派生数据，见服务端设计文档 §4.3。

---

## 6. 解码器必须拒绝的输入

以下任一情况，解码失败，并返回对应的错误码（错误码用于 golden 向量，各语言实现应映射到等价的错误）：

| 错误码 | 情况 |
|---|---|
| `unexpected_eof` | 输入在字段中途结束 |
| `trailing_bytes` | 对象解码完成后仍有剩余字节 |
| `varint_overflow` | varint 超过 10 字节或超出 u64 |
| `varint_non_minimal` | varint 不是最短编码 |
| `length_out_of_range` | `bytes` 长度超过剩余输入 |
| `wrong_kind` | 头部 kind 与期望不符 |
| `unsupported_version` | `format_version` 不是 1 |
| `unknown_algo` / `unknown_kind` | ObjectId 中的 algo / kind 未定义 |
| `unknown_mode` / `unknown_op` | Tree 条目或 FileChange 的 mode / op 未定义；FileChange 中出现 mode `0x04` 也报 `unknown_mode` |
| `invalid_utf8` | `string` 不是合法 UTF-8 |
| `invalid_name` / `invalid_path` | 违反 §8 |
| `not_sorted` | 条目、delta 或 extensions 未严格升序（含重复） |
| `id_kind_mismatch` | id 的 kind 与 mode 或字段要求不符 |
| `nonzero_tree_size` | 子目录条目的 size 不为 0 |
| `duplicate_parent` | parents 中有重复 |
| `invalid_tz_offset` | 时区偏移超出范围 |
| `invalid_extension_key` | extension key 为空 |
| `invalid_chunk_list` | ChunkList 违反 §7 的约束 |

---

## 7. ChunkList（服务端内部）

大于 8 MiB 的文件在服务端按 FastCDC 分块存储，ChunkList 记录分块清单。文件在 Tree 和 Changeset 中**始终使用整个文件的 Blob ID**，ChunkList 只是存储细节，客户端看不到。

```
header        = 0x05 0x01
total_size    uvarint
chunks        list<Chunk>

Chunk:
  id          id                  # kind 必须是 Blob
  size        uvarint             # 必须大于 0
```

约束：至少 1 个分块；所有分块 size 之和等于 `total_size`。

---

## 8. 文件名与路径规则

**文件名**（Tree 条目的 `name`、路径中的每一段）：
- 非空，不超过 255 字节；
- 不能是 `.` 或 `..`；
- 不能包含 `/`（0x2F）或 NUL（0x00）。

**路径**（FileChange 的 `path`）：
- 由一个或多个合法文件名用 `/` 连接而成，不超过 4096 字节；
- 不能以 `/` 开头或结尾，不能有连续的 `/`。

大小写、Unicode 规范化都**不做**处理：字节不同就是不同的名字。

---

## 9. Golden 测试向量

位于 `testdata/vectors/`，JSON 格式：

```jsonc
{
  "valid": [
    {
      "name": "tree_single_file",
      "kind": "tree",                // blob | tree | changeset | chunk_list
      "input": { ... },             // 结构化输入，字段名同本文
      "encoding": "0201...",        // 规范编码（小写十六进制）；blob 为原始内容
      "id": "t1..."                 // 期望的 ObjectId 文本形式
    }
  ],
  "invalid": [
    { "name": "...", "kind": "tree", "encoding": "...", "error": "not_sorted" }
  ]
}
```

结构化输入中：ID 用文本形式，`change_id` 和 extension 的 `value` 用小写十六进制，blob 的内容用十六进制字段 `data_hex`。

一个实现合格的标准：对每个 `valid` 向量，编码结果与 `encoding` 逐字节相同、ID 相同、解码后得到相同结构；对每个 `invalid` 向量，解码失败且错误码一致。

---

## 10. 版本演进

- 新增字段一律通过 `extensions` 承载，不改变 v1 的字节布局。
- 确需改变布局时，提升 `format_version`。新版本的对象拥有不同的字节，因此拥有不同的 ID；解码器按版本号分别处理，旧版本对象永远可读。
