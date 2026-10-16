-- Scion 元数据（SQLite / Lite 档）。表结构与 PostgreSQL 版保持一致，见服务端设计文档 §4.4.1。

CREATE TABLE repos (
    repo_id       INTEGER PRIMARY KEY AUTOINCREMENT,
    name          TEXT    NOT NULL UNIQUE,
    created_at_ms INTEGER NOT NULL
);

-- 唯一的可变状态：只能通过带版本条件的 UPDATE / INSERT 修改。
CREATE TABLE refs (
    repo_id INTEGER NOT NULL REFERENCES repos (repo_id),
    name    TEXT    NOT NULL,
    target  BLOB    NOT NULL,
    version INTEGER NOT NULL,
    PRIMARY KEY (repo_id, name)
);

-- 审计记录：每次成功的 CAS 追加一行。
CREATE TABLE ref_log (
    repo_id       INTEGER NOT NULL,
    name          TEXT    NOT NULL,
    version       INTEGER NOT NULL,
    target        BLOB    NOT NULL,
    updated_at_ms INTEGER NOT NULL,
    PRIMARY KEY (repo_id, name, version)
);
