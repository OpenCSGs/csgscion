-- 不透明令牌（PAT / service token / 刷新令牌）与全局设置（访问控制策略等），服务端设计文档 §3.7、§9。
-- 令牌本身不保存，只保存 SHA-256。
CREATE TABLE tokens (
    id            TEXT    PRIMARY KEY,
    hash          BLOB    NOT NULL UNIQUE,
    kind          TEXT    NOT NULL,
    subject       TEXT    NOT NULL,
    groups_text   TEXT    NOT NULL,
    description   TEXT    NOT NULL,
    created_at_ms INTEGER NOT NULL,
    expires_at_ms INTEGER
) WITHOUT ROWID;
CREATE INDEX tokens_subject ON tokens (subject);

CREATE TABLE settings (
    key   TEXT PRIMARY KEY,
    value BLOB NOT NULL
) WITHOUT ROWID;
