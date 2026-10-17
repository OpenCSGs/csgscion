-- 租约：合入 leader（每个 ref 一个）等（服务端设计文档 §4.4.1、§6.2）。
CREATE TABLE leases (
    scope         TEXT    PRIMARY KEY,
    holder        TEXT    NOT NULL,
    token         INTEGER NOT NULL,
    expires_at_ms INTEGER NOT NULL
) WITHOUT ROWID;
