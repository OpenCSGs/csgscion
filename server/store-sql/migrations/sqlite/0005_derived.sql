-- 派生数据（服务端设计文档 §8）：按 (种类, 算法版本, 键) 存储。
CREATE TABLE derived (
    repo_id INTEGER NOT NULL REFERENCES repos (repo_id),
    kind    TEXT    NOT NULL,
    version INTEGER NOT NULL,
    key     BLOB    NOT NULL,
    value   BLOB    NOT NULL,
    PRIMARY KEY (repo_id, kind, version, key)
) WITHOUT ROWID;
