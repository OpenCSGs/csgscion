-- 派生数据（服务端设计文档 §8）：按 (种类, 算法版本, 键) 存储。
CREATE TABLE derived (
    repo_id BIGINT  NOT NULL REFERENCES repos (repo_id),
    kind    TEXT    NOT NULL,
    version INTEGER NOT NULL,
    key     BYTEA   NOT NULL,
    value   BYTEA   NOT NULL,
    PRIMARY KEY (repo_id, kind, version, key)
);
