-- 合入过 ref 的 change：同一 ref 上每个 change id 只合入一次（服务端设计文档 §4.4.1、§6.2）。
CREATE TABLE landed (
    repo_id      INTEGER NOT NULL REFERENCES repos (repo_id),
    ref          TEXT    NOT NULL,
    change_id    BLOB    NOT NULL,
    changeset    BLOB    NOT NULL,
    landed       BLOB    NOT NULL,
    ref_version  INTEGER NOT NULL,
    landed_at_ms INTEGER NOT NULL,
    PRIMARY KEY (repo_id, ref, change_id)
) WITHOUT ROWID;

-- 写请求的结果，按 request id 保存一段时间（默认 24 小时），重试时原样返回（§3.6）。
CREATE TABLE idempotency (
    repo_id       INTEGER NOT NULL REFERENCES repos (repo_id),
    request_id    TEXT    NOT NULL,
    fingerprint   BLOB    NOT NULL,
    response      BLOB    NOT NULL,
    created_at_ms INTEGER NOT NULL,
    PRIMARY KEY (repo_id, request_id)
) WITHOUT ROWID;
CREATE INDEX idempotency_created ON idempotency (created_at_ms);
