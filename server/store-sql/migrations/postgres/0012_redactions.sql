-- redaction（服务端设计文档 §9）：按 (仓库, 对象) 一条记录，撤销后保留（active = false）。
-- seq 每次修改递增（全局），缓存层按它增量同步。
CREATE TABLE redactions (
    repo_id        BIGINT  NOT NULL REFERENCES repos (repo_id),
    object_id      BYTEA   NOT NULL,
    redaction_id   TEXT    NOT NULL,
    reason         TEXT    NOT NULL,
    created_by     TEXT    NOT NULL,
    created_at_ms  BIGINT  NOT NULL,
    active         BOOLEAN NOT NULL,
    purge_after_ms BIGINT,
    purged_at_ms   BIGINT,
    seq            BIGINT  NOT NULL,
    PRIMARY KEY (repo_id, object_id)
);
CREATE UNIQUE INDEX redactions_seq ON redactions (seq);
CREATE INDEX redactions_redaction_id ON redactions (redaction_id);

-- seq 计数器（单行）。不用 SEQUENCE：序列值的分配顺序与提交顺序无关，增量同步可能漏掉晚提交的
-- 小序号。写入事务先更新这一行，行锁让写入按 seq 顺序提交。
CREATE TABLE redaction_seq (
    id  INTEGER PRIMARY KEY CHECK (id = 1),
    seq BIGINT  NOT NULL
);
INSERT INTO redaction_seq (id, seq) VALUES (1, 0);
