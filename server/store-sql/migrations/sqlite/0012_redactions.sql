-- redaction（服务端设计文档 §9）：按 (仓库, 对象) 一条记录，撤销后保留（active = 0）。
-- seq 每次修改递增（全局），缓存层按它增量同步。
CREATE TABLE redactions (
    repo_id        INTEGER NOT NULL REFERENCES repos (repo_id),
    object_id      BLOB    NOT NULL,
    redaction_id   TEXT    NOT NULL,
    reason         TEXT    NOT NULL,
    created_by     TEXT    NOT NULL,
    created_at_ms  INTEGER NOT NULL,
    active         INTEGER NOT NULL,
    purge_after_ms INTEGER,
    purged_at_ms   INTEGER,
    seq            INTEGER NOT NULL,
    PRIMARY KEY (repo_id, object_id)
) WITHOUT ROWID;
CREATE UNIQUE INDEX redactions_seq ON redactions (seq);
CREATE INDEX redactions_redaction_id ON redactions (redaction_id);

-- seq 计数器（单行）。写入事务先更新这一行，行锁让写入按 seq 顺序提交，增量同步不会漏掉记录。
CREATE TABLE redaction_seq (
    id  INTEGER PRIMARY KEY CHECK (id = 1),
    seq INTEGER NOT NULL
);
INSERT INTO redaction_seq (id, seq) VALUES (1, 0);
