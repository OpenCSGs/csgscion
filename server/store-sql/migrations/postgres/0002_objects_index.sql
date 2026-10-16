-- 仓库能看到哪些对象。对象字节在 BlobStore 中全局只存一份（跨仓库去重）。
CREATE TABLE objects_index (
    repo_id BIGINT NOT NULL REFERENCES repos (repo_id),
    id      BYTEA  NOT NULL,
    size    BIGINT NOT NULL,
    PRIMARY KEY (repo_id, id)
);
