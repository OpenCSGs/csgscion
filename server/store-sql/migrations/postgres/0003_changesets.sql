-- 已创建的 changeset 及其根目录树（cs_tree）。changeset 字节本身作为对象存在 BlobStore。
CREATE TABLE changesets (
    repo_id      BIGINT NOT NULL REFERENCES repos (repo_id),
    cs_id        BYTEA  NOT NULL,
    root_tree    BYTEA  NOT NULL,
    first_parent BYTEA,
    generation   BIGINT NOT NULL,
    PRIMARY KEY (repo_id, cs_id)
);
