-- 已创建的 changeset 及其根目录树（cs_tree）。changeset 字节本身作为对象存在 BlobStore。
CREATE TABLE changesets (
    repo_id      INTEGER NOT NULL REFERENCES repos (repo_id),
    cs_id        BLOB    NOT NULL,
    root_tree    BLOB    NOT NULL,
    first_parent BLOB,
    generation   INTEGER NOT NULL,
    PRIMARY KEY (repo_id, cs_id)
) WITHOUT ROWID;
