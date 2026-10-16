-- PathHistory 索引：每个提交改动的路径（相对第一个父提交）。
-- COLLATE "C"：按字节比较，前缀区间查询不受数据库 locale 影响。
CREATE TABLE path_changes (
    repo_id    BIGINT  NOT NULL REFERENCES repos (repo_id),
    path       TEXT    COLLATE "C" NOT NULL,
    cs_id      BYTEA   NOT NULL,
    generation BIGINT  NOT NULL,
    deleted    BOOLEAN NOT NULL,
    PRIMARY KEY (repo_id, path, cs_id)
);
CREATE INDEX path_changes_by_generation ON path_changes (repo_id, path, generation);

-- 按代数批量预取提交记录。
CREATE INDEX changesets_by_generation ON changesets (repo_id, generation);
