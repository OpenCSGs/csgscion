-- PathHistory 索引：每个提交改动的路径（相对第一个父提交）。
CREATE TABLE path_changes (
    repo_id    INTEGER NOT NULL REFERENCES repos (repo_id),
    path       TEXT    NOT NULL,
    cs_id      BLOB    NOT NULL,
    generation INTEGER NOT NULL,
    deleted    INTEGER NOT NULL,
    PRIMARY KEY (repo_id, path, cs_id)
) WITHOUT ROWID;
CREATE INDEX path_changes_by_generation ON path_changes (repo_id, path, generation);

-- 按代数批量预取提交记录。
CREATE INDEX changesets_by_generation ON changesets (repo_id, generation);
