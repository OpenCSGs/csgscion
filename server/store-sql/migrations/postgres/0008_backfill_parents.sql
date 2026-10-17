-- M1 写入的提交只在 first_parent 列记录父提交；0004 新增的 parents 列默认为空，而读取只用 parents，
-- 不修复的话这些提交都会被当成根提交。
-- 1. 这些仓库的跳表可能已按"根提交"算错（修复前跑过 reindex，或在旧提交上新建了提交），先全部标记为未知；
-- 2. 把 first_parent 拷入 parents。
-- 合并提交的其余父提交和正确的跳表由 `scion admin reindex` 从提交对象补全。
UPDATE changesets SET fp_depth = 0, fp_jump = NULL
WHERE repo_id IN (
    SELECT DISTINCT repo_id FROM changesets WHERE octet_length(parents) = 0 AND first_parent IS NOT NULL
);
UPDATE changesets SET parents = first_parent WHERE octet_length(parents) = 0 AND first_parent IS NOT NULL;
