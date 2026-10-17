-- 全部父提交（每个 34 字节 ObjectId 依次拼接），用于 log、祖先判断与 merge-base。
ALTER TABLE changesets ADD COLUMN parents BYTEA NOT NULL DEFAULT '\x'::bytea;
