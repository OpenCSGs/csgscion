-- 第一父提交链的深度与跳跃指针（skew-binary 跳表）；fp_depth = 0 表示未知（之前写入的数据）。
ALTER TABLE changesets ADD COLUMN fp_depth INTEGER NOT NULL DEFAULT 0;
ALTER TABLE changesets ADD COLUMN fp_jump BLOB;
