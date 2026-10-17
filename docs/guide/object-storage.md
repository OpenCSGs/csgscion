# 对象存储：SeaweedFS、MinIO 与其他 S3 兼容服务

Scion 服务端把文件内容、目录树和提交对象存放在对象存储中（`--blobs`）。可选后端：

| `--blobs` | 用途 |
|---|---|
| `mem` | 测试，进程退出即丢失 |
| 目录路径（或 `--lite --data-dir`） | 单机 Lite 档 |
| `s3://bucket[/prefix]` | 生产：SeaweedFS、MinIO、AWS S3，或其他兼容 S3 的服务 |

数据读写只用 S3 核心子集（PUT / GET / HEAD / LIST），不依赖条件写或版本控制。SeaweedFS 和 MinIO 每次 CI 都会跑完整的存储一致性测试。

## 连接参数

| 环境变量 | 说明 | 默认 |
|---|---|---|
| `SCION_S3_ENDPOINT` | 服务地址，如 `http://minio:9000`。访问 AWS S3 时留空 | 无 |
| `SCION_S3_PROVIDER` | `auto`、`minio`、`seaweedfs`、`aws`、`generic` | `auto` |
| `SCION_S3_ACCESS_KEY_ID` / `SCION_S3_SECRET_ACCESS_KEY` | 访问密钥；未设置时读 `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` | 无 |
| `SCION_S3_REGION` | 区域；未设置时读 `AWS_REGION` | `us-east-1` |
| `SCION_S3_VIRTUAL_HOSTED` | `true` 时用 `bucket.host` 风格 | 按服务决定，只有 AWS 默认为 `true` |
| `SCION_S3_MAX_RETRIES` | 单个请求的最大重试次数 | 10 |

`s3://` 后面的 `/prefix` 用来让多个部署共用一个 bucket。

## 服务适配

启动时服务端会先检查对象存储，任何一步失败都会拒绝启动，并说明原因：

1. **识别服务**：读 endpoint 根路径返回的 `Server` 响应头，`MinIO` 识别为 MinIO，`SeaweedFS …` 识别为 SeaweedFS，`AmazonS3` 识别为 AWS。
   - `SCION_S3_PROVIDER=auto` 时直接采用识别结果。
   - 显式配置的服务与识别结果不一致时报错，多半是 endpoint 写错了。
   - 前面有反向代理、识别不出时，以配置为准。
2. **健康检查**：MinIO 检查 `/minio/health/ready`，SeaweedFS 检查 `/healthz`。服务还没就绪时，报错中会带上返回的状态码。
3. **bucket 检查**：bucket 不存在时，报错中给出该服务的创建方法。
4. **读写自检**：写入、读回并校验一个探测对象。用于发现密钥错误、权限不足等问题。

启动日志会记录识别结果，例如：

```
object store ready backend=s3 (minio, Server: MinIO) bucket=scion-objects
```

| | SeaweedFS | MinIO |
|---|---|---|
| 识别依据（`Server` 响应头） | `SeaweedFS <版本>` | `MinIO` |
| 健康检查 | `/healthz` | `/minio/health/ready` |
| 创建 bucket | `echo 's3.bucket.create -name B' \| weed shell` | `mc mb <alias>/B`，或容器环境变量 `MINIO_DEFAULT_BUCKETS=B` |
| 请求风格 | path | path |
| 已测试版本 | 4.48 | 2023.12.23（bitnami 镜像，与 starhub / CSGHub 部署一致） |

## SeaweedFS

开发环境默认使用 SeaweedFS。MinIO 开源版已归档、停止维护，SeaweedFS 是仍在维护的 Apache 2.0 项目。

```sh
docker compose -f deploy/dev/docker-compose.yml up -d s3 s3-init
SCION_S3_ENDPOINT=http://localhost:8333 \
SCION_S3_ACCESS_KEY_ID=scion SCION_S3_SECRET_ACCESS_KEY=scion-dev-secret \
SCION_S3_PROVIDER=seaweedfs \
scion server --meta postgres://scion:scion-dev-secret@localhost:5432/scion --blobs s3://scion-objects
```

## MinIO

接入 starhub / CSGHub 时，可以直接使用它们现有的 MinIO：

```sh
docker compose -f deploy/dev/docker-compose.yml --profile minio up -d minio
SCION_S3_ENDPOINT=http://localhost:9000 \
SCION_S3_ACCESS_KEY_ID=scion SCION_S3_SECRET_ACCESS_KEY=scion-dev-secret \
SCION_S3_PROVIDER=minio \
scion server --meta postgres://scion:scion-dev-secret@localhost:5432/scion --blobs s3://scion-objects/scion
```

与其他服务共用 MinIO 时，建议给 Scion 单独的 bucket 或前缀，并只授予该范围的读写权限。

## 代理

请求遵循系统代理设置（`HTTP_PROXY` / `HTTPS_PROXY` / `NO_PROXY`）。对象存储在内网或本机时，记得把它的地址加入 `NO_PROXY`，例如 `NO_PROXY=127.0.0.1,localhost,minio`。

## 测试

对象存储测试通过环境变量指向真实服务，未设置时跳过：

```sh
SCION_TEST_S3_ENDPOINT=http://127.0.0.1:9000 SCION_TEST_S3_BUCKET=scion-test \
SCION_TEST_S3_ACCESS_KEY=scion SCION_TEST_S3_SECRET_KEY=scion-dev-secret \
SCION_TEST_S3_PROVIDER=minio \
cargo test -p scion-store-blob -p scion
```

`SCION_TEST_S3_PROVIDER` 用来校验识别结果，可以不设置。
