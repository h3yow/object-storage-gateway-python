# 本地对象存储网关（object-storage-gateway）

基于 FastAPI 的本地对象存储网关：桶 / 对象生命周期、分段上传、条件写入、
桶策略鉴权、配额限制与崩溃恢复。全部状态落在本地文件系统，不依赖任何外部
云服务或真实凭据。

## 快速开始

```bash
uv sync                      # 安装依赖
uv run uvicorn main:app      # 启动服务（默认 127.0.0.1:8000）
uv run pytest -q -s          # 运行测试（-s 可看到每个用例的输入与判定依据）
```

本地验证示例：

```bash
curl -X PUT localhost:8000/buckets/demo -H 'X-Identity: alice'
curl -X PUT localhost:8000/buckets/demo/objects/a.txt -H 'X-Identity: alice' --data 'hello'
curl localhost:8000/buckets/demo/objects/a.txt -H 'X-Identity: alice'
```

## 接口语义

身份通过请求头 `X-Identity` 传递（本地身份占位，无外部 IdP）。缺失该头一律
返回 `AccessDenied`。

### 桶

| 方法 | 路径 | 语义 | 所需权限 |
|---|---|---|---|
| PUT | `/buckets/{bucket}` | 创建桶（body 可带 `{"policy": {...}}`），重复创建返回 `BucketAlreadyExists` | 任何已认证身份 |
| GET | `/buckets` | 列出调用者可见的桶 | 已认证 |
| GET | `/buckets/{bucket}` | 桶元信息 | `read` |
| PUT | `/buckets/{bucket}/policy` | 覆盖桶策略（仅 owner） | owner |
| DELETE | `/buckets/{bucket}` | 删除空桶；非空返回 `BucketNotEmpty`，并清理桶内未完成上传 | `delete` |

### 对象

| 方法 | 路径 | 语义 | 所需权限 |
|---|---|---|---|
| PUT | `/buckets/{b}/objects/{key}` | 流式写入/覆盖；支持 `?if_absent=true`、`?if_version=N` | `write` |
| GET | `/buckets/{b}/objects/{key}` | 流式读取（分块返回，不整载入内存） | `read` |
| HEAD | `/buckets/{b}/objects/{key}` | 元信息（`X-ETag` / `X-Version`） | `read` |
| GET | `/buckets/{b}/objects` | 列举，`prefix`/`marker`/`limit` 分页，limit 受 `max_list_keys` 钳制 | `list` |
| DELETE | `/buckets/{b}/objects/{key}` | 删除；重复删除返回 `NoSuchKey`（可预期的重试语义） | `delete` |

**条件写入**：`if_absent` 在键已存在时返回 409 `Conflict`；`if_version=N`
在当前版本不等于 N 时返回 412 `PreconditionFailed`。判定与提交在同一把桶级
锁内完成，同一键的并发写被串行化，不会出现丢失更新或半更新。乐观并发控制
的客户端应读 `X-Version` → 修改 → 带 `if_version` 写回，412 时重试。

### 分段上传

| 方法 | 路径 | 语义 |
|---|---|---|
| POST | `/buckets/{b}/uploads?key=K` | 创建上传，返回 `upload_id` |
| GET | `/buckets/{b}/uploads` | 列出进行中的上传 |
| PUT | `/buckets/{b}/uploads/{id}/parts/{n}` | 上传分片 n（1..10000），可乱序、可重传，重传同号分片为替换而非累加 |
| POST | `/buckets/{b}/uploads/{id}/complete` | 按分片号升序合并并原子提交；支持 `if_absent` / `if_version` |
| DELETE | `/buckets/{b}/uploads/{id}` | 中止上传，清理全部分片 |

未完成的上传对外不可见；合并成功前目标对象保持旧值。`complete` 是幂等
安全的：重试会得到 `NoSuchUpload`，对象内容不会重复合并或重复计数。

## 错误分类（稳定契约）

所有错误返回统一信封：

```json
{"error": {"code": "NoSuchKey", "message": "...", "detail": {}}}
```

| HTTP | code | 场景 |
|---|---|---|
| 400 | `InvalidRequest` | 非法参数（未知策略动作、非法桶名等） |
| 400 | `InvalidPart` | 分片号越界、空上传合并 |
| 403 | `AccessDenied` | 未认证或越权（不泄露资源是否存在） |
| 403 | `QuotaExceeded` | 容量配额超限（拒绝后无残留） |
| 404 | `NoSuchBucket` / `NoSuchKey` / `NoSuchUpload` | 资源不存在 |
| 409 | `BucketAlreadyExists` / `BucketNotEmpty` / `Conflict` | 状态冲突 |
| 412 | `PreconditionFailed` | `if_version` 不匹配（落后的写入被拒绝） |
| 413 | `EntityTooLarge` | 超过 `max_object_size` |
| 429 | `TooManyRequests` | 活跃上传数超限 |

客户端超时后重试是安全的：覆盖写与分片重传是替换语义，删除/合并的重复
执行得到可预期的 404，副作用不会放大。

## 权限规则

* 桶策略：`{"read": [...], "write": [...], "list": [...], "delete": [...]}`，
  身份列表中 `"*"` 表示任意身份；桶 owner 永远拥有全部权限。
* 读、写、列举、删除分别校验，互不含蕴。
* 未授权调用一律得到 403 `AccessDenied`，无论目标对象是否存在——鉴权失败
  不泄露对象存在性。桶级 GET 对不存在的桶返回 404（桶名存在性不属于
  机密；桶内对象的存在性受保护）。

## 一致性与恢复

* 元数据写入均为 tmp 文件 + fsync + 原子 rename。
* 对象数据先于元数据提交；删除时先删元数据。崩溃只可能留下"无元数据的
  孤儿数据"，绝不会出现"有元数据无数据"。
* 启动时 `recover()` 清理孤儿数据、残留 tmp 与无主体的分片目录，报告可
  在 `GET /healthz` 查看；进行中的分段上传跨重启保留并可续传。
* 所有变更在全局锁（桶注册表 + 配额记账）与桶级锁内串行执行。

## 配置项（环境变量）

| 变量 | 默认 | 含义 |
|---|---|---|
| `OSG_DATA_DIR` | `./osg-data` | 数据根目录 |
| `OSG_MAX_OBJECT_SIZE` | 64 MiB | 单对象大小上限，超限 413 |
| `OSG_MAX_LIST_KEYS` | 1000 | 单次列举上限（服务端钳制） |
| `OSG_QUOTA_BYTES` | 0（不限） | 总容量配额（对象 + 在途分片），超限 403 |
| `OSG_MAX_ACTIVE_UPLOADS` | 0（不限） | 单桶活跃上传上限，超限 429 |
| `OSG_STREAM_CHUNK_SIZE` | 1 MiB | 读写流式分块大小（内存上界） |
| `OSG_MIN_PART_SIZE` | 5 MiB | 分片大小下限（本地测试可调小） |

## 代码结构

```
gateway/
  errors.py      稳定错误分类（code/status/信封）
  config.py      配置与环境变量
  models.py      可 JSON 序列化的元数据模型
  auth.py        身份解析与桶策略判定
  store.py       文件系统持久化：原子提交、锁、配额、恢复
  streaming.py   异步请求流 → 同步分块迭代器（有界内存）
  service.py     业务编排：先鉴权后操作
main.py          FastAPI 路由层（create_app 工厂）
tests/           并发、分段重传、越权、中断恢复、配额、流式测试
```
