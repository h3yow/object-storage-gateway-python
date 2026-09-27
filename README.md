# 本地对象存储网关（object-storage-gateway）

基于 FastAPI 的本地对象存储网关：桶 / 对象生命周期、分段上传、条件写入、
桶策略鉴权、配额限制、崩溃恢复与**历史版本保留**。全部状态落在本地文件
系统，不依赖任何外部云服务或真实凭据。

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
| PUT | `/buckets/{bucket}/versioning` | 开启/关闭历史版本保留、调整保留上限（仅 owner） | owner |
| DELETE | `/buckets/{bucket}` | 删除空桶；非空返回 `BucketNotEmpty`，并清理桶内未完成上传 | `delete` |

### 对象

| 方法 | 路径 | 语义 | 所需权限 |
|---|---|---|---|
| PUT | `/buckets/{b}/objects/{key}` | 流式写入/覆盖；支持 `?if_absent=true`、`?if_version=N` | `write` |
| GET | `/buckets/{b}/objects/{key}` | 流式读取当前版本（分块返回，不整载入内存）；带 `?version_id=` 读指定历史版本 | `read` |
| HEAD | `/buckets/{b}/objects/{key}` | 元信息（`X-ETag` / `X-Version` / `X-Version-Id`） | `read` |
| GET | `/buckets/{b}/objects` | 列举，`prefix`/`marker`/`limit` 分页，limit 受 `max_list_keys` 钳制 | `list` |
| DELETE | `/buckets/{b}/objects/{key}` | 删除（含全部历史版本）；重复删除返回 `NoSuchKey`（可预期的重试语义） | `delete` |
| GET | `/buckets/{b}/objects/{key}/versions` | 列出该对象全部版本（当前版本在最前，带 `is_current`） | `read` |
| POST | `/buckets/{b}/objects/{key}/versions/{id}/restore` | 将指定版本重新提交为当前版本；支持 `if_absent` / `if_version` | `write` |

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

### 历史版本保留

默认关闭。关闭时行为与旧版完全一致：覆盖写直接替换数据文件，没有
`version_id`，不产生额外空间占用。按桶开启后，**普通覆盖写（PUT）与
分段合并（complete）都不会销毁旧内容**：每次提交生成一个不可变的历史
版本，拥有稳定的 32 位十六进制 `version_id`，可反复引用。

开启方式（二选一）：

```bash
# 建桶时开启，并设定保留上限
curl -X PUT localhost:8000/buckets/demo -H 'X-Identity: alice' \
  -H 'Content-Type: application/json' \
  -d '{"versioning_enabled": true, "max_versions": 10, "max_version_bytes": 104857600}'

# 对已存在的桶开启；现有对象的当前内容自动迁移为该键的第 1 个历史版本
curl -X PUT localhost:8000/buckets/demo/versioning -H 'X-Identity: alice' \
  -H 'Content-Type: application/json' -d '{"enabled": true}'

# 只调整保留上限（立即按新策略淘汰超限的旧版本）
curl -X PUT localhost:8000/buckets/demo/versioning -H 'X-Identity: alice' \
  -H 'Content-Type: application/json' -d '{"enabled": true, "max_versions": 5}'

# 关闭：删除全部历史版本并把当前版本迁回普通布局，空间立即释放
curl -X PUT localhost:8000/buckets/demo/versioning -H 'X-Identity: alice' \
  -H 'Content-Type: application/json' -d '{"enabled": false}'
```

**版本标识与读取语义**：

* 每次成功写入返回体和读响应头都带 `X-Version`（单调递增的整数序号，
  沿用乐观并发控制）与 `X-Version-Id`（稳定标识）。
* `GET .../versions` 返回当前版本及全部历史版本，按时间从新到旧排列，
  每条含 `version_id` / `version` / `size` / `etag` / `is_current`。
* `GET .../objects/{key}?version_id=<id>` 读取指定版本，响应头中的长度、
  etag 与正文严格来自同一个版本快照。
* 从未存在或已被淘汰的 `version_id` 返回 404 `NoSuchVersion`，与对象本身
  不存在的 404 `NoSuchKey` 明确区分。
* **回滚是一次新写入**：`POST .../versions/{id}/restore` 把历史版本的
  内容复制成一个全新的当前版本（新 `version_id`、序号 +1），源历史版本
  保持不变；回滚与普通覆盖写一样走 `if_version` / `if_absent` 条件判断，
  并发安全。历史是只追加的，回滚本身不会修改或删除任何旧版本。

**并发读取自洽**：读取指定版本时，服务端在桶级锁内一次性完成"元数据
快照 + 打开数据文件"，之后正文从该文件描述符流式读出。即使读取期间
别的线程正在覆盖对象或按保留策略淘汰旧版本（POSIX 下已打开的文件被
unlink 后字节仍存活到 fd 关闭），也不会出现头与正文对不上或读到一半
源数据消失。

**保留策略（只约束非当前版本，当前版本永不淘汰）**：

| 参数 | 含义 |
|---|---|
| `max_versions` | 非当前版本数量上限（0 = 不限） |
| `max_version_bytes` | 非当前版本总字节上限（0 = 不限） |

提交新版本后，超出任一时，按 **FIFO 最旧前缀**淘汰：从最旧的历史版本
开始删除，直到剩余历史版本满足全部上限；淘汰只删一个连续的最旧前缀，
不会为保留一个恰好较小的旧版本而删掉更新的版本。被淘汰版本的数据文件
在同一次提交内真正 unlink，配额统计以版本索引为准（`usage_bytes` 与
`OSG_QUOTA_BYTES` 立即反映释放后的占用），不会留下悬挂引用或半清理
状态；崩溃只可能留下可被 `recover()` 清理的孤儿数据。

本地验证示例：

```bash
curl -X PUT localhost:8000/buckets/demo/versioning -H 'X-Identity: alice' \
  -H 'Content-Type: application/json' -d '{"enabled": true}'
curl -X PUT localhost:8000/buckets/demo/objects/a -H 'X-Identity: alice' --data 'v1'
curl -X PUT localhost:8000/buckets/demo/objects/a -H 'X-Identity: alice' --data 'v2'
curl localhost:8000/buckets/demo/objects/a/versions -H 'X-Identity: alice'   # 列版本
# 用返回的旧 version_id 读回 v1
curl 'localhost:8000/buckets/demo/objects/a?version_id=<id>' -H 'X-Identity: alice'
# 把 v1 重新提交为当前版本（产生第 3 个版本）
curl -X POST localhost:8000/buckets/demo/objects/a/versions/<id>/restore -H 'X-Identity: alice'
```

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
| 404 | `NoSuchBucket` / `NoSuchKey` / `NoSuchUpload` / `NoSuchVersion` | 资源不存在（`NoSuchVersion` 特指版本标识从未存在或已被淘汰） |
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
* 版本化桶按"数据 → 版本索引 → 对象指针"顺序提交；开启/关闭历史保留时的
  布局迁移幂等，崩溃后重启自动补完。
* 启动时 `recover()` 清理孤儿数据、残留 tmp 与无主体的分片目录，报告可
  在 `GET /healthz` 查看；进行中的分段上传跨重启保留并可续传。
* 所有变更在全局锁（桶注册表 + 配额记账）与桶级锁内串行执行。版本读取在
  桶级锁内同时拿到元数据快照与打开的文件描述符，之后无锁流式读取。

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
  models.py      可 JSON 序列化的元数据模型（含 VersionRecord）
  auth.py        身份解析与桶策略判定
  store.py       文件系统持久化：原子提交、锁、配额、恢复、版本布局与淘汰
  streaming.py   异步请求流 → 同步分块迭代器（有界内存）
  service.py     业务编排：先鉴权后操作
main.py          FastAPI 路由层（create_app 工厂）
tests/           并发、分段重传、越权、中断恢复、配额、流式、版本历史测试
```
