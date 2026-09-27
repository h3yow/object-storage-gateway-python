# 本地对象存储网关（object-storage-gateway）

基于 FastAPI 的本地对象存储网关：桶 / 对象生命周期、分段上传、条件写入、
桶策略鉴权、配额限制、崩溃恢复，以及**桶级历史版本保留**（覆盖写与分段
合并后的旧内容可按稳定标识查回、列举与回滚）。全部状态落在本地文件系统，
不依赖任何外部云服务或真实凭据。

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
| PUT | `/buckets/{bucket}` | 创建桶（body 可带 `{"policy": {...}}` 或版本保留配置），重复创建返回 `BucketAlreadyExists` | 任何已认证身份 |
| GET | `/buckets` | 列出调用者可见的桶 | 已认证 |
| GET | `/buckets/{bucket}` | 桶元信息 | `read` |
| PUT | `/buckets/{bucket}/policy` | 覆盖桶策略（仅 owner） | owner |
| PUT | `/buckets/{bucket}/versioning` | 开启/关闭历史保留、调整保留上限（仅 owner） | owner |
| DELETE | `/buckets/{bucket}` | 删除空桶；非空返回 `BucketNotEmpty`，并清理桶内未完成上传 | `delete` |

### 对象

| 方法 | 路径 | 语义 | 所需权限 |
|---|---|---|---|
| PUT | `/buckets/{b}/objects/{key}` | 流式写入/覆盖；支持 `?if_absent=true`、`?if_version=N` | `write` |
| GET | `/buckets/{b}/objects/{key}` | 流式读取（分块返回，不整载入内存），响应头含 `X-Version-Id` | `read` |
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

### 历史版本保留

**开启方式**（默认关闭，关闭时所有行为与未启用时完全一致）：

```bash
# 建桶时直接开启，并给每个键保留至多 3 个版本
curl -X PUT localhost:8000/buckets/demo -H 'X-Identity: alice' \
  -H 'Content-Type: application/json' \
  -d '{"versioning_enabled": true, "max_versions": 3}'
# 或对已有桶开启 / 调整 / 关闭（仅 owner；字段缺省表示该项不变）
curl -X PUT localhost:8000/buckets/demo/versioning -H 'X-Identity: alice' \
  -H 'Content-Type: application/json' \
  -d '{"enabled": true, "max_versions": 5, "max_version_bytes": 10485760}'
```

开启之后，同一个键无论是普通 `PUT` 覆盖还是分段上传 `complete` 合并，旧内容
都会作为**不可变历史版本**保留：

| 方法 | 路径 | 语义 | 所需权限 |
|---|---|---|---|
| GET | `/buckets/{b}/versions/{key}` | 列出该键全部版本（含当前版本），最新在前；每条含 `version`、`version_id`、`size`、`etag`、`is_current` | `read` |
| GET | `/buckets/{b}/version/{version_id}/{key}` | 按稳定标识流式读取某一版；响应头 `X-Version-Id` / `X-ETag` / `Content-Length` 与正文属于同一版 | `read` |
| POST | `/buckets/{b}/version/{version_id}/{key}/rollback` | 把历史版本的内容**作为一次新写入**重新提交为当前版本；支持 `?if_version=N`、`?if_absent=true` | `write` |

语义要点：

* **版本标识**：每次提交产生一个随机、全局唯一的 `version_id`（32 位十六
  进制），一旦产生永不改变、可再次引用；`version` 是键内从 1 递增的序号。
  开启保留之前就存在的旧对象（遗留数据）会在开启后的第一次覆盖时自动迁入
  版本存储，成为版本 1；在未开启保留的桶里该对象的 `version_id` 为空。
* **并发自洽的读取**：读取某一版时，版本元数据与数据文件在同一把桶级锁内
  定位并打开，之后正文以固定 inode 流式返回（POSIX 下 rename/unlink 不影响
  已打开的句柄）。因此即使读取过程中该键正被覆盖、或该版本正被淘汰，返回
  的长度、`etag` 与正文也必然来自同一版本；版本恰好在打开瞬间被淘汰时，得
  到明确的 404 `NoSuchVersion`，绝不会读到半截或对不上的内容。
* **回滚是新写入**：回滚不修改、不删除任何历史版本——源版本正文以硬链接
  （不支持硬链接的文件系统上退化为拷贝）复制成一个全新的当前版本，版本号
  +1，并照常走 `if_version` / `if_absent` 条件判断与桶锁串行规则。412 时
  历史版本保持不动。
* **关闭保留**：已有历史版本继续可读（"冻结的历史"）；关闭后的新覆盖不再
  保留旧内容。删除对象会连同冻结历史一起释放空间。
* **删除对象**：显式 `DELETE` 会删除该键当前版本与全部历史版本（版本保留
  不是删除保护）；查询一个已删键的版本返回 `NoSuchKey`。
* **不存在的版本**：标识从未存在、已被淘汰，查询一律返回 404
  `NoSuchVersion`，与键不存在的 404 `NoSuchKey` 可区分。

**保留与淘汰策略**（按桶配置，对每个键独立生效；0 = 不限）：

* `max_versions=N`：每个键至多保留 N 个版本（含当前版本）。
* `max_version_bytes=B`：每个键**历史版本**（不含当前版本）占用字节合计上
  限；当前版本永远不计入淘汰、永远不会被淘汰。
* 超限时在每次新提交的同一把锁内，按版本号从旧到新淘汰，直到同时满足两条
  上限。淘汰先删版本元数据再删数据文件，空间立即释放，计入既有
  `OSG_QUOTA_BYTES` 配额记账（保留的每个版本按一份正文计空间，当前版本也
  只计一次）。崩溃只可能留下待 `recover()` 回收的孤儿数据，不会留下悬挂
  引用或"元数据在、正文没了"的半清理状态。`max_versions=1` 会被拒绝（等于
  不保留历史，请直接关闭保留或传 ≥ 2）。

本地验证：

```bash
B=http://localhost:8000/buckets/demo
curl -X PUT $B -H 'X-Identity: alice' -H 'Content-Type: application/json' \
  -d '{"versioning_enabled": true, "max_versions": 3}'
curl -X PUT $B/objects/k -H 'X-Identity: alice' --data v1
curl -X PUT $B/objects/k -H 'X-Identity: alice' --data v2-longer
curl -X PUT $B/objects/k -H 'X-Identity: alice' --data v3
curl $B/versions/k -H 'X-Identity: alice'            # 列出全部版本
VID=...                                              # 取某条 version_id
curl $B/version/$VID/k -H 'X-Identity: alice'        # 读取该版正文
curl -X POST $B/version/$VID/k/rollback -H 'X-Identity: alice'
curl $B/objects/k -H 'X-Identity: alice'             # 现在又是旧内容，版本号 +1
uv run pytest tests/test_versioning.py -q -s         # 版本相关单测
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
| 404 | `NoSuchBucket` / `NoSuchKey` / `NoSuchUpload` / `NoSuchVersion` | 资源不存在（版本可能从未存在或已被淘汰） |
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
* 开启保留的版本同样遵守上述次序：版本数据先落盘（immutable，文件名即
  version_id），再原子提交版本元数据与当前对象元数据；淘汰先删版本元数据。
  旧内容迁入版本存储期间崩溃，恢复时会清掉遗留的旧路径数据。
* 启动时 `recover()` 清理孤儿数据、残留 tmp 与无主体的分片目录，报告可
  在 `GET /healthz` 查看；进行中的分段上传跨重启保留并可续传。版本存储中
  无当前对象的版本目录、无 VersionMeta 的版本数据、缺数据文件的
  VersionMeta 都会被回收，且当前存活对象的版本不受影响。
* 所有变更在全局锁（桶注册表 + 配额记账）与桶级锁内串行执行。锁顺序固定
  为"全局锁 → 桶锁"；版本读取只在桶锁内完成"定位元数据 + 打开数据文件"
  这一快照动作，之后的正文流式传输不持锁、不受覆盖与淘汰影响。

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
  models.py      可 JSON 序列化的元数据模型（含 VersionMeta、桶级保留配置）
  auth.py        身份解析与桶策略判定
  store.py       文件系统持久化：原子提交、锁、配额、版本归档/淘汰/恢复
  streaming.py   异步请求流 → 同步分块迭代器（有界内存）
  service.py     业务编排：先鉴权后操作
main.py          FastAPI 路由层（create_app 工厂）
tests/           并发、分段重传、越权、中断恢复、配额、流式、版本保留测试
```
