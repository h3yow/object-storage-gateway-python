# Local Object Storage Gateway

一个纯本地、零外部依赖的对象存储网关（FastAPI 实现）。提供桶 / 对象生命周期、
分段上传、条件写入、基于桶策略的鉴权、容量与并发配额，以及进程重启后的崩溃恢复。
所有数据落在本地文件系统，**不依赖任何云服务、真实凭据或付费资源**。

## 1. 运行

```bash
uv sync
# 启动（默认数据目录 ./.osg-data）
uv run uvicorn main:app --reload
# 交互式文档： http://127.0.0.1:8000/docs
```

指定数据目录：

```bash
OSG_DATA_DIR=/var/lib/osg uv run uvicorn main:app --host 127.0.0.1 --port 8000
```

## 2. 身份模型

每个请求必须带头 `X-OSG-Principal: <name>`（本地网关约定的简单身份头，仅用于
演示与本地验证，不构成真实认证体系）。缺失或非法时返回 `401 Unauthorized`。
桶的创建者即 **owner**，对桶内一切操作隐式放行，且只有 owner 能读写桶策略。

## 3. HTTP 接口

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| `GET` | `/health` | 健康检查 |
| `GET` | `/v1/buckets` | 列出当前身份拥有的桶 |
| `PUT` | `/v1/buckets/{bucket}` | 创建桶；JSON 体可带 `policy` 与 `quotas` |
| `DELETE` | `/v1/buckets/{bucket}` | 删除空桶（非空返回 409） |
| `GET/PUT` | `/v1/buckets/{bucket}/policy` | owner 读取 / 设置桶策略 |
| `GET` | `/v1/buckets/{bucket}/objects?prefix=&delimiter=&marker=&max-keys=` | 列举对象（分页 + 分组） |
| `PUT` | `/v1/buckets/{bucket}/objects/{key}` | 整体写入 / 覆盖对象（流式） |
| `GET/HEAD` | `/v1/buckets/{bucket}/objects/{key}` | 读取元数据 / 流式下载对象 |
| `DELETE` | `/v1/buckets/{bucket}/objects/{key}` | 删除对象（幂等） |
| `POST` | `.../{key}?uploads` | 初始化分段上传 |
| `PUT` | `.../{key}/parts/{n}?uploadId=...` | 上传第 n 段（可乱序、可重传） |
| `POST` | `.../{key}?uploadId=...&action=parts` | 列举已上传分段 |
| `POST` | `.../{key}?uploadId=...` | 合并分段（JSON 体携带有序 manifest） |
| `POST` | `.../{key}?uploadId=...&action=abort` | 中止分段上传（幂等安全） |

### 3.1 整体写入与条件头

* `If-None-Match: *`：仅当对象不存在时写入，否则 `412 PreconditionFailed`。
* `If-Match: "<etag>"`：仅当当前版本 etag 匹配时写入（CAS）；落后的写入被
  明确拒绝，不会发生丢失更新、半更新或读到中间状态。
* 响应头返回 `ETag` 与 `X-OSG-Version-Id`；每次覆盖生成新版本，旧版本在元数据
  提交后才被回收。

### 3.2 分段上传语义

* 分片可以**乱序到达**，服务端按 part number 存储；合并时必须按号严格递增，
  否则 `400 InvalidPartOrder`。
* 同一 part number 用**相同内容**重传是幂等的（不重复计数、返回同一 etag）；
  用不同内容重传返回 `409 Conflict`，已存分片不受影响。
* 合并请求的 manifest 中 etag 与服务端不符返回 `400 InvalidPart`；未完成的
  上传**绝不**作为完整对象可见。
* 合并是 exactly-once：客户端超时后用同一 `uploadId` 重试合并，返回**同一个
  version_id**，不会产生第二个对象。
* 除最后一段外，每段不得小于 `OSG_MIN_PART_SIZE`（默认 5 MiB）。

### 3.3 幂等与可重试性

* `DELETE` 对象：对象存在返回 `{"existed": true}`，重复删除返回
  `{"existed": false}`，都是 200，不放大副作用。
* 分段合并 / 中止 / 分片重传均可安全重试（见上）。
* 错误响应统一结构：

```json
{"error": {"code": "NoSuchKey", "message": "...", "request_id": "..."}}
```

## 4. 错误分类

| code | HTTP | 触发场景 |
| --- | --- | --- |
| `Unauthorized` | 401 | 缺少 / 非法身份头 |
| `AccessDenied` | 403 | 鉴权未通过（读/写/列举/删除分别校验） |
| `NoSuchBucket` | 404 | 桶不存在 |
| `NoSuchKey` | 404 | 对象不存在（仅授权身份可见此区别） |
| `NoSuchUpload` | 404 | 分段上传不存在 / 已完成 / 已中止 |
| `BucketAlreadyExists` | 409 | 创建同名桶 |
| `BucketNotEmpty` | 409 | 删除仍有对象或未完成上传的桶 |
| `PreconditionFailed` | 412 | `If-Match` / `If-None-Match` 不满足 |
| `EntityTooLarge` | 413 | 对象 / 合并结果超限，或分段过小 |
| `QuotaExceeded` | 507 | 桶字节 / 对象数 / 并发写入配额超限 |
| `InvalidArgument` | 400 | 桶名 / key / 参数非法 |
| `InvalidPart` / `InvalidPartOrder` | 400 | 分段缺失、etag 不符、顺序错误 |
| `Conflict` | 409 | 同号分段内容冲突 |
| `InternalError` | 500 | 未预期错误（不吞异常、不静默成功） |

**存在性不泄露**：鉴权在资源存在性检查之前执行。对无权限身份，无论 key 是否
存在都返回完全相同的 `403 AccessDenied`；只有授权身份才会得到可区分的 404。

## 5. 权限规则

策略 JSON 形如：

```json
{"statements": [
  {"effect": "allow", "principal": "bob", "actions": ["s3:GetObject"]},
  {"effect": "deny",  "principal": "bob", "actions": ["s3:DeleteObject"]}
]}
```

* 动作：`s3:CreateBucket` `s3:DeleteBucket` `s3:ListBucket` `s3:GetObject`
  `s3:PutObject` `s3:DeleteObject`，`"*"` 匹配全部。
* 判定顺序：**owner 直通 → 显式 deny 优先 → 匹配 allow → 默认 deny**。
* 读、写、列举、删除分别校验：只授予 `GetObject` 的身份不能列举或写入。

## 6. 配额

创建桶时可声明：`{"quotas": {"max_bytes": 1073741824, "max_objects": 10000}}`
（0 表示不限）。

* 覆盖按**新尺寸**重新核算；超限在写入前 / 提交前拒绝。
* 并发写入受 `OSG_MAX_INFLIGHT_WRITES`（默认 32）约束，超限立即 507 快速失败，
  排队不会发生，且拒绝时**尚未产生任何临时分片或占位元数据**。
* 每个 key 的活跃分段上传数受 `OSG_MAX_UPLOADS_PER_OBJECT`（默认 100）限制。

## 7. 崩溃恢复（启动时自动执行）

`GatewayStore.recover()` 在启动时扫描数据目录并对账：

1. 删除所有 `.tmp` / `.incoming-*` / `.merge-*` 临时文件（写入中途崩溃）；
2. 中止所有未完成的分段上传并删除其分片目录（不残留悬挂上传）；
3. 删除“有元数据无主体”的对象条目（无主体元数据）；
4. 删除“有主体无元数据”的孤儿 blob（不可回收残留）；
5. 重算桶统计字节 / 对象数。

写路径本身是崩溃安全的：payload 先写入临时文件并 `fsync`，再用同目录原子
`rename` 发布，最后才提交元数据；任何时刻崩溃都只会留下“旧版本”或“完整新版本”。

> 分段上传在重启后默认按“中止”恢复（`OSG_ABORT_ORPHAN_UPLOADS=1`）。客户端
> 收到 `NoSuchUpload` 后重新初始化上传即可，这是可解释、可预期的恢复策略。

## 8. 内存与流式边界

* 上传：网络块 → 容量 2 块的有界队列 → 工作线程落盘，`fsync` 不阻塞事件循环；
  超过 `OSG_MAX_OBJECT_SIZE` 立即 413 并删除临时文件。
* 下载：按 `OSG_STREAM_CHUNK_SIZE`（默认 1 MiB，硬上限 64 MiB）分块读，不整对象
  载入内存。
* 列举：单页受 `OSG_MAX_LIST_KEYS`（默认 1000，硬上限 10000）约束，通过
  `marker` 续页；不会一次性返回全量键。

## 9. 配置项（环境变量）

| 变量 | 默认 | 含义 |
| --- | --- | --- |
| `OSG_DATA_DIR` | `./.osg-data` | 本地数据目录 |
| `OSG_MAX_OBJECT_SIZE` | `5368709120` (5 GiB) | 单对象 / 合并结果硬上限 |
| `OSG_STREAM_CHUNK_SIZE` | `1048576` (1 MiB) | 流式读写块大小（硬上限 64 MiB） |
| `OSG_MAX_LIST_KEYS` | `1000` | 列举单页上限（硬上限 10000） |
| `OSG_MIN_PART_SIZE` | `5242880` (5 MiB) | 非末段分片最小尺寸 |
| `OSG_MAX_UPLOADS_PER_OBJECT` | `100` | 单 key 活跃上传数 |
| `OSG_MAX_INFLIGHT_WRITES` | `32` | 单桶并发写闸门 |
| `OSG_ABORT_ORPHAN_UPLOADS` | `1` | 重启时中止悬挂上传 |

## 10. 本地验证

```bash
# 全量单测（并发写入 / 分段重传 / 越权访问 / 中断恢复 / 配额无残留）
uv run pytest -q

# 让分段最小尺寸变小，便于手工用小文件验证合并
OSG_MIN_PART_SIZE=4 uv run uvicorn main:app --port 8000
```

手工 curl 演练：

```bash
H='X-OSG-Principal: alice'
curl -s -XPUT http://127.0.0.1:8000/v1/buckets/demo -H "$H"
echo hello | curl -s -XPUT --data-binary @- http://127.0.0.1:8000/v1/buckets/demo/objects/a.txt -H "$H"
curl -s http://127.0.0.1:8000/v1/buckets/demo/objects/a.txt -H "$H"

# 分段上传
UID=$(curl -s -XPOST "http://127.0.0.1:8000/v1/buckets/demo/objects/big?uploads" -H "$H" | python -c 'import sys,json;print(json.load(sys.stdin)["upload_id"])')
E1=$(printf AAAA | curl -s -XPUT --data-binary @- "http://127.0.0.1:8000/v1/buckets/demo/objects/big/parts/1?uploadId=$UID" -H "$H" | python -c 'import sys,json;print(json.load(sys.stdin)["etag"])')
E2=$(printf BBBB | curl -s -XPUT --data-binary @- "http://127.0.0.1:8000/v1/buckets/demo/objects/big/parts/2?uploadId=$UID" -H "$H" | python -c 'import sys,json;print(json.load(sys.stdin)["etag"])')
curl -s -XPOST "http://127.0.0.1:8000/v1/buckets/demo/objects/big?uploadId=$UID" \
  -H "$H" -H 'Content-Type: application/json' \
  -d "{\"parts\":[{\"part_number\":1,\"etag\":\"$E1\"},{\"part_number\":2,\"etag\":\"$E2\"}]}"
```

## 11. 磁盘布局

```
<data_dir>/
  buckets/<bucket>..json                     # 桶记录：owner/policy/对象索引/配额/上传索引
  objects/<bucket>/<encoded-key>/<version>.part
  uploads/<bucket>/<encoded-key>/<upload-id>/meta.json
  uploads/<bucket>/<encoded-key>/<upload-id>/parts/<nnnnn>.part
```

桶名与 key 在落盘前都做百分号编码，`../`、绝对路径、unicode key 都无法逃逸出
自身目录。
