# Data Sync v1 架构与实现

本版本采用 Windows Python Agent → HAProxy 单端口 SNI/TCP 转发 → 多个 MinIO 的链路。前置机只维护静态路由，不解密、不落盘、不运行自研业务程序。每个目标独立上传相同批次，文件及 MySQL 新增记录都通过 manifest 提交。

## 职责和完成语义

Agent 持久保存目录清单、文件稳定状态、批次、目标任务、MySQL 游标及 multipart 会话；所有目标只接收其启用后创建的批次。启用时间取配置时间与首次观察启用时间的较晚者，重启不会重置；目标重新启用时只为后续新批次建立任务，原有未完成任务仍会重试。禁用期间新批次不自动补发。

`COMMITTED` 表示数据文件及 manifest 已写入该目标 MinIO并通过 HEAD 校验；不表示下游数据库已入库。数据库接收端消费程序不属于本次 v1 Agent 实施范围。下游应验证 manifest，然后在一个数据库事务内完成业务写入及 manifest_id 去重记录。

## 持久化与状态

SQLite 使用 WAL、synchronous=FULL 和 schema migration。进程通过操作系统文件锁保证同一 work_dir 只有一个 Agent。工作线程各自使用 SQLite 连接，按目标领取带 lease 的任务；正常传输刷新 lease，重启在进程锁内清除旧所有者后恢复。

- 文件：DISCOVERED → STABILIZING → STABLE → BATCHED；首次跳过记为 IGNORED，异常记为 QUARANTINED。
- 批次：OPEN → SEALED → READY；内容冲突或封口时源文件变化进入 QUARANTINED。
- 目标任务：PENDING → UPLOADING_FILES → VERIFYING → UPLOADING_MANIFEST → COMMITTED；暂时故障进入 RETRY_WAIT，认证、证书或对象冲突进入 BLOCKED。

目录扫描失败或部分子目录不可读时，本轮不会封口。文件大小和 mtime 连续稳定后参与批次；所有已知文件稳定且经过 quiet_seconds 无新成员才封口。静默窗口是一项上游时序假设，无法证明未知文件不会迟到，迟到文件记录 LATE_FILE，不改变已封口清单。源文件首次扫描的基线持久保存在 SQLite，服务重启会发现停机期间新增文件。

封口后制作 fsync + 原子重命名的 spool 快照，后续所有目标只读快照。拷贝前后核对源文件大小/mtime，发现变化时隔离。已经 READY 后原文件被改写，只隔离对应源文件记录，不改变已生成批次和快照。

## Manifest 传输契约

结构包含 schema_version、manifest_id、batch_no、source、dataset、files、summary 和 created_at。每个 files 元素携带 target_key、size、SHA-256、relative_path、role 和 content_type。完整示例由测试和 Agent 按配置生成，字段约束在 `data_sync/manifest.py` 中定义。

规范化 JSON：UTF-8、不转义非 ASCII、字段字典序排列、紧凑分隔符、禁止 NaN/Infinity。manifest_id = `sha256:` + SHA-256(包含 schema_version、source_id、batch_no、dataset、按 target_key 排序 files 的规范化 JSON)。created_at 首次创建后持久化，不参与身份计算，重试使用同一份字节内容。

对象布局：`{target.prefix}/{source.prefix}/{agent.source_id}/{source.id}/{日期/仅文件批次}{batch_no}/data/{相对路径}`，manifest 位于该批次目录的 manifest.json。所有目标 prefix 必须相同，bucket 可不同。对象键包含来源和批次，避免同名文件跨来源覆盖。batch_no 对同一 source.id 必须唯一，未按文件名正则匹配的数据隔离。

上传使用 PutObject/CompleteMultipartUpload 的 `If-None-Match: *`，已存在同 key 时仅在大小和 metadata.sha256 相同后视为成功；条件写不受支持的目标应升级，不降级为覆盖写。SDK分片请求携带 Content-MD5校验传输，ETag只用于 multipart 完成。每个文件上传完成后 HEAD 核对 size 和 metadata.sha256，所有对象再次确认后才上传 manifest，最后 HEAD核验 manifest 后提交状态。

HEAD 中的 SHA-256 metadata 是生产者的声明，不是服务端重新计算的文件摘要。严格端到端验收需接收端 GET 对象并计算 SHA-256；不得把 HEAD 检查描述为已完成目标字节级对账。

分片恢复持久保存 upload_id、分片尺寸和已完成 parts，并通过 ListParts 分页修复丢失响应造成的本地状态差异。完成请求丢失响应时先 HEAD 最终对象；上传会话过期则重新上传。创建 upload_id 后、写入 SQLite前崩溃的孤立会话由目标 bucket 未完成 multipart 生命周期清理。

## MySQL 增量边界

仅支持正的有符号 64 位整数单列游标，配置字段必须包含游标。使用参数化 `id > last_id ORDER BY id LIMIT n`；initial_scan=new_only 首次持久记录 MAX(id)，existing_and_new 从 0 开始。数据库表只读，更新、删除、DDL 和通用 SQL 不在范围内。

**自增 ID 的分配顺序不等于事务提交顺序。** 例如 ID=11 已提交、ID=10 尚未提交，按游标读到 11 后可能永久漏掉 10。因此要求业务保证可见记录按游标单调提交（例如串行写入），并显式配置 commit_order_guaranteed=true。这不是程序自动验证的保证；普通并发写入表需采用 Outbox/CDC 等后续方案。有限时间回看不能无条件消除长事务风险。

查询一批后先写 rows.jsonl，再原子写 prepared envelope（包含固定批次内容地址、目标名单、创建时间和范围）。登记 SEALED 后生成 manifest，在一个 SQLite事务内设置 READY并推进游标。恢复优先处理 envelope，不重新查询已准备批次。查询后、envelope 完成前崩溃时游标未推进，可以重新查询；尚未提交任何目标。

JSONL 普通值保持 JSON类型；Decimal采用 `{ "$type":"decimal", "value":"..." }`；二进制使用 binary/base64 标签；date/time/datetime保留原值文本和类型；MySQL TIME对应 timedelta使用 duration_us。MySQL DATETIME未带时区时不擅自附加时区；manifest的 created_at/obs_time才要求带时区。NULL使用 null，禁止非有限浮点数。

## 运维和验证

源文件不自动删除。spool 快照当前保留，磁盘预留不足时停止新批次准备而不推进游标，已准备任务仍能发送。工作目录必须预留最慢目标积压容量；保留与清理、凭据、部署操作见 operations.md。

自动测试覆盖扫描、稳定/静默窗口、迟到隔离、新目标边界、multipart断线和完成响应丢失、manifest失败重试、对象冲突、多目标隔离、SQL分页与checkpoint恢复。Windows SCM、真实 HAProxy TLS路由和真实 MinIO/MySQL需在部署环境进行联调，不能由内存替身测试替代。

协议依据：[S3 条件完成上传](https://docs.aws.amazon.com/boto3/latest/reference/services/s3/client/complete_multipart_upload.html)、[MySQL AUTO_INCREMENT锁与提交边界](https://dev.mysql.com/doc/refman/8.0/en/innodb-auto-increment-handling.html)。
