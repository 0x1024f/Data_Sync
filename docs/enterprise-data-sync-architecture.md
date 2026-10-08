# Data Sync 架构与实现

Windows Python Agent 通过 HTTPS/HAProxy SNI 或 HTTP 直连多个 MinIO。目录文件独立上传并保留根目录相对路径，MySQL 增量记录保留批次与 manifest 协议。前置机不保存业务状态。

## 目录文件与完成语义

`root` 确定监测范围，include/exclude 使用已有 glob 匹配语义且 exclude 优先；recursive 控制子目录扫描，符号链接跳过。文件大小和 mtime 连续稳定达到 stable_seconds 后独立生成不可变快照，不解析文件名、不等待整组、不限制后到文件。

对象键严格为相对 root 的 POSIX 路径，不拼接任何 prefix。每条目录传输记录仅含一个文件的键、大小、SHA-256、内容类型和本地快照位置；这些描述只保存在 SQLite，不上传 manifest。内部沿用 batches/tasks 表作为传输记录与队列，但文件记录不代表业务分组。

初次扫描 new_only 记录现有文件基线；existing_and_new 同时采集已有文件。两种策略都采集后续新增和修改。变化通过大小和纳秒 mtime 检测，内容摘要相同不创建重复版本；无法检测大小和 mtime 同时保持不变的外部改写。源文件删除不触发远端删除。

单文件完成意味着数据对象已提交并经 HEAD 核对大小和 metadata.sha256。后续版本覆盖同键对象；不会写入额外清单。MySQL 完成仍要求数据和 manifest 全部提交，不代表下游数据库已经入库。

## 持久化、并发与恢复

SQLite 使用 WAL 和 synchronous=FULL；进程锁防止同一 work_dir 同时运行多个 Agent。状态格式版本为 3，旧文件分组状态在打开写连接前被拒绝，必须使用新 work_dir，旧状态和远端对象保留。

目录文件状态为 IGNORED、STABILIZING、BATCHED 或 MISSING。快照先 fsync 并原子重命名，再通过一个事务登记文件版本、目标任务及库存关联。复制期间变化重新等待稳定；读取失败单独记错，其他文件继续。登记前崩溃不发布半成品，重启重新采集；可能留下未登记的 spool 文件，不自动清理。

每个目标只为启用后生成的新版本建立任务，新目标不补历史版本。禁用期间新版本不自动补发，已有任务保留。领取任务时，同一目标同键的旧版本只要未 COMMITTED，后续版本就等待；旧版本 BLOCKED 后需修复并重试，防止旧任务覆盖已提交的新版本。不同键和目标独立执行。

目标键所有权在上传前通过 SQLite 事务登记：文件键归属来源与相对路径，MySQL 数据及 manifest 键归属对应批次。跨来源冲突返回 ObjectKeyConflict，不写远端。这项约束只覆盖同一工作目录；独立 Agent 或外部写入者不共享所有权记录。

任务流为 PENDING → UPLOADING_FILES → VERIFYING → COMMITTED；MySQL 在提交前另有 UPLOADING_MANIFEST。临时错误进入 RETRY_WAIT，权限、证书、快照损坏和键归属冲突进入 BLOCKED。工作线程独立连接 SQLite，上传刷新租约，重启在进程锁内清除旧租约后恢复。

普通文件通过 PutObject 覆盖，分片文件通过 CompleteMultipartUpload 覆盖；大小与摘要相同时跳过。上传前校验不可变快照，分片请求携带 Content-MD5；ETag 用于分片完成。持久保存 upload_id、part_size 与 parts，并通过 ListParts 恢复；完成响应丢失后通过 HEAD 确认，失效会话重新建立。

## MySQL manifest 传输契约

结构保持 schema_version、manifest_id、batch_no、source、dataset、files、summary、created_at。files 包含 target_key、size、SHA-256、relative_path、role、content_type。规范化 JSON 使用 UTF-8、字典序、紧凑分隔符且禁止非有限数；manifest_id 由规范化身份数据计算，created_at 不参与身份计算。

MySQL 保留原有 target.prefix/source.prefix/来源/批次路径和 manifest.json。含 MySQL 源时所有目标 prefix 必须一致。写入保留 If-None-Match: *；已有同键内容不同则阻止，同内容可以复用。数据先上传并校验，再上传 manifest，最后提交任务状态。

HEAD 的 SHA-256 metadata 是生产者声明，不是服务端重新计算摘要。严格验收需要 GET 对象并计算 SHA-256，不能将 HEAD 校验视为字节级对账。

## MySQL 增量边界

仅支持正的有符号 64 位整数单列游标，配置字段必须包含游标。使用参数化 `id > last_id ORDER BY id LIMIT n`；initial_scan=new_only 首次持久记录 MAX(id)，existing_and_new 从 0 开始。数据库表只读，更新、删除、DDL 和通用 SQL 不在范围内。

**自增 ID 的分配顺序不等于事务提交顺序。** 例如 ID=11 已提交、ID=10 尚未提交，按游标读到 11 后可能永久漏掉 10。因此要求业务保证可见记录按游标单调提交（例如串行写入），并显式配置 commit_order_guaranteed=true。这不是程序自动验证的保证；普通并发写入表需采用 Outbox/CDC 等后续方案。有限时间回看不能无条件消除长事务风险。

查询一批后先写 rows.jsonl，再原子写 prepared envelope（包含固定批次内容地址、目标名单、创建时间和范围）。登记 SEALED 后生成 manifest，在一个 SQLite事务内设置 READY并推进游标。恢复优先处理 envelope，不重新查询已准备批次。查询后、envelope 完成前崩溃时游标未推进，可以重新查询；尚未提交任何目标。

JSONL 普通值保持 JSON类型；Decimal采用 `{ "$type":"decimal", "value":"..." }`；二进制使用 binary/base64 标签；date/time/datetime保留原值文本和类型；MySQL TIME对应 timedelta使用 duration_us。MySQL DATETIME未带时区时不擅自附加时区；manifest的 created_at/obs_time才要求带时区。NULL使用 null，禁止非有限浮点数。

## 运维和验证

源文件与已登记 spool 快照不自动删除，容量应覆盖最慢目标积压。磁盘不足时暂停对应快照生成，MySQL 不推进未完成准备的游标。status 区分 file_versions、MySQL batches、目标任务及文件错误。

自动测试覆盖独立稳定、筛选、相对路径、修改覆盖、首次扫描、同键版本顺序、分片恢复、响应丢失、键冲突、MySQL manifest 与游标恢复。Windows SCM、实际 MinIO/MySQL 和 HAProxy 需现场验证。详见 operations.md 与 validation.md。
