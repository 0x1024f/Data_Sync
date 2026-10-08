# 部署与故障处理

目录文件上传后可调用业务通知接口，支持 30 秒、3 分钟重试及重启恢复，详见[上传后通知配置与故障处理](upload-notification.md)。

## 目录文件同步

文件仅按 `root`、`include`、`exclude` 匹配，exclude 优先；保留原有 glob 匹配规则。`recursive: false` 只扫描根目录。文件连续 `stable_seconds` 大小和修改时间不变后独立生成快照，上传对象键为根目录相对路径。例如 root 为 `E:/data` 时，`E:/data/FY3F/a.hdf` 上传为 `FY3F/a.hdf`，不使用 targets.prefix，不添加日期、来源或批次目录。

目录文件不生成或上传 manifest，`_data_sync` 不再是保留名称。后续文件变化稳定后覆盖原对象；内容摘要相同则不重复上传。本地删除不删除远端对象。`initial_scan: new_only`（默认）首次只建立已有文件基线，之后新增或修改时上传；`existing_and_new` 同时上传已有文件。基线跨重启保留。

同一目标、同一对象键的版本串行执行。旧版本重试或 BLOCKED 时，该键后续版本等待，其他键和目标继续处理。不同文件源或 MySQL 占用同一目标键会报告 ObjectKeyConflict 并阻止覆盖；不要让多个独立 Agent 向同一键写数据，本地所有权记录不跨工作目录共享。

### 从分组版本升级

停止旧进程，保留旧配置、完整工作目录及远端对象。移除文件源的 `filename_regex`、`quiet_seconds`、`path_layout`、`obs_time_format`、`timezone_offset`、`prefix`；改用新的 `agent.work_dir`，例如 `./runtime-files`。旧状态不会迁移，未完成旧任务不会自动转入新工作目录。按 `initial_scan` 选择是否重新采集现有文件；程序检测旧分组状态时明确拒绝打开。

账号需要业务相对路径范围的 PutObject、GetObject（HEAD）、AbortMultipartUpload、ListMultipartUploadParts 和对应 ListBucket 权限。仅授权原 transfer/ 前缀不能覆盖全部新对象键。MySQL 继续使用原有 prefix 与 manifest 协议。

## Windows Agent

建议在 Windows x64 / Python 3.11 或 3.12 上构建。源代码兼容 Python 3.9+。在项目目录执行 `python -m pip install '.[test,windows]'`，再执行 `powershell -File deploy/windows/build.ps1`。Windows 二进制必须在 Windows构建；不能把 macOS构建产物作为 Windows服务部署。

交付 dist/data-sync 与 dist/data-sync-service 两个完整目录，包含各自 _internal依赖。把 config.example.yaml复制为服务目录 config.yaml，填写源目录、include/exclude、目标域名、CA和工作目录。命令行工具始终用 `--config`指定同一文件。相对路径以配置文件目录为基准。文件扫描范围避免包含 work_dir，避免多个 Agent使用同一 source_id向同一对象前缀写数据。

MinIO 的 access_key 和 secret_key 支持直接填写带引号的字符串，也支持 env:变量名；以 env: 开头的值始终按环境变量引用解析。真实凭据可保存在已被 Git 忽略的 config.local.yaml 中。MySQL 凭据仍使用 env:变量名。使用环境变量时，在服务账户可见的环境中配置；交互终端里的临时环境变量不会自动传给 SCM启动的服务。安装后通过服务管理器设置专用账户，赋予源目录只读、work_dir读写和配置/CA读取权限。不要把密钥写入命令行参数、版本库或日志。

命令示例：

```powershell
.\data-sync.exe --config C:\DataSync\service\config.yaml validate
.\data-sync.exe --config C:\DataSync\service\config.yaml once
.\data-sync.exe --config C:\DataSync\service\config.yaml status
powershell -File deploy/windows/install.ps1 -ServiceDirectory C:\DataSync\service
Start-Service DataSyncAgent
Stop-Service DataSyncAgent
```

`once`只扫描一轮并尝试当下可执行任务，不会等待整个稳定窗口，也不意味着所有任务成功。用 status查看实际状态。持续运行使用 run或服务。安装脚本配置自动启动和异常恢复但不自动启动，以便先设置服务账户。SCM接收到停止后，Agent停止领新任务，当前请求最长等待网络超时，再保存状态退出。

升级时停止服务，保留配置、环境凭据和完整 work_dir，备份 SQLite与WAL（或关闭后备份），替换完整程序目录再启动。数据库版本比程序新时拒绝打开；不要用旧程序写新版本状态。

## 单端口与证书

### HTTP 直连 MinIO

对 `http://192.168.0.21:30009`，目标配置使用：

```yaml
targets:
  - id: minio-http
    scheme: http
    host: 192.168.0.21
    port: 30009
    bucket: enterprise-raw # 示例值，必须改为对方已创建的实际桶名
    prefix: transfer
    access_key: 'YOUR_MINIO_ACCESS_KEY'
    secret_key: 'YOUR_MINIO_SECRET_KEY'
```

HTTP 不配置 `ca_bundle`，不需要证书，但仍通过 Access Key / Secret Key 签名认证。将对方提供的 MINIO_ROOT_USER 值填写到 access_key，将 MINIO_ROOT_PASSWORD 值填写到 secret_key。也可分别填写 env:MINIO_ACCESS_KEY 和 env:MINIO_SECRET_KEY 并设置对应环境变量；程序不会自动读取 .env 文件。HTTP 传输内容不加密，应在可信网络中使用。

复制 `config.test.yaml` 为 `config.local.yaml`，采集目录为 `./test-input`。填写凭据并确认实际桶名后执行 `python -m data_sync --config config.local.yaml validate`，再执行 `python -m data_sync --config config.local.yaml run`。validate 只检查配置和凭据是否存在，不会验证远端连接。

省略 `scheme` 仍默认 HTTPS，端口默认 443；HTTP 请显式填写实际端口。不同目标可使用不同端口。已有目标改变协议、地址或桶时使用新 id；新目标不会补发历史批次。

### HTTPS 单端口 SNI 透传

以下配置仅适用于 HTTPS。HTTP 直连必须从 Agent 网络可达上述 IP 和端口，不能经过现有拒绝非 TLS 流量的 SNI 透传入口。

内网 DNS或 hosts将 suzhou-transfer.example.internal、site-b-transfer.example.internal均指向前置机 IP。端口统一为443。使用域名确保 TLS SNI和证书验证生效。前置机到各目标放行 MinIO HTTPS API端口，MinIO证书 SAN必须覆盖对应传输域名。目标的9000如果仍为明文 HTTP，不能直接接入此 TLS透传配置。

HAProxy建议使用发行版受支持版本，需支持 req.ssl_sni/master-worker。模板中的 192.0.2.0/24、198.51.100.10和203.0.113.10是文档保留地址，必须替换。使用发行版自带的 haproxy systemd单元，开启 `systemctl enable haproxy`。首次检查 `haproxy -c -f /etc/haproxy/haproxy.cfg`，更新时运行 validate-reload.sh。确认发行版单元的 ExecReload使用master-worker平滑重载，不要用restart替代reload来验证长连接不中断。

未知SNI、非TLS连接拒绝，不设置默认出口。check只检测TCP可达性，不代表证书、凭据或bucket正常。配置日志输出 /dev/log，rsyslog规则转存 /var/log/haproxy.log，安装对应 logrotate配置（若发行版已有同等规则则复用）。

新增目标顺序：准备目标 HTTPS与bucket → 开放前置机出站网络 → 添加域名解析及SNI路由 → 校验并reload → Agent添加新id并重启。配置 enabled_at可安排未来启用；不允许倒填时间触发旧批次补发。已有目标的host/port/bucket/prefix不允许原id修改；必须使用新id，避免把旧成功状态错误用于新存储。

所有bucket预先创建。账号需要指定prefix下的 s3:PutObject、s3:GetObject（HEAD使用）、s3:AbortMultipartUpload、s3:ListMultipartUploadParts；通常还需要限定prefix的 s3:ListBucket以区分404和403。管理端预设AbortIncompleteMultipartUpload生命周期，避免断点会话永久占用空间；时限应长于通常故障恢复时间。Agent不会自动建bucket或更改生命周期。

## 故障检查

status输出目标任务状态、file_versions 文件版本计数、MySQL batches、文件错误、游标、heartbeat及源采集错误。work_dir/logs/agent.jsonl采用结构化日志并轮转5个10MiB备份。错误只记类型/代码，避免输出凭据或数据库行。任务的尝试时间和结果记录于SQLite attempts。

- RETRY_WAIT：检查网络、前置机、目标存储和磁盘；恢复后自动重试。
- BLOCKED：修复证书/权限/配置后，停止Agent，执行 `data-sync --config ... retry TASK_ID`，重新启动。ObjectKeyConflict 表示不同来源占用同一键，应调整来源的相对路径或使用不同目标桶；目录文件自身的后续修改会正常覆盖。
- SOURCE_MISSING：恢复文件后重新等待稳定。单文件读取失败记录错误并在后续扫描重试，不阻塞其他文件；目录遍历不完整时不完成首次扫描基线。
- 磁盘空间不足：停止生成新快照/数据库批次，不推进相关游标；释放安全可清理空间或扩容后重试。

v1不自动删除spool。容量监控同时关注尚未提交目标和已提交快照的累计量。停服务后，仅可删除所有既定目标任务均为COMMITTED的批次快照目录；保留SQLite清单和幂等记录。MySQL envelope所引用的spool、无任务批次（可能所有目标当时禁用）、OPEN/SEALED/失败任务均不要清理。原文件由业务方自行管理。

## 现场验收

1. 使用独立测试目录和桶，持续写文件并确认稳定窗口前不提交；确认文件保留相对路径且无额外 manifest，再修改内容检查覆盖。MySQL 单独验证 manifest 最后提交。
2. 通过前置机分别访问两个SNI域名，确认落入不同目标；用未知SNI检查拒绝，reload期间保留大文件上传连接。
3. 断网、强杀Agent并重启，确认ListParts恢复；目标B停机时目标A继续完成。
4. 在各目标GET文件并计算SHA-256，与本地快照摘要比较（MySQL 可比较 manifest）；HEAD metadata 检查不能替代此步骤。
5. 仅对已确认按ID提交的MySQL测试表测试分页、停机新增和checkpoint恢复。普通并发长事务表不得以本轮询方案宣称不漏数。
6. Windows SCM检查开机启动、停止、异常恢复、服务账户凭据、打包依赖和日志轮转。

数据库消费者需要按manifest_id幂等，在同一事务中写业务行和消费记录；tagged-jsonl-v1的Decimal/二进制/日期类型须按标签还原。对象存储到达不等于目标数据库同步完成。
