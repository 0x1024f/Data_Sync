# 部署与故障处理

## 文件相对路径模式

文件源设置 `path_layout: relative` 后，数据对象直接使用相对于 `root` 的路径：
例如 `E:/data/FY3F/a.hdf`（root 为 `E:/data`）上传到 `bucket/FY3F/a.hdf`。
此模式不使用 targets.prefix 或 files.prefix，不添加日期、站点、批次或 data 目录。
省略 path_layout 或设置 batch 时保持原来的路径结构；MySQL 路径不受影响。

批次清单最后提交到 `_data_sync/manifests/<manifest_id摘要>.json`。
清单 dataset.capture_id 使用持久化批次 ID，以区分内容相同但采集时间不同的批次。
桶根目录的 `_data_sync` 是保留名称，命中采集规则的同名根文件或该目录下文件会以
`RESERVED_PATH` 隔离。原有正则分批、稳定窗口和静默窗口仍然有效。
同路径同内容复用，内容不同则 BLOCKED，不覆盖；多个来源共享桶时同样适用。

切换已有源时使用新 files.id，并设置 `initial_scan: new_only`；首次扫描所见文件被忽略，
之后新增文件才采集（首次扫描之前到达的文件也属于基线）。不要删除 work_dir 或远端旧对象。
旧任务继续使用保存的对象路径。旧源 ID 直接改变 path_layout 会被拒绝。
相对路径模式账号需要业务相对路径及 `_data_sync/manifests/` 的上传、HEAD、分片恢复权限，
以及对应范围的 ListBucket 权限；仅授权原 transfer/ 前缀将无法使用新模式。

## Windows Agent

建议在 Windows x64 / Python 3.11 或 3.12 上构建。源代码兼容 Python 3.9+。在项目目录执行 `python -m pip install '.[test,windows]'`，再执行 `powershell -File deploy/windows/build.ps1`。Windows 二进制必须在 Windows构建；不能把 macOS构建产物作为 Windows服务部署。

交付 dist/data-sync 与 dist/data-sync-service 两个完整目录，包含各自 _internal依赖。把 config.example.yaml复制为服务目录 config.yaml，填写源目录、文件正则、目标域名、CA和工作目录。命令行工具始终用 `--config`指定同一文件。相对路径以配置文件目录为基准。文件扫描范围避免包含 work_dir，避免多个 Agent使用同一 source_id向同一对象前缀写数据。

凭据只支持 env:变量名；在服务账户可见的环境中配置。交互终端里的临时环境变量不会自动传给 SCM启动的服务。安装后通过服务管理器设置专用账户，赋予源目录只读、work_dir读写和配置/CA读取权限。部署时确认账户环境变量已生效，必要时重新登录或重启主机。不要把密钥写入命令行参数、版本库或日志。

命令示例：

```powershell
.\data-sync.exe --config C:\DataSync\service\config.yaml validate
.\data-sync.exe --config C:\DataSync\service\config.yaml once
.\data-sync.exe --config C:\DataSync\service\config.yaml status
powershell -File deploy/windows/install.ps1 -ServiceDirectory C:\DataSync\service
Start-Service DataSyncAgent
Stop-Service DataSyncAgent
```

`once`只扫描一轮并尝试当下可执行任务，不会等待整个稳定/静默窗口，也不意味着所有任务成功。用 status查看实际状态。持续运行使用 run或服务。安装脚本配置自动启动和异常恢复但不自动启动，以便先设置服务账户。SCM接收到停止后，Agent停止领新任务，当前请求最长等待网络超时，再保存状态退出。

升级时停止服务，保留配置、环境凭据和完整 work_dir，备份 SQLite与WAL（或关闭后备份），替换完整程序目录再启动。数据库版本比程序新时拒绝打开；不要用旧程序写新版本状态。

## 单端口与证书

内网 DNS或 hosts将 suzhou-transfer.example.internal、site-b-transfer.example.internal均指向前置机 IP。端口统一为443。使用域名确保 TLS SNI和证书验证生效。前置机到各目标放行 MinIO HTTPS API端口，MinIO证书 SAN必须覆盖对应传输域名。目标的9000如果仍为明文 HTTP，不能直接接入此 TLS透传配置。

HAProxy建议使用发行版受支持版本，需支持 req.ssl_sni/master-worker。模板中的 192.0.2.0/24、198.51.100.10和203.0.113.10是文档保留地址，必须替换。使用发行版自带的 haproxy systemd单元，开启 `systemctl enable haproxy`。首次检查 `haproxy -c -f /etc/haproxy/haproxy.cfg`，更新时运行 validate-reload.sh。确认发行版单元的 ExecReload使用master-worker平滑重载，不要用restart替代reload来验证长连接不中断。

未知SNI、非TLS连接拒绝，不设置默认出口。check只检测TCP可达性，不代表证书、凭据或bucket正常。配置日志输出 /dev/log，rsyslog规则转存 /var/log/haproxy.log，安装对应 logrotate配置（若发行版已有同等规则则复用）。

新增目标顺序：准备目标 HTTPS与bucket → 开放前置机出站网络 → 添加域名解析及SNI路由 → 校验并reload → Agent添加新id并重启。配置 enabled_at可安排未来启用；不允许倒填时间触发旧批次补发。已有目标的host/port/bucket/prefix不允许原id修改；必须使用新id，避免把旧成功状态错误用于新存储。

所有bucket预先创建。账号需要指定prefix下的 s3:PutObject、s3:GetObject（HEAD使用）、s3:AbortMultipartUpload、s3:ListMultipartUploadParts；通常还需要限定prefix的 s3:ListBucket以区分404和403。管理端预设AbortIncompleteMultipartUpload生命周期，避免断点会话永久占用空间；时限应长于通常故障恢复时间。Agent不会自动建bucket或更改生命周期。

## 故障检查

status输出目标状态计数、隔离文件计数、游标、heartbeat及源采集错误。work_dir/logs/agent.jsonl采用结构化日志并轮转5个10MiB备份。错误只记类型/代码，避免输出凭据或数据库行。任务的尝试时间和结果记录于SQLite attempts。

- RETRY_WAIT：检查网络、前置机、目标存储和磁盘；恢复后自动重试。
- BLOCKED：修复证书/权限/配置后，停止Agent，执行 `data-sync --config ... retry TASK_ID`，重新启动。对象内容冲突先人工核查，不提供覆盖开关。
- LATE_FILE：调大quiet_seconds并确认上游行为；已封口批次不会自动修改。需要新业务批次号才能重新采集。
- NAME_MISMATCH/METADATA_INVALID：核对正则完整匹配及obs_time格式；既有隔离记录不会因配置改变被静默重投。
- SOURCE_MISSING：恢复封口前丢失的文件，后续扫描重新等待稳定。源目录读取失败时不会封口。
- SOURCE_CHANGED_DURING_SEAL：上游在稳定窗口后仍修改文件，批次隔离，需人工提供新批次；不要修改已存快照。
- 磁盘空间不足：停止生成新快照/数据库批次，不推进相关游标；释放安全可清理空间或扩容后重试。

v1不自动删除spool。容量监控同时关注尚未提交目标和已提交快照的累计量。停服务后，仅可删除所有既定目标任务均为COMMITTED的批次快照目录；保留SQLite清单和幂等记录。MySQL envelope所引用的spool、无任务批次（可能所有目标当时禁用）、OPEN/SEALED/失败任务均不要清理。原文件由业务方自行管理。

## 现场验收

1. 先用无业务敏感数据的单批测试，持续写文件并确认稳定窗口前不提交，随后验证manifest最后出现。
2. 通过前置机分别访问两个SNI域名，确认落入不同目标；用未知SNI检查拒绝，reload期间保留大文件上传连接。
3. 断网、强杀Agent并重启，确认ListParts恢复；目标B停机时目标A继续完成。
4. 在各目标GET文件并计算SHA-256，比较manifest；HEAD metadata检查不能替代此步骤。
5. 仅对已确认按ID提交的MySQL测试表测试分页、停机新增和checkpoint恢复。普通并发长事务表不得以本轮询方案宣称不漏数。
6. Windows SCM检查开机启动、停止、异常恢复、服务账户凭据、打包依赖和日志轮转。

数据库消费者需要按manifest_id幂等，在同一事务中写业务行和消费记录；tagged-jsonl-v1的Decimal/二进制/日期类型须按标签还原。对象存储到达不等于目标数据库同步完成。
