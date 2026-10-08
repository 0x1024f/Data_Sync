# Data Sync Agent

Windows Python Agent：按 root、include、exclude 监测目录，文件稳定后独立上传多个 MinIO，保留根目录相对路径；后续修改稳定后覆盖同路径对象。目录文件不分组、不上传 manifest。MySQL 新增记录仍按批次上传，manifest.json 最后提交；目标独立重试，前置机无业务状态。

支持为每个目标配置[上传成功后通知接口](docs/upload-notification.md)：文件上传校验成功后发送 JSON POST，失败后分别等待 30 秒、3 分钟重试，总共最多 3 次，并支持重启恢复。

也支持 HTTP 直连 MinIO。`config.test.yaml` 已配置 `http://192.168.0.21:30009`，无需证书；使用前确认桶名，并填写 `access_key` 和 `secret_key`。具体配置见部署文档。

```bash
python -m pip install '.[test]'
python -m pytest -q
data-sync --config config.local.yaml validate
data-sync --config config.local.yaml run
data-sync --config config.local.yaml status
```

以 `config.example.yaml`为模板配置；MinIO 密钥可直接填写带引号的字符串，也可通过 `env:变量名`引用。MySQL 凭据仍使用环境变量。Windows构建/服务安装脚本位于 deploy/windows，HAProxy示例位于 deploy/haproxy。

详情见 [架构与协议](docs/enterprise-data-sync-architecture.md)、[部署与故障处理](docs/operations.md)。MySQL轮询要求**按游标顺序提交**，仅有AUTO_INCREMENT不能保证不漏数。目标数据库入库消费者需按协议另行接入。

源文件默认保留，spool不自动清理。initial_scan 保留默认 new_only：目录首次扫描建立基线，此后新增或修改时上传；existing_and_new 也上传现有文件。新目标不补历史版本；本地删除不删除远端对象。文件对象键不使用 targets.prefix，该配置只用于 MySQL。

从分组上传版本升级时，删除已移除的文件源配置字段，并改用新的 agent.work_dir（例如 ./runtime-files）；保留旧工作目录及远端对象。程序拒绝直接打开旧文件分组状态。现场需验证真实 Windows 服务、HAProxy、MinIO 和 MySQL 提交约束。
