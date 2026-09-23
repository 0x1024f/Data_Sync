# Data Sync Agent

Windows Python Agent：目录文件与 MySQL新增记录分批、持久化快照，通过前置机单端口 HAProxy SNI路由分别上传多个 MinIO。数据文件先提交，manifest.json最后提交；目标独立重试，前置机无业务状态。

也支持 HTTP 直连 MinIO。`config.test.yaml` 已配置 `http://192.168.0.21:30009`，无需证书；使用前确认桶名，并填写 `access_key` 和 `secret_key`。具体配置见部署文档。

```bash
python -m pip install '.[test]'
python -m pytest -q
data-sync --config config.local.yaml validate
data-sync --config config.local.yaml run
data-sync --config config.local.yaml status
```

以 `config.example.yaml`为模板配置；MinIO 密钥可直接填写带引号的字符串，也可通过 `env:变量名`引用。MySQL 凭据仍使用环境变量。Windows构建/服务安装脚本位于 deploy/windows，HAProxy示例位于 deploy/haproxy。

详情见 [架构与协议](docs/enterprise-data-sync-architecture.md)、[部署与故障处理](docs/operations.md)。MySQL轮询要求**按游标顺序提交**，仅有AUTO_INCREMENT不能保证不漏数。v1完成的是到MinIO的批次传输，目标数据库入库消费者需按协议另行接入。

源文件默认保留，spool不自动清理；初次目录/数据库采集可选择 existing_and_new，新目标不补历史批次。现场需验证真实Windows服务、HAProxy、MinIO条件写和MySQL提交约束。
