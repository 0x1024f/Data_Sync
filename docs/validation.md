# 当前验证记录

本地执行环境：macOS ARM64，Python 3.9.6，隔离虚拟环境。

已执行：

- `python -m pytest -q`：34项通过，包括完整Agent单轮处理、真实botocore请求参数校验、分片中断恢复、完成响应丢失、manifest最后提交、同key冲突拒绝、多目标隔离、迟到文件隔离、首次扫描与重启、封口重启后拒绝源文件变化、磁盘/目录错误、MySQL prepared/checkpoint故障恢复。
- `python -m pip check`：依赖无冲突。
- `python -m compileall -q data_sync`：语法检查通过。
- CLI帮助、配置验证和只读status已验证。

SDK在Python3.9上报告停止支持提示，生产部署按operations.md使用Python3.11/3.12。GitHub workflow已提供Linux/Windows测试、HAProxy配置语法检查和Windows PyInstaller产物构建；该workflow尚未在此会话触发。

未在本机验证：Windows SCM实际安装/停止/异常拉起、Windows EXE运行、真实HAProxy TLS SNI路由与平滑reload、真实MinIO条件写支持与字节级SHA-256对账、真实MySQL提交顺序及权限。测试中的内存S3/MySQL替身只验证Agent逻辑；这些现场验收项见operations.md。
