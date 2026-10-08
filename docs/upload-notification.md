# MinIO 上传后通知

在需要通知的 `targets` 项下添加 `notify`。不配置或填写 `notify: null` 即关闭；每个目标独立设置。示例：

```yaml
    bucket: rs-data
    notify:
      url: http://192.168.0.22:30084/rs/third-party/tif-push/notify
      module_type: ecology
      path_prefix: /rs-shared
      timeout_seconds: 30
```

请使用实际上传桶名；已有目标更换桶名仍须使用新的目标 ID。通知的 `bucketName` 自动取当前目标的 `bucket`，不能单独覆盖。

目录文件稳定、上传到 MinIO 并校验通过后，才发送第一次通知。每个文件、每个启用通知的目标分别发送一次，后续修改上传的新版本也会通知。MySQL 批次及 manifest 不通知。

请求为 `Content-Type: application/json` 的 POST：

```json
{
  "moduleType": "ecology",
  "bucketName": "rs-data",
  "filePathList": ["/rs-shared/fy3f/2026/07/20/a.HDF"]
}
```

`filePathList` 由 `path_prefix` 与实际对象键拼接，使用 `/`，保留中文和空格，不修改对象键。空前缀表示直接使用对象键；`/` 前缀表示在对象键前加 `/`。注意对象键相对于监测的 `root`，前缀需与接口侧实际路径映射一致。

只有 HTTP 2xx 且响应 JSON 顶层 `code` 为数字 `200` 才成功，字符串 `"200"` 不算成功。接口目前不携带鉴权，不使用系统代理，也不跟随重定向；请配置最终接口地址。超时默认 30 秒，可设置为 1–600 秒。

## 重试与重启恢复

- 上传校验成功后立即尝试第 1 次通知。
- 第 1 次失败后等待 30 秒，第 2 次失败后等待 3 分钟，总共最多 3 次。
- 连接失败、超时、非 2xx、无效 JSON、响应超过 1 MiB、业务码非 200 都作为失败处理。
- 重试由持久化调度器安排，不占用工作线程等待，不重新上传已完成的文件。同一目标同一对象键的新版本等待通知结束；其他文件和目标继续执行。
- 成功或三次耗尽后，上传任务均为 `COMMITTED`，通知结果另行保留。三次耗尽记录错误日志，不再补发，也不阻塞后续版本。

请求地址、请求体、超时、次数和下次执行时间保存在 SQLite。修改或删除 `notify` 只影响尚未准备通知的任务；已经准备的通知继续使用保存的参数。停用整个目标则暂停该目标的任务。

请求发送前记录次数。若进程在请求期间退出，该次仍计入三次上限，恢复时按保存的时间继续（预留请求超时和退避时间）。响应丢失可能导致重复通知，接收端应能处理重复；进程恰好在记录次数后、发出请求前退出，也会消耗一次机会。通知不保证恰好一次。

持续运行 `run` 会自动执行到期重试。`once` 只处理当前到期任务，未来重试需要再次执行 `once` 或启动 `run`。

## 状态与升级

`status` 增加 `notifications`，按目标与通知状态汇总：`PENDING`、`SENDING`、`RETRY_WAIT`、`SUCCEEDED`、`FAILED`。上传任务等待通知时为 `NOTIFYING` 或 `NOTIFY_RETRY_WAIT`；最终 `COMMITTED` 仅表示上传任务完成，不代表通知必定成功。

日志事件为 `notification_succeeded`、`notification_failed`、`notification_exhausted`，包含 `target`、`task`、`notification_attempt` 及错误类别 `code`，不记录原始响应内容。可结合通知状态检查最终失败；现有 `retry` 命令不重置通知次数，也不会重开已完成的任务。

首次启动自动将状态库升级至版本 4，保留上传记录，不补发历史已完成任务。旧版程序不能读取升级后的库；升级前应停服务并备份工作目录，回退需使用升级前备份。只读 `status` 兼容尚未升级的版本 3 状态库。
