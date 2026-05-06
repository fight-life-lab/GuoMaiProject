# One-API 日志同步接口文档

本文档描述了用于同步 One-API 数据库日志的两个主要接口：全量同步接口（可选限流）和日更增量同步接口。

## 基础信息

- **接口服务地址**: `http://60.217.65.245:30076`
- **认证方式**: 统一使用 URL 查询参数 `token` 进行鉴权。请求时必须携带该参数。
- **全局 Token**: `one_api_600640`

---

## 1. 全量数据同步接口

此接口用于获取所有历史日志数据，支持通过 `limit` 参数限制返回条数，以防止数据量过大导致内存溢出或超时。

- **URL路径**: `/api/sync_all`
- **请求方式**: `GET`

### 请求参数 (Query Parameters)

| 参数名 | 必填 | 类型 | 默认值 | 说明 |
| --- | --- | --- | --- | --- |
| `token` | 是 | String | 无 | 鉴权秘钥，必须传入 `one_api_600640` |
| `limit` | 否 | Integer | 无 | 可选参数。限制返回的数据条数，例如传入 `100` 表示只获取前 100 条。如果不传该参数，则返回数据库中所有数据。 |

### 请求示例

**获取全部数据（无限制，慎用）：**
```bash
curl "http://60.217.65.245:30076/api/sync_all?token=one_api_600640"
```

**限制只获取前 2 条数据（推荐测试用）：**
```bash
curl "http://60.217.65.245:30076/api/sync_all?token=one_api_600640&limit=2"
```

### 响应数据

```json
{
    "status": "success",
    "mode": "sync_all_with_limit_1000", // 若不传 limit，此处为 "sync_all_unlimited"
    "count": 1000,
    "data": [
        {
            "id": 1,
            "created_at": 1690000000,
            "user_id": 101,
            "token_name": "test-token",
            "model_name": "gpt-3.5-turbo",
            "quota": 1500,
            "content": "..."
        },
        // ... (剩余数据)
    ]
}
```

---

## 2. 日更日志拉取接口

此接口主要用于每天定时任务（Cron Job）拉取指定日期（通常是前一天 00:00:00 到 23:59:59）的日志数据。

- **URL路径**: `/api/daily_log`
- **请求方式**: `GET`

### 请求参数 (Query Parameters)

| 参数名 | 必填 | 类型 | 默认值 | 说明 |
| --- | --- | --- | --- | --- |
| `token` | 是 | String | 无 | 鉴权秘钥，必须传入 `one_api_600640` |
| `date` | 否 | String | `昨天` | 目标日期，格式为 `YYYY-MM-DD`（如：`2023-10-01`）。若不填，默认自动拉取服务器时间的前一天数据。 |

### 请求示例

**拉取昨天一整天的所有日志（默认方式）：**
```bash
curl "http://60.217.65.245:30076/api/daily_log?token=one_api_600640"
```

**拉取指定日期（如 2023年10月1日）的日志：**
```bash
curl "http://60.217.65.245:30076/api/daily_log?token=one_api_600640&date=2023-10-01"
```

### 响应数据

```json
{
    "status": "success",
    "mode": "daily_log",
    "target_date": "2023-10-01",
    "time_range": "2023-10-01 00:00:00 -> 23:59:59",
    "count": 256,
    "data": [
        {
            "id": 1050,
            "created_at": 1696118400,
            // ... (其他字段)
        },
        // ...
    ]
}
```

---

## 错误代码说明

当请求出错时，接口会返回对应状态码及说明。

| HTTP状态码 | 错误说明 | 可能原因 |
| --- | --- | --- |
| `403 Forbidden` | `Forbidden: Wrong Token` | URL 中没有包含 `token`，或者 `token` 不正确。 |
| `404 Not Found` | `Not Found: Use /api/sync_all or /api/daily_log` | 路径错误，使用了不支持的接口路由。 |
| `500 Server Error` | `Server Error: <Exception Msg>` | 服务器内部错误，如数据库无权限读取、路径错误等。 |