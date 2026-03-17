# Kafka 消息转发/重放工具

基于 Go + Sarama + Viper 的单二进制工具，支持：

- `Kafka -> Kafka` 转发（`forward`）
- `JSONL 文件 -> Kafka` 重放（`replay`）
- `jobs[]` 多任务并行运行
- 可选消息转换流水线（内置插件 + CEL 表达式）

## 1. 构建与运行

### 1.1 本地构建

```bash
go build -o kafka-redirect ./cmd/kafka-redirect
```

### 1.2 Makefile 交叉编译

```bash
make windows
make linux
```

会输出：

- `kafka-redirect-windows-amd64.exe`
- `kafka-redirect-linux-amd64`

### 1.3 启动

```bash
./kafka-redirect -c ./redirect.json
```

## 2. 配置模式

支持两种配置形式：

- 新格式：`jobs[]` 多任务（推荐）
- 旧格式：单任务（向后兼容）

### 2.1 每个 Job 的模式识别规则

- 存在 `source` 且不存在 `file-source` => `forward`
- 存在 `file-source` 且不存在 `source` => `replay`
- 两者同时存在或都不存在 => 配置错误

### 2.2 多任务失败语义

- `jobs[]` 并发执行
- 任一任务启动失败或运行失败，进程整体退出（fail-fast）

## 3. 转换机制（Transform）

### 3.1 设计概览

- 转换按 `transform.steps[]` 顺序执行（流水线）
- 使用内置插件注册表（非动态库加载）
- 脚本语言使用 CEL
- 未配置 `transform` 时，保持原样转发/重放行为

### 3.2 支持的插件

| plugin | 作用 | 必填字段 | 结果 |
|---|---|---|---|
| `drop_if` | 条件过滤消息 | `expr` | 表达式返回 `true` 时丢弃消息 |
| `rewrite_value` | 重写消息 `value` | `expr` | 表达式结果会序列化为 JSON 并写回 `value` |
| `set_headers` | 设置/覆盖 headers | `headers` | `headers` 的 value 是 CEL 表达式 |
| `set_key` | 重写消息 `key` | `expr` | 表达式结果转字符串写入 `key` |

### 3.3 CEL 上下文变量

- `key`：当前消息 key（字符串）
- `value`：当前消息 value 解析后的 JSON 值（对象/数组/基础类型）
- `headers`：`map(string,string)`
- `topic`：源 topic
- `partition`：源分区
- `offset`：源 offset
- `timestamp`：RFC3339Nano 字符串（无时间戳则为空字符串）

### 3.4 运行语义

- `forward`：先保存原始消息（若开启 `source.save.enabled`），再做转换
- `replay`：读取 JSONL 后做转换
- `drop_if` 命中：记为 `filtered`，不发送，不算错误
- 转换执行错误：记为 `transform_errors` 和 `skipped`，记录日志并继续
- 不支持热更新规则（修改配置后重启进程生效）
- 不内置 DLQ（失败仅日志+计数）

### 3.5 重要约束

- 启用转换时，消息 `value` 必须是 UTF-8 JSON；否则该消息转换失败并跳过
- `key/headers` 按 UTF-8 字符串语义参与表达式
- CEL 表达式会在任务启动阶段编译，编译失败将导致任务启动失败

## 4. 配置示例

### 4.1 多任务（`jobs[]`，含 transform）

```json
{
  "jobs": [
    {
      "name": "forward-a",
      "source": {
        "bootstrap_servers": "192.168.200.170:30901",
        "topic": "source-topic-a",
        "group_id": "redirect-forward-a",
        "initial_offset": "latest",
        "save": {
          "enabled": true,
          "path": "./data/save/a",
          "max_size": "1GB"
        }
      },
      "target": {
        "bootstrap_servers": "192.168.102.49:9092",
        "topic": "target-topic-a"
      },
      "transform": {
        "steps": [
          {
            "name": "drop-noise",
            "plugin": "drop_if",
            "expr": "value.type == \"noise\""
          },
          {
            "name": "map-value",
            "plugin": "rewrite_value",
            "expr": "{\"id\": value.id, \"ts\": value.ts, \"source\": \"redirect\"}"
          },
          {
            "name": "set-headers",
            "plugin": "set_headers",
            "headers": {
              "trace-id": "value.id",
              "source": "\"redirect\""
            }
          },
          {
            "name": "set-key",
            "plugin": "set_key",
            "expr": "value.id"
          }
        ]
      },
      "retry": {
        "max_attempts": 3,
        "backoff_ms": 500
      }
    },
    {
      "name": "replay-b",
      "file-source": {
        "file": "./data/save/a/messages-20260317-090000-0001.jsonl",
        "interval": 1000
      },
      "target": {
        "bootstrap_servers": "192.168.102.49:9092",
        "topic": "target-topic-b"
      },
      "transform": {
        "steps": [
          {
            "plugin": "drop_if",
            "expr": "value.level == \"debug\""
          }
        ]
      },
      "retry": {
        "max_attempts": 3,
        "backoff_ms": 500
      }
    }
  ]
}
```

说明：

- `name` 可选，留空自动生成 `job-<index>`
- `name` 去空格后必须唯一
- `jobs[]` 内可混合 `forward/replay`

### 4.2 单任务旧格式（仍支持）

#### Forward

```json
{
  "source": {
    "bootstrap_servers": "localhost:9092",
    "topic": "source-topic",
    "group_id": "kafka-redirect-source-topic",
    "initial_offset": "latest",
    "save": {
      "enabled": true,
      "path": "./data/save",
      "max_size": "1GB"
    }
  },
  "target": {
    "bootstrap_servers": "localhost:9092",
    "topic": "target-topic"
  },
  "retry": {
    "max_attempts": 3,
    "backoff_ms": 500
  }
}
```

#### Replay

```json
{
  "file-source": {
    "file": "./data/save/messages-20260317-090000-0001.jsonl",
    "interval": 1000
  },
  "target": {
    "bootstrap_servers": "localhost:9092",
    "topic": "target-topic"
  },
  "retry": {
    "max_attempts": 3,
    "backoff_ms": 500
  }
}
```

## 5. 字段说明

- `source.bootstrap_servers`：源 Kafka 地址，逗号分隔
- `source.topic`：源 topic
- `source.group_id`：Consumer Group ID；空时默认 `kafka-redirect-{topic}`
- `source.initial_offset`：`latest` 或 `earliest`
- `source.save.enabled`：是否保存源消息到本地
- `source.save.path`：本地保存目录
- `source.save.max_size`：保存文件总大小上限，支持 `B/KB/MB/GB`，空表示不限
- `file-source.file`：重放输入文件（JSONL）
- `file-source.interval`：每条消息发送后等待毫秒数，`0` 表示不等待
- `target.bootstrap_servers`：目标 Kafka 地址，逗号分隔
- `target.topic`：目标 topic
- `retry.max_attempts`：最大重试次数（含首次发送），默认 `3`
- `retry.backoff_ms`：重试间隔毫秒，默认 `500`

## 6. JSONL 消息格式

本地保存与重放使用统一 JSONL 结构：

```json
{"topic":"source-topic","key":"a2V5LTE=","value":"eyJpZCI6MX0=","headers":[{"key":"trace-id","value":"MTIzNDU2"}],"timestamp":"2026-03-17T09:30:00Z"}
```

说明：

- `key/value/headers[].value` 使用 Base64 编码
- `timestamp` 使用 RFC3339 格式

## 7. 日志统计

Forward 汇总包含：

- `received`
- `forwarded`
- `skipped`
- `filtered`
- `transform_errors`
- `saved`
- `save_errors`

Replay 汇总包含：

- `total`
- `sent`
- `invalid`
- `skipped`
- `filtered`
- `transform_errors`

## 8. 测试

```bash
go test ./...
```
