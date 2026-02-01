# Parser Kit

[![Go Reference](https://pkg.go.dev/badge/github.com/soulteary/parser-kit.svg)](https://pkg.go.dev/github.com/soulteary/parser-kit)
[![Go Report Card](https://goreportcard.com/badge/github.com/soulteary/parser-kit)](https://goreportcard.com/report/github.com/soulteary/parser-kit)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)
[![codecov](https://codecov.io/gh/soulteary/parser-kit/graph/badge.svg)](https://codecov.io/gh/soulteary/parser-kit)

[English](README.md)

支持从多个数据源（文件、Redis、远程 HTTP）加载数据的通用数据加载器工具包，支持基于优先级的回退策略。

## 特性

- **多源支持**：从本地文件、Redis 或远程 HTTP 端点加载数据
- **基于优先级的回退**：如果前一个源失败，自动回退到下一个源
- **泛型设计**：适用于任何可 JSON 序列化的类型
- **HTTP 重试机制**：远程请求自动重试，支持指数退避
- **大小限制**：防止内存耗尽攻击
- **规范化支持**：解析后可选的数据规范化

## 安装

```bash
go get github.com/soulteary/parser-kit
```

## 使用

### 基本示例

```go
package main

import (
    "context"
    "github.com/soulteary/parser-kit"
    "github.com/soulteary/redis-kit/client"
)

type User struct {
    ID    string `json:"id"`
    Email string `json:"email"`
    Phone string `json:"phone"`
}

func main() {
    // 创建加载器
    loader, err := parserkit.NewLoader[User](nil)
    if err != nil {
        panic(err)
    }

    // 定义带优先级的源（数字越小优先级越高）
    sources := []parserkit.Source{
        {
            Type:     parserkit.SourceTypeRedis,
            Priority: 0, // 最高优先级
            Config: parserkit.SourceConfig{
                RedisKey:    "users:cache",
                RedisClient: redisClient, // *redis.Client
            },
        },
        {
            Type:     parserkit.SourceTypeRemote,
            Priority: 1,
            Config: parserkit.SourceConfig{
                RemoteURL:           "https://api.example.com/users",
                AuthorizationHeader: "Bearer token",
            },
        },
        {
            Type:     parserkit.SourceTypeFile,
            Priority: 2, // 最低优先级（回退）
            Config: parserkit.SourceConfig{
                FilePath: "/path/to/users.json",
            },
        },
    }

    // 加载数据（先尝试 Redis，然后远程，最后文件）
    ctx := context.Background()
    users, err := loader.Load(ctx, sources...)
    if err != nil {
        panic(err)
    }

    // 使用 users...
}
```

### 单独源加载

```go
// 从文件加载
users, err := loader.FromFile(ctx, "/path/to/users.json")

// 从远程加载
users, err := loader.FromRemote(ctx, "https://api.example.com/users", "Bearer token")

// 从 Redis 加载
users, err := loader.FromRedis(ctx, redisClient, "users:cache")
```

### 自定义选项

```go
opts := &parserkit.LoadOptions{
    MaxFileSize:  20 * 1024 * 1024, // 20MB
    MaxRetries:   5,
    RetryDelay:   2 * time.Second,
    HTTPTimeout:  10 * time.Second,
}

normalizeFunc := func(users []User) []User {
    // 解析后规范化数据
    for i := range users {
        // 应用规范化逻辑
    }
    return users
}

loader, err := parserkit.NewLoaderWithNormalize[User](opts, normalizeFunc)
```

## 源类型

### 文件源

从本地 JSON 文件加载数据。

```go
{
    Type: parserkit.SourceTypeFile,
    Priority: 2,
    Config: parserkit.SourceConfig{
        FilePath: "/path/to/data.json",
    },
}
```

### Redis 源

从 Redis 键加载数据（必须包含 JSON）。

```go
{
    Type: parserkit.SourceTypeRedis,
    Priority: 0,
    Config: parserkit.SourceConfig{
        RedisKey:    "data:cache",
        RedisClient: redisClient, // redis-kit 的 *redis.Client
    },
}
```

### 远程源

从远程 HTTP/HTTPS 端点加载数据。

```go
{
    Type: parserkit.SourceTypeRemote,
    Priority: 1,
    Config: parserkit.SourceConfig{
        RemoteURL:           "https://api.example.com/data",
        AuthorizationHeader: "Bearer token", // 可选
        Timeout:             5 * time.Second, // 可选，未设置则使用默认值
        InsecureSkipVerify: false,           // 可选，用于开发环境
    },
}
```

## 优先级系统

源按优先级顺序处理：
- 优先级数字越小 = 优先级越高
- 优先级 0 是最高优先级
- 如果源失败，加载器自动尝试下一个源
- 具体行为由 LoadStrategy 决定（见下）

## 加载策略

两种策略控制多源数据的组合方式：

### 回退（默认）

`LoadStrategyFallback`：返回**第一个成功源**的数据。适用于「缓存 → 远程 → 文件」式加载。

### 合并

`LoadStrategyMerge`：**合并**所有成功源的数据并去重。适用于「远程为主 + 本地补充」（如 Warden 的 REMOTE_FIRST）。需提供 `KeyFunc` 以提取每条数据的唯一键。

```go
keyFunc := func(u User) (string, bool) { return u.Phone, true } // 键, 是否纳入
opts := parserkit.DefaultLoadOptions()
opts.LoadStrategy = parserkit.LoadStrategyMerge
opts.KeyFunc = keyFunc
loader, _ := parserkit.NewLoader[User](opts)

// Load 会合并 file1 + file2；同键时后者覆盖前者
users, _ := loader.Load(ctx, sources...)
```

## 选项说明

| 选项 | 默认值 | 说明 |
|------|--------|------|
| `MaxFileSize` | 10MB | 文件/响应最大读取字节数 |
| `MaxRetries` | 3 | 远程请求重试次数 |
| `RetryDelay` | 1s | 重试间隔基准 |
| `HTTPTimeout` | 5s | 远程请求超时 |
| `InsecureSkipVerify` | false | 跳过 TLS 校验（仅开发） |
| `AllowEmptyFile` | false | 文件不存在时返回 `[]` 而非错误 |
| `AllowEmptyData` | false | 为 false 时，空源视为失败并尝试下一源 |
| `LoadStrategy` | `fallback` | `fallback` 或 `merge` |
| `KeyFunc` | nil | `merge` 时必填；`func(T) (string, bool)` |

建议使用 `DefaultLoadOptions()` 再按需覆盖字段，以保证 `MaxFileSize` 等被正确设置。

## 错误处理

- 如果所有源都失败，`Load()` 返回错误，包含最后遇到的错误
- 单独的源方法（`FromFile`、`FromRemote`、`FromRedis`）立即返回错误
- 文件未找到：默认返回错误；设置 `AllowEmptyFile: true` 可改为返回 `[]`

## 依赖

- `github.com/soulteary/http-kit` - 用于 HTTP 客户端和重试逻辑
- `github.com/redis/go-redis/v9` - 用于 Redis 操作

## 许可证

详见 LICENSE 文件。
