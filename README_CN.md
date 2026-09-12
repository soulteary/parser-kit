# Parser Kit

[![Go Reference](https://pkg.go.dev/badge/github.com/soulteary/parser-kit.svg)](https://pkg.go.dev/github.com/soulteary/parser-kit)
[![Go Report Card](.github/goreportcard.svg)](.github/goreportcard-report.md)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)
[![codecov](https://codecov.io/gh/soulteary/parser-kit/graph/badge.svg)](https://codecov.io/gh/soulteary/parser-kit)

[English](README.md)

支持从多个数据源（文件、Redis、远程 HTTP）加载数据的通用数据加载器工具包，支持基于优先级的回退策略。

## 特性

- **多源支持**：从本地文件、Redis 或远程 HTTP 端点加载数据
- **基于优先级的回退**：如果前一个源失败，自动回退到下一个源
- **泛型设计**：适用于任何可 JSON 序列化的类型
- **HTTP 重试机制**：远程请求自动重试，支持指数退避
- **大小限制**：防止内存耗尽攻击（文件/远程；Redis 也会基于 MaxFileSize 做大小校验）
- **规范化支持**：解析后可选的数据规范化

## 环境要求

- **Go 1.27+**（`go.mod` 声明 `go 1.27.0`）
- Redis 源需要 `github.com/redis/go-redis/v9`

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
请确保 `RemoteURL` 可信或在调用方做校验，以避免 SSRF 风险。

```go
{
    Type: parserkit.SourceTypeRemote,
    Priority: 1,
    Config: parserkit.SourceConfig{
        RemoteURL:           "https://api.example.com/data",
        AuthorizationHeader: "Bearer token", // 可选
        Timeout:             5 * time.Second, // 可选，未设置则使用默认值
    },
}
// 说明：InsecureSkipVerify 在创建 Loader 时通过 LoadOptions 设置，非按源配置。
```
> 说明：`InsecureSkipVerify` 仅在创建 loader 时通过 `LoadOptions` 生效；单个 source 的该字段会被忽略，如需不同 TLS 行为请创建不同 loader。

### 远程源的安全提示

`FromRemote` **原样**使用传入的 URL，并把 `auth` 作为 `Authorization` 头发送，不做任何
校验。因此一个调用方可控的 URL 会让它变成一个 SSRF 原语，并把你的凭证转发到 URL 指向的
任何地方。

请在传入之前校验 URL，并在拨号层面关掉 DNS 重绑定窗口：

```go
import "github.com/soulteary/cli-kit/validator"

opts := &validator.URLOptions{AllowedSchemes: []string{"https"}}
if err := validator.ValidateURL(remoteURL, opts); err != nil {
    return err
}
// 并在获取它的 transport 上使用 validator.SSRFDialControl(opts)
```

`InsecureSkipVerify` 会完全关闭 TLS 校验——仅限开发环境。

## 优先级系统

源按优先级顺序处理：
- 优先级数字越小 = 优先级越高
- 优先级 0 是最高优先级
- 如果源失败，加载器自动尝试下一个源
- 具体行为由 LoadStrategy 决定（见下）

排序是**稳定的**，因此优先级相同的两个源会保持你传入时的顺序——相同输入总是得到相同
顺序。

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
`MaxFileSize` 也会在读取 Redis 时用于大小校验。

## 错误处理

- 如果所有源都失败，`Load()` 返回错误，其中带着最后遇到的那一个。
- 单独的源方法（`FromFile`、`FromRemote`、`FromRedis`）会立即返回错误。
- 文件未找到默认是错误；`AllowEmptyFile: true` 则返回 `[]`。

### 源数据过大

超过 `MaxFileSize` 的源会以 `ErrSourceTooLarge` 失败，三种源类型都是如此：

```go
data, err := loader.Load(ctx, sources...)
if errors.Is(err, parserkit.ErrSourceTooLarge) {
    // 源存在且可达，但超过了 MaxFileSize
}
```

请用 `errors.Is` 判断，而不是检查错误文本——"过大"和"格式错误"是两个不同的问题，修法
也不同。

把 `MaxFileSize` 设为 `math.MaxInt64` 表示"不限制"；那一个字节的溢出探测余量会饱和而
不是溢出。

### 空源算作失败

在 `AllowEmptyData: false`（默认）时，一个加载成功但没有任何条目的源被当作**失败**，
回退策略会继续尝试下一个源。

对白名单或黑名单来说这一点值得想清楚：**清空主数据源会回退到 Redis 或远程上的旧副本**，
你刚删掉的条目又回来了。当"空"是一个合法状态时，请设置 `AllowEmptyData: true`。

## 测试

测试无需真实 Redis。用例通过 [miniredis](https://github.com/alicebob/miniredis) 模拟 Redis，本地即可跑通全部测试与覆盖率：

```bash
go test ./...
go test -coverprofile=coverage.out -covermode=atomic ./...
go tool cover -func=coverage.out
```

## 依赖

- `github.com/soulteary/http-kit` - 用于 HTTP 客户端和重试逻辑
- `github.com/redis/go-redis/v9` - 用于 Redis 操作

仅测试依赖：`github.com/alicebob/miniredis/v2`（测试用内存 Redis）。

## 升级说明（v1.6.0）

新增一个哨兵错误，没有删除任何东西。两条错误路径的报告方式变了。

- **源数据过大现在报告为 `ErrSourceTooLarge`，而不是格式错误。** `io.LimitReader`
  是静默截断的，于是超过 `MaxFileSize` 的文件或 HTTP 响应会以"非法 JSON"的形式返回，
  无法区分这两种情况。现在两条路径都会多读一个字节并报告该哨兵错误——与一开始就用
  `STRLEN` 检查的 `FromRedis` 对齐，此前三种源对同一个状况的表现各不相同。
  **如果你是靠匹配"非法 JSON"来识别过大的源，请改用
  `errors.Is(err, ErrSourceTooLarge)`。**
- **`FromRedis` 会包装该哨兵错误。** 它此前用普通格式化错误报告超大值，于是对那个
  一直都能检测到此状况的源，`errors.Is(err, ErrSourceTooLarge)` 反而为假。
- **`MaxFileSize: math.MaxInt64` 现在可用。** 那一个字节的探测余量
  （`MaxFileSize+1`）会回绕成 `math.MinInt64`，`io.LimitReader` 立即返回 EOF，于是
  **所有源——无论多小——都返回"非法 JSON"**。现在该上限会饱和。
- **源排序是稳定的。** 此前用 `sort.Slice` 排序，优先级相同的两个源会取一个任意顺序，
  在相同输入上每次运行都可能不同。
- **`NewLoader`/`NewLoaderWithNormalize` 不再修改你的 `LoadOptions`。** 它们此前把
  默认值写回了调用方的结构体。
- **只是补充文档、行为未变**：`AllowEmptyData` 为 false 时，"成功但为空"的源算作失败
  ——见[空源算作失败](#空源算作失败)——以及对调用方可控的 URL 来说，`FromRemote` 是一个
  SSRF 原语。

## 许可证

Apache License 2.0 —— 详见 [LICENSE](LICENSE)。
