# Parser Kit

[![Go Reference](https://pkg.go.dev/badge/github.com/soulteary/parser-kit/v2.svg)](https://pkg.go.dev/github.com/soulteary/parser-kit/v2)
[![Go Report Card](.github/goreportcard.svg)](.github/goreportcard-report.md)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)
[![codecov](https://codecov.io/gh/soulteary/parser-kit/graph/badge.svg)](https://codecov.io/gh/soulteary/parser-kit)

[English](README.md)

通用数据加载器：按优先级从多个数据源（本地文件、Redis、远程 HTTP）读取 JSON 列表，
支持回退与合并两种策略。

## 特性

- **多源支持**：本地文件、Redis、远程 HTTP 端点，以及任何你自己实现 `Fetcher` 的源
- **基于优先级的回退**：某个源失败时自动尝试下一个
- **合并策略**：把所有成功的源合并起来，按键去重
- **泛型设计**：适用于任何可 JSON 序列化的类型，且 `LoadOptions[T]` 让 `KeyFunc` 在
  编译期就被检查
- **用到什么才链接什么**：根包不依赖标准库之外的任何东西
- **大小限制**：每个源都有字节上限，超限报 `ErrSourceTooLarge`，而不是伪装成 JSON 解析错误
- **HTTP 重试**：远程源支持带抖动的指数退避
- **规范化支持**：解析后可选的数据规范化

## 包结构

所有需要第三方客户端的东西都放进子包，程序只会链接它真正读取的那些源：

| 包 | 内容 | 代价 |
|---|---|---|
| `parser-kit/v2` | 加载器、`File`、`BytesFetcher`、`Fetcher` | 仅标准库 |
| `parser-kit/v2/redissource` | Redis 源 | `go-redis` |
| `parser-kit/v2/remotesource` | HTTP 源 | `http-kit`（以及 OpenTelemetry） |

以「只从文件加载」的程序实测，v1.8.0 对比 v2.0.0：二进制从 9,699,758 字节降到
3,952,636 字节，链接的包从 248 降到 78，它自己的 `go.sum` 从 20 个模块降到 2 个。
完整数据（包括「三种源都用」时这次拆分的实际代价）见
[CHANGELOG.md](CHANGELOG.md#200--2026-09-21)。

## 环境要求

- **Go 1.27+**（`go.mod` 声明 `go 1.27.0`）
- `github.com/redis/go-redis/v9` —— 仅当你 import `redissource` 时
- `github.com/soulteary/http-kit` —— 仅当你 import `remotesource` 时

## 安装

```bash
go get github.com/soulteary/parser-kit/v2
```

从 v1 升级？所有人的 import 路径都会变，见[升级说明](#升级说明)。

## 使用

### 基本示例

```go
package main

import (
    "context"

    "github.com/redis/go-redis/v9"
    parserkit "github.com/soulteary/parser-kit/v2"
    "github.com/soulteary/parser-kit/v2/redissource"
    "github.com/soulteary/parser-kit/v2/remotesource"
)

type User struct {
    ID    string `json:"id"`
    Email string `json:"email"`
    Phone string `json:"phone"`
}

func main() {
    loader, err := parserkit.NewLoader[User](nil)
    if err != nil {
        panic(err)
    }

    rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379"})

    remote, err := remotesource.New("https://api.example.com/users",
        remotesource.WithAuthorization("Bearer token"),
    )
    if err != nil {
        panic(err)
    }

    // 优先级数字越小越先尝试。
    users, err := loader.Load(context.Background(),
        parserkit.At(0, redissource.New(rdb, "users:cache")),
        parserkit.At(1, remote),
        parserkit.At(2, parserkit.File("/path/to/users.json")),
    )
    if err != nil {
        panic(err)
    }

    _ = users
}
```

不用 Redis、也不用远程端点的程序，两个子包都不 import，两个依赖也都不会被链接：

```go
loader, _ := parserkit.NewLoader[User](nil)
users, err := loader.Load(ctx,
    parserkit.At(0, parserkit.File("/etc/app/users.json")),
    parserkit.At(1, parserkit.BytesFetcher(builtinDefaults)),
)
```

### 单独源加载

```go
users, err := loader.LoadOne(ctx, parserkit.File("/path/to/users.json"))
users, err := loader.LoadOne(ctx, redissource.New(rdb, "users:cache"))
users, err := loader.LoadOne(ctx, remote)
```

### 自定义选项

```go
opts := &parserkit.LoadOptions[User]{
    MaxBytes:       20 * 1024 * 1024, // 20MB
    AllowEmptyData: true,
}

loader, err := parserkit.NewLoaderWithNormalize[User](opts, func(users []User) []User {
    for i := range users {
        users[i].Phone = strings.TrimSpace(users[i].Phone)
    }
    return users
})
```

超时、重试和 TLS 属于「有这些东西的那个源」，所以它们是源的选项，不是加载器的选项：

```go
remote, err := remotesource.New("https://api.example.com/users",
    remotesource.WithTimeout(10*time.Second),
    remotesource.WithRetry(remotesource.RetryPolicy{
        MaxRetries:    5,
        RetryDelay:    2 * time.Second,
        MaxRetryDelay: 30 * time.Second,
    }),
)
```

## 源类型

### 文件源

```go
parserkit.At(2, parserkit.File("/path/to/data.json"))

// 文件不存在时算作「源缺失」而不是错误，加载器会继续尝试下一个源：
parserkit.At(2, parserkit.File("/path/to/data.json").AllowMissing())
```

### Redis 源

读取存放 JSON 的 Redis 键。取值之前先用 `STRLEN` 校验大小，超限的值会被直接拒绝，
而不是先读进内存。

```go
parserkit.At(0, redissource.New(rdb, "data:cache"))
parserkit.At(0, redissource.New(rdb, "data:cache").AllowMissing())
```

`redissource.New` 接受的是 `redissource.Getter`——也就是这个源真正会发出的两条命令——
而不是某个具体的客户端类型：

```go
type Getter interface {
    Get(ctx context.Context, key string) *redis.StringCmd
    StrLen(ctx context.Context, key string) *redis.IntCmd
}
```

`*redis.Client`、`*redis.ClusterClient`、`*redis.Ring` 和 `redis.UniversalClient`
都满足它，所以集群或 Sentinel 部署用的是同一个源。客户端为 nil 时——包括接口里装着一个
nil 的 `*redis.Client`（这种情况直接调用会 panic）——会报 `redissource.ErrClientNil`。

### 远程源

```go
remote, err := remotesource.New("https://api.example.com/data",
    remotesource.WithAuthorization("Bearer token"),
    remotesource.WithHeader("X-Tenant", "acme"),
    remotesource.WithTimeout(5*time.Second),
)
```

每个 `remotesource.Fetcher` 默认自建一个 HTTP 客户端。如果多个远程源要共用一个连接池，
或者你已经配置好了 mTLS、代理、自定义 transport，把客户端建好传进去：

```go
client, err := httpkit.NewClient(&httpkit.Options{
    BaseURL: "https://api.example.com",
    Timeout: 5 * time.Second,
})
a, _ := remotesource.New("https://api.example.com/users", remotesource.WithClient(client))
b, _ := remotesource.New("https://api.example.com/groups", remotesource.WithClient(client))
```

### 远程源的安全提示

`remotesource` **原样**使用你给的 URL，并把 `WithAuthorization` 的值作为
`Authorization` 头发出去。`New` 只检查 URL 是不是一个绝对的 `http`/`https` URL，
仅此而已——所以由调用方控制的 URL 仍然会让它变成一个 SSRF 原语，把你的凭据转发到
URL 指向的任何地方。

传进来之前先校验 URL，并在拨号层关掉 DNS rebinding 的窗口：

```go
import "github.com/soulteary/cli-kit/validator"

opts := &validator.URLOptions{AllowedSchemes: []string{"https"}}
if err := validator.ValidateURL(remoteURL, opts); err != nil {
    return err
}
// 并在获取它的 transport 上使用 validator.SSRFDialControl(opts)，
// 通过 remotesource.WithClient 传入
```

`remotesource.WithInsecureSkipVerify()` 会完全关闭 TLS 校验——仅限开发环境。

### 自己写一个源

源就是任何带 `Fetch` 方法的类型。`ReadLimited` 会按内置源的同一套规则执行字节上限——
它会多读一个字节，因此超限的源会报 `ErrSourceTooLarge`，而不是被悄悄截断成一个
JSON 语法错误。

```go
type s3Source struct {
    bucket, key string
    client      *s3.Client
}

func (s s3Source) Fetch(ctx context.Context, maxBytes int64) ([]byte, error) {
    out, err := s.client.GetObject(ctx, &s3.GetObjectInput{
        Bucket: &s.bucket, Key: &s.key,
    })
    if err != nil {
        return nil, err
    }
    defer out.Body.Close()

    raw, err := parserkit.ReadLimited(out.Body, maxBytes)
    if err != nil {
        return nil, fmt.Errorf("s3://%s/%s: %w", s.bucket, s.key, err)
    }
    return raw, nil
}

users, err := loader.Load(ctx, parserkit.At(0, s3Source{...}))
```

返回「零字节且无错误」表示这个源缺失；加载器把它当成空结果，之后由 `AllowEmptyData`
决定怎么处理。

## 优先级系统

源按优先级顺序处理：

- 优先级数字越小 = 优先级越高
- 优先级 0 是最高优先级
- 如果源失败，加载器自动尝试下一个源
- 具体行为由 `LoadStrategy` 决定（见下）

排序是**稳定的**，因此优先级相同的两个源会保持你传入时的顺序——相同输入总是得到相同
顺序。

## 加载策略

### 回退（默认）

`LoadStrategyFallback`：返回**第一个成功源**的数据。适用于「缓存 → 远程 → 文件」式加载。

### 合并

`LoadStrategyMerge`：**合并**所有成功源的数据并按 `KeyFunc` 去重。某个键第一次出现的
位置就固定了，后面的源只会覆盖它的值。

```go
opts := parserkit.DefaultLoadOptions[User]()
opts.LoadStrategy = parserkit.LoadStrategyMerge
opts.KeyFunc = func(u User) (string, bool) { return u.Phone, u.Phone != "" } // 键, 是否纳入

loader, err := parserkit.NewLoader[User](opts)
users, err := loader.Load(ctx, sources...)
```

`KeyFunc` 的类型是 `KeyFunc[T]`，签名写错是调用处的编译错误，而不是 `NewLoader`
返回的运行时错误。

## 选项说明

### `LoadOptions[T]`

| 选项 | 默认值 | 说明 |
|------|--------|------|
| `MaxBytes` | 10MB | 单个源最多可以返回的字节数 |
| `AllowEmptyData` | false | 为 false 时，空源视为失败并尝试下一源 |
| `LoadStrategy` | `fallback` | `fallback` 或 `merge` |
| `KeyFunc` | nil | `merge` 时必填；`func(T) (string, bool)` |

### `remotesource` 选项

| 选项 | 默认值 | 说明 |
|------|--------|------|
| `WithTimeout(d)` | 5s | 单次请求的时限，含重试 |
| `WithRetry(RetryPolicy{MaxRetries})` | 3 | 请求失败后的重试次数 |
| `WithRetry(RetryPolicy{RetryDelay})` | 1s | 首次重试前的等待，之后逐次翻倍。**必须为正**——见下文 |
| `WithRetry(RetryPolicy{MaxRetryDelay})` | 30s | 退避上限。**必须为正**——见下文 |
| `WithAuthorization(v)` | — | `Authorization` 头，原样发送 |
| `WithHeader(k, v)` | — | 额外的请求头，可多次使用 |
| `WithUserAgent(ua)` | — | 这个源发出请求时的 `User-Agent` |
| `WithInsecureSkipVerify()` | 关 | 跳过 TLS 校验（仅开发） |
| `WithClient(c)` | — | 使用已有的 `*httpkit.Client`，不再自建 |

`RetryDelay` 和 `MaxRetryDelay` 不像通常的零值那样「可省略」。http-kit 计算
`RetryDelay × 2^attempt` 后会无条件夹到 `MaxRetryDelay`，所以**任何一个**字段为零
都意味着每次重试立即发生，失败的远程源会以网络允许的最快速度被反复请求。`New` 会把
这两个字段中非正的值替换成默认值——这也正是 `WithRetry(RetryPolicy{MaxRetries: 5})`
可以放心这么写的原因。

`NewLoader` 和 `NewLoaderWithNormalize` 作用于你 `LoadOptions` 的**副本**，因此补默认值
不会修改你传进去的那个结构体。

## 错误处理

- 如果所有源都失败，`Load` 返回错误，其中带着最后遇到的那一个。
- `LoadOne` 直接返回该源的错误。
- `Source` 里没有 `Fetcher` 时报 `ErrNoFetcher`。
- 文件缺失、Redis 键不存在默认都是错误；在对应的源上调用 `AllowMissing()` 会把它变成
  「源缺失」，加载器继续往下走。

### 源数据过大

超过 `MaxBytes` 的源会以 `ErrSourceTooLarge` 失败，所有源类型都是如此：

```go
data, err := loader.Load(ctx, sources...)
if errors.Is(err, parserkit.ErrSourceTooLarge) {
    // 源存在且可达，但超过了 MaxBytes
}
```

请用 `errors.Is` 判断，而不是检查错误文本——「过大」和「格式错误」是两个不同的问题，修法
也不同。

把 `MaxBytes` 设为 `math.MaxInt64` 表示「不限制」；那一个字节的溢出探测余量会饱和而
不是溢出。

### 空源算作失败

在 `AllowEmptyData: false`（默认）时，一个加载成功但没有任何条目的源被当作**失败**，
回退策略会继续尝试下一个源。

对白名单或黑名单来说这一点值得想清楚：**清空主数据源会回退到 Redis 或远程上的旧副本**，
你刚删掉的条目又回来了。当「空」是一个合法状态时，请设置 `AllowEmptyData: true`。

## 测试

测试无需真实 Redis，也不需要真实 HTTP 服务。用例使用
[miniredis](https://github.com/alicebob/miniredis) 和 `net/http/httptest`，本地即可
跑通全部测试与覆盖率：

```bash
go test ./...
go test -race -coverprofile=coverage.out -covermode=atomic ./...
go tool cover -func=coverage.out
```

## 依赖

根包除标准库外没有任何依赖。

- `github.com/redis/go-redis/v9` —— `redissource` 使用
- `github.com/soulteary/http-kit` —— `remotesource` 使用

仅测试依赖：`github.com/alicebob/miniredis/v2`（内存 Redis）与
`github.com/stretchr/testify`。

被移出去的两个依赖仍然保留在本模块 `go.mod` 的最低版本要求里，MVS 也仍会把这些最低
版本传递给 import 了子包的人。这次拆分去掉的，是**其他所有人**的那份要求。

## 升级说明

### v2.0.0

**所有人的 import 路径都要改**，包括只读文件的程序——因为 Go 把主版本号编码在模块
路径里：

```go
import parserkit "github.com/soulteary/parser-kit/v2"
```

Redis 源和 HTTP 源被移进了子包，这样根包才能不再 import go-redis 和 http-kit。
保留废弃的兼容 shim 不是一个选项：`FromRedis` 的 shim 必须 import go-redis，
于是它又被重新链接进来，收益全部归零。

| v1 | v2 |
|---|---|
| `loader.FromFile(ctx, path)` | `loader.LoadOne(ctx, parserkit.File(path))` |
| `loader.FromRedis(ctx, client, key)` | `loader.LoadOne(ctx, redissource.New(client, key))` |
| `loader.FromRemote(ctx, url, auth)` | `loader.LoadOne(ctx, remotesource.New(url, remotesource.WithAuthorization(auth)))` |
| `Source{Type: SourceTypeFile, Priority: n, Config: SourceConfig{FilePath: p}}` | `parserkit.At(n, parserkit.File(p))` |
| `Source{Type: SourceTypeRedis, …RedisClient: c, RedisKey: k}` | `parserkit.At(n, redissource.New(c, k))` |
| `Source{Type: SourceTypeRemote, …RemoteURL: u, AuthorizationHeader: a}` | 先 `remotesource.New(u, remotesource.WithAuthorization(a))`，再 `parserkit.At(n, remote)` |
| `LoadOptions` | `LoadOptions[T]` |
| `DefaultLoadOptions()` | `DefaultLoadOptions[T]()` |
| `LoadOptions.KeyFunc interface{}` | `LoadOptions[T].KeyFunc KeyFunc[T]` |
| `LoadOptions.MaxFileSize` | `LoadOptions[T].MaxBytes` |
| `LoadOptions.AllowEmptyFile` | `parserkit.File(path).AllowMissing()` |
| `LoadOptions.HTTPTimeout` | `remotesource.WithTimeout(d)` |
| `LoadOptions.MaxRetries` / `.RetryDelay` / `.MaxRetryDelay` | `remotesource.WithRetry(remotesource.RetryPolicy{…})` |
| `LoadOptions.InsecureSkipVerify` | `remotesource.WithInsecureSkipVerify()` |

v2 里另外几处容易被忽略的变化：

- **`RetryDelay` 为零不再意味着「立即重试」。** v1.7.0 修的是 `MaxRetryDelay`，
  旁边这个字段上同样的洞留到了现在——`&LoadOptions{MaxRetries: 3}` 会以网络允许的
  最快速度反复请求。
- **零字节的源现在解析成空列表**，而不是「unexpected end of JSON input」。在默认的
  `AllowEmptyData: false` 下，回退链的行为和以前完全一致。
- **`redissource` 接受任意形态的 go-redis 客户端。** v1 断言
  `client.(*redis.Client)`，把 `*redis.ClusterClient` 在运行时挡了回去。

### v1.8.0

仅升级依赖。没有删除任何 API，调用方无需改代码。

- 测试用 Redis 为 `miniredis` v2.39.0（此前 v2.36.1）。
- 间接依赖 `yuin/gopher-lua` 升至 v1.1.2（此前 v1.1.1），与其他 kit 对齐。
- 仍依赖 `http-kit` v1.5.0。该模块没有更新的已发布版本。

### v1.7.0

- **远程重试恢复退避。** `FromRemote` 从未设置 `MaxRetryDelay`，而 http-kit 会把每个
  算出来的退避时间**无条件**夹到这个上限——所以上限为零会让三次重试全部立即发生，
  把一个失败的源变成一个高频请求循环。在默认配置下，现在的间隔是 1s、2s、4s，而不是
  完全没有。**原本"快速失败"的远程源，现在要几秒钟才会把重试跑完**；如果你依赖原来的
  时序，请调低 `RetryDelay` 或 `MaxRetries`。
- **新增 `LoadOptions.MaxRetryDelay`**（默认 30s）。`NewLoader` 和
  `NewLoaderWithNormalize` 在你留空时会把它填上，原因同上：你传入的零会被替换而不是被
  采纳，因为它的含义是"不退避"，而不是"不设上限"。要换一个上限，请显式设置。

### v1.6.0

新增了一个哨兵错误；没有删除任何东西。两条错误路径的报错方式变了。

- **源数据过大现在报 `ErrSourceTooLarge`，而不再伪装成数据格式错误。**
  `io.LimitReader` 会静默截断，所以超过 `MaxFileSize` 的文件或 HTTP 响应过去会以
  "invalid JSON" 的形式返回，无法区分这两种情况。现在这两条路径都会多读一个字节并报出
  哨兵错误——这与 `FromRedis` 一致，它本来就会先用 `STRLEN` 检查，于是同一个条件下三种源的
  表现原本是不一样的。**如果你曾经靠 "invalid JSON" 来判断源过大，请改用
  `errors.Is(err, ErrSourceTooLarge)`。**
- **`FromRedis` 现在包装哨兵错误。** 它此前用普通格式化错误报告超限的值，导致
  `errors.Is(err, ErrSourceTooLarge)` 对这个"唯一一直能检测到该条件"的源反而为 false。
- **`MaxFileSize: math.MaxInt64` 现在可用。** 那一个字节的溢出探测余量
  （`MaxFileSize+1`）会回绕成 `math.MinInt64`，`io.LimitReader` 立即返回 EOF，于是
  **任何源——无论多小——都会被报成 JSON 格式错误**。现在这个上限会饱和。
- **源顺序是稳定的。** 此前用 `sort.Slice` 排序，优先级相同的两个源会得到任意顺序，
  相同输入在不同运行之间也可能不同。
- **`NewLoader`/`NewLoaderWithNormalize` 不再修改你的 `LoadOptions`。**
  它们此前会把默认值写回调用方的结构体。
- **只是写进文档、行为未变**：`AllowEmptyData` 为 false 时，一个成功但为空的源算作失败
  ——见[空源算作失败](#空源算作失败)——以及 `FromRemote` 对调用方可控的 URL 是一个 SSRF 原语。

## 许可证

Apache License 2.0 —— 详见 [LICENSE](LICENSE)。
