# httpgo

支持断点续传的多线程、多块 HTTP/HTTPS 下载工具。使用 Go 标准库实现。

大文件会切成多个数据块，用多条连接同时下载。按 Ctrl+C 中断后，再次执行同一条命令即可从上次进度继续。

## 环境

- Go 1.22 或更高版本
- 可在 macOS、Linux 和 Windows 10 上编译运行

在 macOS 或 Linux 上编译出的程序不能直接在 Windows 上运行，需要单独生成 Windows 版 exe。

## 编译

当前系统：

```bash
go build -o httpgo .
```

Windows 10（64 位），可在 macOS 或 Linux 上交叉编译：

```bash
GOOS=windows GOARCH=amd64 go build -o httpgo.exe .
```

32 位 Windows 把 `amd64` 换成 `386`。生成的 `httpgo.exe` 拷到 Windows 后可以直接运行，不需要再安装 Go。

也可以不编译，直接运行：

```bash
go run . -c 8 -b 1MB https://example.com/file.zip
```
真实例子
```bash
edy@edydeMacBook-Pro out % ./httpgo -o abc https://lf3-static.bytednsdoc.com/obj/eden-cn/lapzild-tss/ljhwZthlaukjlkulzlp/protenix/20251217/assets/bhrf1_demo.mp4
大小: 62.1MB
分块: 8，并发: 8
保存: abc
 100.0% [========================] 62.1MB/62.1MB  7.3MB/s  eta --:--  块 8/8
完成: abc (62.1MB, 平均 7.8MB/s, 用时 8s)
```
## 用法

```bash
httpgo [选项] <url>
```

```bash
httpgo -c 16 -b 4MB -o ~/Downloads/a.iso https://example.com/a.iso
```

不指定 `-o` 时，文件名来自响应头 `Content-Disposition`，否则取 URL 路径中的文件名。`-o` 如果指向一个已存在的目录，文件会保存到该目录下。

## 选项

| 参数 | 默认值 | 说明 |
| --- | --- | --- |
| `-o` | URL 文件名 | 保存路径 |
| `-c` | `8` | 并发连接数，范围 1–128 |
| `-b` | 按并发数均分 | 分块大小，例如 `512KB`、`4MB`、`1.5MiB` |
| `-retries` | `3` | 每个分块失败后的重试次数 |
| `-timeout` | `30` | 读空闲超时（秒）。连接在这段时间内没有新数据会重试 |
| `-H` | 无 | 额外请求头，可重复，格式为 `Key: Value` |
| `-f` | 关闭 | 覆盖已存在且大小不同的目标文件 |
| `-no-resume` | 关闭 | 忽略已有进度，重新下载 |
| `-q` | 关闭 | 不显示进度 |

查看帮助：

```bash
httpgo -h
```

## 分块与并发

`-c` 是同时下载的连接数，`-b` 是每块的大小。两者可以不同。例如 `-c 8 -b 4MB` 表示每块 4MB，最多 8 条连接一起拉取这些块。

不写 `-b` 时，文件按并发数平均切开。小于 256KB 的文件默认只用一条连接；需要分块时请指定 `-b`。分块数量最多 4096，超过时会自动加大每块的大小。

服务器不支持 `Range` 时，自动改为单连接下载整个文件。

为了让多条连接真正并行，HTTPS 使用 HTTP/1.1，不走 HTTP/2 多路复用。

## 断点续传

下载过程中会在目标文件旁边写两个文件：

- `文件名.part`：已经下载的数据
- `文件名.httpgo.json`：每块的完成进度、文件大小和 ETag

按 Ctrl+C 后进度会保存，退出码为 130。再次执行同一条命令会跳过已完成的块，只下载剩余部分。远程文件的大小或 ETag 变了，会丢弃旧进度并重新下载。

目标文件已经存在，且大小与远程文件一致时，会直接跳过。大小不同时需要加 `-f` 覆盖。

代理使用环境变量 `HTTP_PROXY`、`HTTPS_PROXY` 和 `NO_PROXY`。

## 平台差异

macOS 和 Linux 上，同一个目标文件同一时间只能有一个 httpgo 在写。后启动的进程会退出。

Windows 版目前没有这个文件锁。不要用两个窗口同时下载同一个文件。

## 测试

```bash
go test ./...
```

## 代码分析

整个程序都在 `package main` 里，没有第三方依赖。入口是 `main.go`，下载逻辑在 `download.go`，进度显示在 `progress.go`。文件锁按系统拆开：`lock_unix.go` 在 macOS 和 Linux 上用 `flock`，`lock_other.go` 在 Windows 上是空实现。

### 文件职责

| 文件 | 职责 |
| --- | --- |
| `main.go` | 解析参数，监听 Ctrl+C 和 SIGTERM，调用 `Download` |
| `download.go` | 探测服务器、切分数据块、并发下载、保存进度 |
| `progress.go` | 终端进度条、速度、剩余时间和字节单位换算 |
| `lock_unix.go` | 用 `flock` 防止两个进程写同一个目标 |
| `lock_other.go` | Windows 上不加锁，保证代码能编译 |
| `download_test.go` | 分块、续传、重试、中断和跨进程锁的测试 |

### 一次下载的过程

`main` 把命令行收成 `Option`，再用 `signal.NotifyContext` 包一层上下文。Ctrl+C 会取消这个上下文，下载循环看到取消后退出。`Download` 创建 `downloader`，然后按下面的顺序执行：

```text
probe          询问文件大小，确认服务器是否支持 Range
resolvePath    决定最终路径、.part 路径和进度文件路径
alreadyHave    目标已存在且大小一致时直接跳过
prepare        读取旧进度或重新切块，打开 .part 并加锁
transfer       启动工作协程下载未完成的块
close          成功则把 .part 改名为目标文件，失败则把进度写回磁盘
```

`transfer` 结束时一定会调用 `close`。全部块完成时 `committed` 为真，`close` 把 `文件名.part` 改成最终文件，再删除 `文件名.httpgo.json`。中途失败或被取消时，`close` 先 `Sync` 数据文件，再把各块已写入的字节数写入进度文件。

### 探测是否能分块

`probe` 先发 `HEAD`。文件大小为 0 时直接创建空文件，不再发第二次请求。

大小未知或需要确认分块能力时，再发 `GET`，请求头是 `Range: bytes=0-0`：

- 返回 `206 Partial Content`：服务器支持范围请求。文件总大小从 `Content-Range` 的 `bytes 0-0/总长度` 里取。
- 返回 `200`：服务器忽略了范围请求。`mergeRemote` 把 `AcceptRanges` 设为假，后面只用一条连接下载整个文件。
- `HEAD` 已经拿到大小，但这次探测失败：同样退回单连接，避免把错误的分块结果写进文件。

探测响应体最多读 1KB 就关闭，避免 `200` 时把整个文件提前拉下来。`Content-Disposition` 里的 `filename` 和 `filename*` 用来决定默认文件名，`filename*` 会按 RFC 5987 做百分号解码。

### 怎么切块

`planChunks` 根据文件大小、`-c` 和 `-b` 生成 `[]chunk`。每块记录闭区间 `[Start, End]` 和已经写入的 `Downloaded`。

- 指定了 `-b`：从 0 开始按固定大小切，最后一块拿走余数。块数超过 4096 时加大每块尺寸。
- 没有指定 `-b`：用 `evenSplit` 切成与并发数相同的块，余数分给前面的块，所以各块长度最多差 1 字节。文件小于 256KB 时只留一块。
- 服务器没有给出总长度：只生成一块，`End` 为 `-1`，表示一直读到连接结束。

续传时不重新切块，直接使用进度文件里的区间。这样中途改 `-c` 或 `-b` 也不会让新旧区间错位。

### 多协程如何分工

`dispatch` 用一个不带缓冲的 `jobs` 通道分发块编号。工作协程数量是 `min(并发数, 未完成块数)`。一块只会被一个协程领取，所以同一块不会被两个协程同时写。

任意一块在重试耗尽后失败，`sync.Once` 记下第一个错误并取消其余协程。取消和真正的下载错误不会互相覆盖。用户按下 Ctrl+C 时，各块返回 `context.Canceled`，`main` 以此打印“进度已保存”并以退出码 130 结束。

`downloadChunk` 对可恢复错误做重试。网络错误、提前断开、读空闲超时，以及 HTTP 408、429 和 5xx 会重试。404 这类 4xx 不重试。退避从 500ms 开始，每次翻倍，最长 5 秒。

### 一块数据怎么写入

`fetch` 只请求这一块还缺的区间。例如某块是 `0–299`，已经写入 150 字节，下一次请求就是 `Range: bytes=150-299`。有 ETag 时同时带上 `If-Range`，服务器上的文件若已变化，就不会把新文件的一段写进旧数据里。

响应按状态码处理：

- `206`：核对 `Content-Range` 的起始位置和总大小，再写入。
- `200` 且当前只有一块、偏移是 0：这是整文件响应，可以接受。
- `200` 且已经有偏移：说明服务器不能从断点继续。单块下载会清空进度后重下一次；多块下载直接报错，提示改用 `-c 1 --no-resume`。

读取使用 128KB 缓冲区，并且不会读过当前块的末尾。读到的数据用 `os.File.WriteAt` 写到 `Start + 已下载字节` 这个绝对偏移。各块区间不重叠，并发 `WriteAt` 不会互相覆盖。

`Downloaded` 在 `WriteAt` 成功之后才增加。进度计数因此不会超前于已经交给操作系统的数据。进程若在写入成功、计数更新前退出，下次会把这一小段再下载一次，文件内容仍然正确。

`readWithIdle` 给每一次 `Read` 加空闲期限。超时后关闭响应体，让阻塞的读取返回，再由重试逻辑从新的偏移继续。

### 进度文件为什么可靠

下载期间 `saveLoop` 每秒调用一次 `save`。顺序是：

1. 在锁内复制每块的 `Downloaded`。
2. 对 `.part` 做 `Sync`。
3. 把这份快照写成 `文件名.httpgo.json.tmp`，再改名为正式进度文件。

先记录偏移再 `Sync`，是为了保证进度文件里的字节数不会多于内核已经接收的数据。临时文件加改名让进度文件不会停在写了一半的 JSON 上。

下次启动时 `stateProblem` 检查这份进度能不能用：地址一致，或 ETag 与大小同时一致；大小、ETag、`Last-Modified` 没有变化；各块首尾相接并覆盖整个文件；`.part` 的长度不少于已记录的最大偏移。任何一项不满足都会删掉旧进度，重新下载。

### HTTP 客户端

`newClient` 按并发数限制 `MaxConnsPerHost`。`TLSNextProto` 被设成空表，从而关闭 HTTP/2，让每个分块使用独立的 HTTP/1.1 连接。`DisableCompression` 为真，避免传输层把响应解压后破坏按字节计算的范围。TLS 最低版本是 1.2。重定向最多 10 次，并且只允许继续跳到 `http` 或 `https`。

用户传入的 `Range` 和 `If-Range` 会被忽略，这两个头由程序自己设置。

### 进度显示

`meter` 单独跑在一个协程里，每 200ms 读一次 `downloaded` 原子计数和已完成块数。速度用相邻两次采样的差值计算，剩余时间用未完成字节数除以当前速度。输出是终端时用 `\r` 覆盖同一行；输出被重定向时最多每秒打印一行，避免刷屏。
