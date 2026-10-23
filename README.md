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
