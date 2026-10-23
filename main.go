package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		if errors.Is(err, context.Canceled) {
			fmt.Fprintln(os.Stderr, "已暂停，进度已保存。再次执行相同命令即可续传。")
			os.Exit(130)
		}
		fmt.Fprintf(os.Stderr, "错误: %v\n", err)
		os.Exit(1)
	}
}

func run(args []string) error {
	fs := flag.NewFlagSet("httpgo", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	fs.Usage = func() { usage(fs) }

	var (
		output      string
		concurrency int
		block       string
		retries     int
		timeoutSec  int
		force       bool
		noResume    bool
		quiet       bool
		headers     headerList
	)
	fs.StringVar(&output, "o", "", "保存路径。若是已存在的目录，则使用 URL 中的文件名")
	fs.IntVar(&concurrency, "c", 8, "并发连接数，范围 1-128")
	fs.StringVar(&block, "b", "", "分块大小，例如 4MB。默认按并发数平均切分")
	fs.IntVar(&retries, "retries", 3, "每个分块失败后的重试次数")
	fs.IntVar(&timeoutSec, "timeout", 30, "读空闲超时（秒）")
	fs.Var(&headers, "H", "额外请求头，可重复。格式为 \"Key: Value\"")
	fs.BoolVar(&force, "f", false, "覆盖已存在的目标文件")
	fs.BoolVar(&noResume, "no-resume", false, "忽略已有进度，重新下载")
	fs.BoolVar(&quiet, "q", false, "不显示进度")

	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}
		return err
	}
	if fs.NArg() != 1 {
		usage(fs)
		return errors.New("需要一个 http 或 https 地址")
	}
	if concurrency < 1 || concurrency > maxConns {
		return fmt.Errorf("并发数需在 1 到 %d 之间", maxConns)
	}
	if retries < 0 {
		return errors.New("重试次数不能为负")
	}
	if timeoutSec < 0 {
		return errors.New("超时不能为负")
	}
	var blockSize int64
	if block != "" {
		n, err := parseSize(block)
		if err != nil {
			return err
		}
		blockSize = n
	}
	idle := 30 * time.Second
	if timeoutSec > 0 {
		idle = time.Duration(timeoutSec) * time.Second
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	return Download(ctx, Option{
		URL:         fs.Arg(0),
		Output:      output,
		Concurrency: concurrency,
		BlockSize:   blockSize,
		Retries:     retries,
		IdleTimeout: idle,
		Headers:     append([]string(nil), headers...),
		Force:       force,
		NoResume:    noResume,
		Quiet:       quiet,
	})
}

func usage(fs *flag.FlagSet) {
	out := fs.Output()
	if out == nil {
		out = io.Discard
	}
	fmt.Fprintf(out, `httpgo 是支持断点续传的多线程、多块 HTTPS 下载工具。

用法:
  httpgo [选项] <url>

说明:
  大文件会切成多个数据块，用多条连接同时下载。
  未完成的数据在目标文件旁的 .part 中，进度在 .httpgo.json 中。
  中断后执行同一命令即可继续。代理读取 HTTP_PROXY、HTTPS_PROXY 和 NO_PROXY。

选项:
`)
	fs.PrintDefaults()
	fmt.Fprintf(out, `
示例:
  httpgo -c 16 -b 4MB -o ~/Downloads/a.iso https://example.com/a.iso
`)
}

type headerList []string

func (h *headerList) String() string { return "" }

func (h *headerList) Set(v string) error {
	*h = append(*h, v)
	return nil
}
