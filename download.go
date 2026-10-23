package main

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"net"
	"net/http"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

const (
	maxConns     = 128
	maxChunks    = 4096
	autoMultiMin = 256 * 1024
	copyBufSize  = 128 * 1024
	partSuffix   = ".part"
	stateSuffix  = ".httpgo.json"
	userAgent    = "httpgo/1.0"
)

// Option 控制一次下载。
type Option struct {
	URL         string
	Output      string
	Concurrency int
	BlockSize   int64
	Retries     int
	IdleTimeout time.Duration
	Headers     []string
	Force       bool
	NoResume    bool
	Quiet       bool
	Client      *http.Client
	Stderr      io.Writer
}

// Download 下载 URL 指向的文件。支持范围请求时按块并发，并把进度写到磁盘。
func Download(ctx context.Context, opt Option) error {
	if err := opt.validate(); err != nil {
		return err
	}
	if opt.IdleTimeout <= 0 {
		opt.IdleTimeout = 30 * time.Second
	}
	d := &downloader{
		opt:     opt,
		client:  opt.Client,
		printer: newPrinter(opt.Stderr),
	}
	if d.client == nil {
		d.client = newClient(opt.Concurrency)
	}
	return d.run(ctx)
}

// validate 检查地址、并发数、分块大小、重试次数和请求头是否合法。
func (o Option) validate() error {
	if err := validateURL(o.URL); err != nil {
		return err
	}
	if o.Concurrency < 1 || o.Concurrency > maxConns {
		return fmt.Errorf("并发数需在 1 到 %d 之间", maxConns)
	}
	if o.BlockSize < 0 {
		return errors.New("分块大小不能为负")
	}
	if o.Retries < 0 {
		return errors.New("重试次数不能为负")
	}
	for _, h := range o.Headers {
		if err := checkHeader(h); err != nil {
			return err
		}
	}
	return nil
}

// validateURL 只接受带主机名的 http 或 https 地址。
func validateURL(raw string) error {
	u, err := url.Parse(raw)
	if err != nil {
		return fmt.Errorf("无效地址: %w", err)
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return errors.New("仅支持 http 和 https")
	}
	if u.Host == "" {
		return errors.New("地址缺少主机名")
	}
	return nil
}

type downloader struct {
	opt         Option
	client      *http.Client
	printer     *printer
	meta        remote
	finalPath   string
	partPath    string
	statePath   string
	file        *os.File
	chunks      []chunk
	note        string
	mu          sync.Mutex
	saveMu      sync.Mutex
	downloaded  atomic.Int64
	initialDone int64
	opened      bool
	closed      bool
	committed   bool
	restarted   bool
	started     time.Time
	stopProg    context.CancelFunc
	progWG      sync.WaitGroup
}

type remote struct {
	Status       int
	Size         int64
	AcceptRanges bool
	ETag         string
	LastModified string
	Filename     string
	FinalURL     string
}

type chunk struct {
	Start      int64 `json:"start"`
	End        int64 `json:"end"`
	Downloaded int64 `json:"downloaded"`
	Done       bool  `json:"done"`
}

// length 返回分块的字节数。区间无效时返回 -1。
func (c chunk) length() int64 {
	if c.End < c.Start {
		return -1
	}
	return c.End - c.Start + 1
}

// complete 判断分块是否已经下完。未知长度的块以 Done 为准。
func (c chunk) complete() bool {
	if c.End >= c.Start {
		return c.Downloaded >= c.length()
	}
	return c.Done
}

type savedState struct {
	URL          string  `json:"url"`
	ETag         string  `json:"etag"`
	LastModified string  `json:"last_modified"`
	Size         int64   `json:"size"`
	Chunks       []chunk `json:"chunks"`
}

type span struct {
	start int64
	end   int64
}

// run 依次探测、确定路径、准备文件并传输。目标已齐或文件为空时提前结束。
func (d *downloader) run(ctx context.Context) error {
	if err := d.probe(ctx); err != nil {
		return err
	}
	if err := d.resolvePath(); err != nil {
		return err
	}
	if d.opt.NoResume {
		_ = os.Remove(d.statePath)
		_ = os.Remove(d.partPath)
	}
	skip, err := d.alreadyHave()
	if err != nil || skip {
		return err
	}
	if d.meta.Size == 0 {
		return d.writeEmpty()
	}
	if err := d.prepare(); err != nil {
		_ = d.close()
		return err
	}
	return d.transfer(ctx)
}

// probe 用 HEAD 和一次 Range GET 确认文件大小，以及服务器是否支持范围请求。
func (d *downloader) probe(ctx context.Context) error {
	pctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	var head remote
	gotHead := false
	if h, err := d.probeOnce(pctx, http.MethodHead, ""); err == nil && h.Status >= 200 && h.Status < 300 {
		head = h
		gotHead = true
		if h.Size == 0 {
			d.meta = h
			d.meta.AcceptRanges = false
			return nil
		}
	}

	got, err := d.probeOnce(pctx, http.MethodGet, "bytes=0-0")
	if err != nil {
		if gotHead && head.Size > 0 {
			head.AcceptRanges = false
			d.meta = head
			return nil
		}
		return err
	}
	if got.Status != http.StatusOK && got.Status != http.StatusPartialContent {
		if gotHead && head.Size > 0 {
			head.AcceptRanges = false
			d.meta = head
			return nil
		}
		return fmt.Errorf("探测失败: HTTP %d", got.Status)
	}
	d.meta = mergeRemote(head, got)
	if d.meta.FinalURL == "" {
		d.meta.FinalURL = d.opt.URL
	}
	return nil
}

// probeOnce 发一次探测请求，并丢弃最多 1KB 响应体。
func (d *downloader) probeOnce(ctx context.Context, method, rangeHeader string) (remote, error) {
	req, err := http.NewRequestWithContext(ctx, method, d.opt.URL, nil)
	if err != nil {
		return remote{}, err
	}
	if err := applyHeaders(req, d.opt.Headers); err != nil {
		return remote{}, err
	}
	if rangeHeader != "" {
		req.Header.Set("Range", rangeHeader)
	}
	resp, err := d.client.Do(req)
	if err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return remote{}, err
		}
		return remote{}, fmt.Errorf("无法访问 %s: %w", d.opt.URL, err)
	}
	defer resp.Body.Close()
	meta := remoteFrom(resp)
	_, _ = io.CopyN(io.Discard, resp.Body, 1024)
	return meta, nil
}

// remoteFrom 从响应头提取大小、ETag、文件名，以及是否支持范围请求。
func remoteFrom(resp *http.Response) remote {
	m := remote{
		Status:       resp.StatusCode,
		Size:         -1,
		ETag:         resp.Header.Get("ETag"),
		LastModified: resp.Header.Get("Last-Modified"),
		Filename:     filenameFromDisposition(resp.Header.Get("Content-Disposition")),
	}
	if resp.Request != nil && resp.Request.URL != nil {
		m.FinalURL = resp.Request.URL.String()
	}
	if resp.StatusCode == http.StatusPartialContent {
		m.AcceptRanges = true
		if cr := resp.Header.Get("Content-Range"); cr != "" {
			_, _, total, hasTotal, err := parseContentRange(cr)
			if err == nil && hasTotal {
				m.Size = total
			}
		}
		return m
	}
	if resp.ContentLength >= 0 {
		m.Size = resp.ContentLength
	}
	if strings.Contains(strings.ToLower(resp.Header.Get("Accept-Ranges")), "bytes") {
		m.AcceptRanges = true
	}
	return m
}

// mergeRemote 合并 HEAD 与 Range GET 的结果。GET 返回 200 时视为不支持分块。
func mergeRemote(head, got remote) remote {
	out := head
	out.Status = got.Status
	if got.FinalURL != "" {
		out.FinalURL = got.FinalURL
	}
	if out.FinalURL == "" {
		out.FinalURL = head.FinalURL
	}
	if got.ETag != "" {
		out.ETag = got.ETag
	}
	if got.LastModified != "" {
		out.LastModified = got.LastModified
	}
	if got.Filename != "" {
		out.Filename = got.Filename
	}
	if got.Status == http.StatusPartialContent {
		out.AcceptRanges = true
		if got.Size >= 0 {
			out.Size = got.Size
		}
		return out
	}
	out.AcceptRanges = false
	if got.Size >= 0 {
		out.Size = got.Size
	}
	return out
}

// resolvePath 决定最终文件、.part 和进度文件的路径，并创建所需目录。
func (d *downloader) resolvePath() error {
	name := d.meta.Filename
	if name == "" {
		raw := d.meta.FinalURL
		if raw == "" {
			raw = d.opt.URL
		}
		name = filenameFromURL(raw)
	}
	rawOut := d.opt.Output
	asDir := rawOut != "" && (strings.HasSuffix(rawOut, "/") || strings.HasSuffix(rawOut, string(os.PathSeparator)))
	out := rawOut
	if out == "" {
		out = name
	} else if asDir {
		if err := os.MkdirAll(filepath.Clean(out), 0o755); err != nil {
			return err
		}
		out = filepath.Join(out, name)
	} else if st, err := os.Stat(out); err == nil && st.IsDir() {
		out = filepath.Join(out, name)
	}
	out = filepath.Clean(out)
	if out == "." {
		out = name
	}
	if err := os.MkdirAll(filepath.Dir(out), 0o755); err != nil {
		return err
	}
	d.finalPath = out
	d.partPath = out + partSuffix
	d.statePath = out + stateSuffix
	return nil
}

// alreadyHave 在目标已存在且大小一致时跳过下载。大小不同且未指定 -f 则报错。
func (d *downloader) alreadyHave() (bool, error) {
	info, err := os.Stat(d.finalPath)
	if err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, err
	}
	if info.IsDir() {
		return false, fmt.Errorf("保存路径是目录: %s", d.finalPath)
	}
	if !d.opt.NoResume {
		if _, err := os.Stat(d.partPath); err == nil {
			return false, nil
		}
	}
	if d.opt.Force {
		return false, nil
	}
	if d.meta.Size >= 0 && info.Size() == d.meta.Size {
		_ = os.Remove(d.statePath)
		d.printf("已存在相同大小的文件，跳过: %s", d.finalPath)
		return true, nil
	}
	return false, fmt.Errorf("目标文件已存在，使用 -f 覆盖: %s", d.finalPath)
}

// writeEmpty 创建空的目标文件，并清掉残留的进度。
func (d *downloader) writeEmpty() error {
	if err := os.WriteFile(d.finalPath, nil, 0o644); err != nil {
		return err
	}
	_ = os.Remove(d.partPath)
	_ = os.Remove(d.statePath)
	d.printf("完成: %s (0B)", d.finalPath)
	return nil
}

// prepare 读取可用的旧进度或重新切块，打开 .part 并加锁。
func (d *downloader) prepare() error {
	resumed := false
	if !d.opt.NoResume {
		if st, err := loadState(d.statePath); err == nil {
			if problem := d.stateProblem(st); problem == "" {
				d.chunks = append([]chunk(nil), st.Chunks...)
				resumed = true
			} else {
				d.warn("已有进度不可用（%s），重新下载", problem)
				_ = os.Remove(d.statePath)
				_ = os.Remove(d.partPath)
			}
		} else if !os.IsNotExist(err) {
			d.warn("进度文件无法读取，重新下载")
			_ = os.Remove(d.partPath)
		} else if _, statErr := os.Stat(d.partPath); statErr == nil {
			d.warn("发现未完成文件但没有进度，重新下载")
			_ = os.Remove(d.partPath)
		}
	}
	if !resumed {
		d.chunks, d.note = planChunks(d.meta.Size, d.opt.Concurrency, d.opt.BlockSize)
	}

	f, err := os.OpenFile(d.partPath, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return err
	}
	if err := lockFile(f); err != nil {
		f.Close()
		return err
	}
	d.file = f
	if d.meta.Size > 0 {
		if err := f.Truncate(d.meta.Size); err != nil {
			return err
		}
	} else if !resumed {
		if err := f.Truncate(0); err != nil {
			return err
		}
	}
	var already int64
	for _, c := range d.chunks {
		already += c.Downloaded
	}
	d.initialDone = already
	d.downloaded.Store(already)
	d.opened = true
	if !resumed {
		if err := d.save(); err != nil {
			return err
		}
	}
	return nil
}

// stateProblem 检查旧进度能否续传。能用时返回空字符串。
func (d *downloader) stateProblem(st savedState) string {
	if st.URL != d.opt.URL {
		if !(st.ETag != "" && st.ETag == d.meta.ETag && st.Size == d.meta.Size) {
			return "地址不一致"
		}
	}
	if st.Size != d.meta.Size {
		return "文件大小已变化"
	}
	if d.meta.ETag != "" && st.ETag != "" && d.meta.ETag != st.ETag {
		return "文件标识已变化"
	}
	if d.meta.ETag == "" && d.meta.LastModified != "" && st.LastModified != "" && d.meta.LastModified != st.LastModified {
		return "文件修改时间已变化"
	}
	if !coverageOK(st.Chunks, d.meta.Size) {
		return "分块记录不完整"
	}
	info, err := os.Stat(d.partPath)
	if err != nil {
		return "未找到未完成的文件"
	}
	var maxWritten int64
	for _, c := range st.Chunks {
		if c.Downloaded <= 0 {
			continue
		}
		if end := c.Start + c.Downloaded; end > maxWritten {
			maxWritten = end
		}
	}
	if info.Size() < maxWritten {
		return "未完成文件短于已记录进度"
	}
	return ""
}

// transfer 下载未完成的块。结束时无论成败都会关闭文件并落盘进度。
func (d *downloader) transfer(ctx context.Context) (err error) {
	d.started = time.Now()
	pending := d.pending()
	if len(pending) == 0 {
		d.committed = true
	} else {
		workers := d.workerCount(len(pending))
		d.printPlan(len(d.chunks), workers)
		d.startProgress(ctx)
	}
	defer func() {
		d.stopProgress()
		cerr := d.close()
		if err == nil {
			err = cerr
		}
		if err == nil {
			d.printSummary()
		}
	}()
	if len(pending) == 0 {
		return nil
	}

	saveCtx, saveCancel := context.WithCancel(ctx)
	var saveWG sync.WaitGroup
	saveWG.Add(1)
	go func() {
		defer saveWG.Done()
		d.saveLoop(saveCtx)
	}()

	err = d.dispatch(ctx, pending)
	saveCancel()
	saveWG.Wait()
	if err != nil {
		return err
	}
	if !d.allComplete() {
		return errors.New("下载未完成")
	}
	d.committed = true
	return nil
}

// pending 返回尚未完成的分块下标。
func (d *downloader) pending() []int {
	d.mu.Lock()
	defer d.mu.Unlock()
	var idx []int
	for i, c := range d.chunks {
		if !c.complete() {
			idx = append(idx, i)
		}
	}
	return idx
}

// workerCount 取并发数和未完成块数中较小的一个，至少为 1。
func (d *downloader) workerCount(pending int) int {
	n := d.opt.Concurrency
	if pending < n {
		n = pending
	}
	if n < 1 {
		n = 1
	}
	return n
}

// dispatch 把未完成的块分给工作协程。任一失败会取消其余下载。
func (d *downloader) dispatch(ctx context.Context, pending []int) error {
	workers := d.workerCount(len(pending))
	jobs := make(chan int)
	wctx, cancel := context.WithCancel(ctx)
	defer cancel()

	var wg sync.WaitGroup
	var once sync.Once
	var runErr error
	setErr := func(err error) {
		if err == nil {
			return
		}
		once.Do(func() {
			runErr = err
			cancel()
		})
	}

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for idx := range jobs {
				if wctx.Err() != nil {
					return
				}
				if err := d.downloadChunk(wctx, idx); err != nil {
					setErr(err)
					return
				}
			}
		}()
	}

	func() {
		defer close(jobs)
		for _, idx := range pending {
			select {
			case <-wctx.Done():
				return
			case jobs <- idx:
			}
		}
	}()
	wg.Wait()
	if runErr != nil {
		return runErr
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return nil
}

// downloadChunk 下载一块，对可恢复错误按退避重试。
func (d *downloader) downloadChunk(ctx context.Context, idx int) error {
	attempt := 0
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		err := d.fetch(ctx, idx)
		if err == nil {
			return nil
		}
		if errors.Is(err, errRestart) {
			if d.restarted {
				return fmt.Errorf("分块 %d: %w", idx+1, err)
			}
			d.restarted = true
			if rerr := d.resetSingle(); rerr != nil {
				return rerr
			}
			d.meta.AcceptRanges = false
			d.warn("%v", err)
			continue
		}
		if !retryable(err) || attempt >= d.opt.Retries {
			return fmt.Errorf("分块 %d: %w", idx+1, err)
		}
		attempt++
		d.warn("分块 %d 失败，%s 后重试（%d/%d）: %v", idx+1, backoff(attempt), attempt, d.opt.Retries, err)
		if err := sleepCtx(ctx, backoff(attempt)); err != nil {
			return err
		}
	}
}

var errRestart = errors.New("服务器无法从断点继续，将重新下载")

// fetch 请求这一块还缺的区间，并用 WriteAt 写到对应偏移。
func (d *downloader) fetch(ctx context.Context, idx int) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	d.mu.Lock()
	ch := d.chunks[idx]
	ranges := d.meta.AcceptRanges
	d.mu.Unlock()
	if ch.complete() {
		return nil
	}

	reqStart := ch.Start + ch.Downloaded
	expect := int64(-1)
	rangeHeader := ""
	if ch.End >= ch.Start {
		if reqStart > ch.End {
			return nil
		}
		expect = ch.End - reqStart + 1
		rangeHeader = fmt.Sprintf("bytes=%d-%d", reqStart, ch.End)
	} else if ranges && reqStart > 0 {
		rangeHeader = fmt.Sprintf("bytes=%d-", reqStart)
	} else if ranges && reqStart == 0 && ch.End < 0 {
		rangeHeader = "bytes=0-"
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, d.opt.URL, nil)
	if err != nil {
		return err
	}
	if err := applyHeaders(req, d.opt.Headers); err != nil {
		return err
	}
	if rangeHeader != "" && ranges {
		req.Header.Set("Range", rangeHeader)
		if d.meta.ETag != "" {
			req.Header.Set("If-Range", d.meta.ETag)
		}
	}

	resp, err := d.client.Do(req)
	if err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return err
		}
		return temporary(err)
	}
	defer resp.Body.Close()

	single := d.chunkCount() == 1
	switch resp.StatusCode {
	case http.StatusPartialContent:
		if err := d.checkContentRange(resp.Header.Get("Content-Range"), reqStart, ch.End); err != nil {
			return err
		}
	case http.StatusOK:
		if !single {
			return errors.New("服务器未按分块响应，请改用 -c 1 --no-resume 重新下载")
		}
		if reqStart > 0 {
			return errRestart
		}
		if cl := resp.ContentLength; cl >= 0 && expect < 0 {
			expect = cl
		}
	default:
		return &statusError{Code: resp.StatusCode}
	}

	buf := make([]byte, copyBufSize)
	var got int64
	off := reqStart
	for expect < 0 || got < expect {
		if err := ctx.Err(); err != nil {
			return err
		}
		view := buf
		if expect >= 0 && int64(len(view)) > expect-got {
			view = view[:expect-got]
		}
		n, rerr := readWithIdle(ctx, resp.Body, view, d.opt.IdleTimeout)
		if n > 0 {
			if _, werr := d.file.WriteAt(view[:n], off); werr != nil {
				return werr
			}
			off += int64(n)
			got += int64(n)
			d.addProgress(idx, int64(n))
		}
		if rerr == nil {
			continue
		}
		if errors.Is(rerr, io.EOF) {
			if expect >= 0 && got < expect {
				return temporary(io.ErrUnexpectedEOF)
			}
			if ch.End < ch.Start {
				d.markDone(idx)
			}
			break
		}
		if errors.Is(rerr, context.Canceled) || errors.Is(rerr, context.DeadlineExceeded) {
			return rerr
		}
		return temporary(rerr)
	}
	return nil
}

// checkContentRange 核对 206 响应的起始位置和文件总大小。
func (d *downloader) checkContentRange(h string, wantStart, wantEnd int64) error {
	if strings.TrimSpace(h) == "" {
		return nil
	}
	start, end, total, hasTotal, err := parseContentRange(h)
	if err != nil {
		return err
	}
	if start != wantStart {
		return fmt.Errorf("数据起始位置是 %d，期望 %d", start, wantStart)
	}
	if wantEnd >= 0 && end > wantEnd {
		return fmt.Errorf("数据结束位置 %d 超出分块 %d", end, wantEnd)
	}
	if hasTotal && d.meta.Size >= 0 && total != d.meta.Size {
		return fmt.Errorf("远程文件大小已变为 %d", total)
	}
	return nil
}

// chunkCount 返回当前分块数量。
func (d *downloader) chunkCount() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return len(d.chunks)
}

// addProgress 增加某块已写入的字节数，写满时标记完成。
func (d *downloader) addProgress(idx int, n int64) {
	d.mu.Lock()
	d.chunks[idx].Downloaded += n
	if d.chunks[idx].End >= d.chunks[idx].Start && d.chunks[idx].Downloaded >= d.chunks[idx].length() {
		d.chunks[idx].Done = true
	}
	d.mu.Unlock()
	d.downloaded.Add(n)
}

// markDone 把未知长度的分块标为完成。
func (d *downloader) markDone(idx int) {
	d.mu.Lock()
	d.chunks[idx].Done = true
	d.mu.Unlock()
}

// resetSingle 清空进度，改成从偏移 0 开始的单块下载。
func (d *downloader) resetSingle() error {
	d.mu.Lock()
	end := int64(-1)
	if d.meta.Size > 0 {
		end = d.meta.Size - 1
	}
	d.chunks = []chunk{{Start: 0, End: end}}
	d.mu.Unlock()
	d.downloaded.Store(0)
	d.initialDone = 0
	if d.meta.Size > 0 {
		return d.file.Truncate(d.meta.Size)
	}
	return d.file.Truncate(0)
}

// allComplete 判断每一块是否都已完成。
func (d *downloader) allComplete() bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.meta.Size == 0 {
		return true
	}
	if len(d.chunks) == 0 {
		return false
	}
	for _, c := range d.chunks {
		if !c.complete() {
			return false
		}
	}
	return true
}

// saveLoop 每秒把当前进度写到磁盘，直到上下文取消。
func (d *downloader) saveLoop(ctx context.Context) {
	t := time.NewTicker(time.Second)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			_ = d.save()
		}
	}
}

// save 先记下已写入偏移，再 Sync 数据文件，最后原子写入进度。
func (d *downloader) save() error {
	d.saveMu.Lock()
	defer d.saveMu.Unlock()
	if d.closed || d.file == nil {
		return nil
	}
	// 先记下已经写入的偏移，再 Sync。进度不会超前于内核已接收的数据。
	snap := d.snapshot()
	if err := d.file.Sync(); err != nil {
		return err
	}
	return writeJSONAtomic(d.statePath, snap)
}

// snapshot 复制当前各块进度，供写入进度文件。
func (d *downloader) snapshot() savedState {
	d.mu.Lock()
	defer d.mu.Unlock()
	chunks := make([]chunk, len(d.chunks))
	copy(chunks, d.chunks)
	return savedState{
		URL:          d.opt.URL,
		ETag:         d.meta.ETag,
		LastModified: d.meta.LastModified,
		Size:         d.meta.Size,
		Chunks:       chunks,
	}
}

// close 关闭数据文件。全部完成时把 .part 改成目标文件，否则保存进度。
func (d *downloader) close() error {
	d.saveMu.Lock()
	defer d.saveMu.Unlock()
	if d.closed {
		return nil
	}
	d.closed = true
	var first error
	keep := func(err error) {
		if err != nil && first == nil && !os.IsNotExist(err) {
			first = err
		}
	}
	if d.file != nil && d.opened && !d.committed {
		keep(d.file.Sync())
	}
	if d.file != nil {
		keep(d.file.Close())
		d.file = nil
	}
	if !d.opened {
		return first
	}
	if d.committed {
		keep(renameOrCopy(d.partPath, d.finalPath))
		if first == nil {
			keep(os.Remove(d.statePath))
		}
		return first
	}
	keep(writeJSONAtomic(d.statePath, d.snapshot()))
	return first
}

// startProgress 在后台刷新进度条。静默模式下不启动。
func (d *downloader) startProgress(ctx context.Context) {
	if d.opt.Quiet {
		return
	}
	pctx, cancel := context.WithCancel(ctx)
	d.stopProg = cancel
	m := &meter{
		printer: d.printer,
		total:   d.meta.Size,
		done:    &d.downloaded,
		chunks:  d.chunkStatus,
		lastN:   d.downloaded.Load(),
		lastT:   time.Now(),
	}
	d.progWG.Add(1)
	go func() {
		defer d.progWG.Done()
		m.loop(pctx.Done())
	}()
}

// stopProgress 停止进度刷新并等待它退出。
func (d *downloader) stopProgress() {
	if d.stopProg == nil {
		return
	}
	d.stopProg()
	d.progWG.Wait()
	d.stopProg = nil
}

// chunkStatus 返回已完成块数和总块数。
func (d *downloader) chunkStatus() (int, int) {
	d.mu.Lock()
	defer d.mu.Unlock()
	n := 0
	for _, c := range d.chunks {
		if c.complete() {
			n++
		}
	}
	return n, len(d.chunks)
}

// printPlan 打印文件大小、分块数、并发数和保存路径。
func (d *downloader) printPlan(chunks, workers int) {
	if d.opt.Quiet {
		return
	}
	sizeText := "未知"
	if d.meta.Size >= 0 {
		sizeText = humanBytes(d.meta.Size)
	}
	d.printer.println("大小: " + sizeText)
	d.printer.println(fmt.Sprintf("分块: %d，并发: %d", chunks, workers))
	if d.note != "" {
		d.printer.println(d.note)
	}
	if d.initialDone > 0 {
		d.printer.println("续传: 已完成 " + humanBytes(d.initialDone))
	}
	d.printer.println("保存: " + d.finalPath)
}

// printSummary 打印完成信息，包括本次平均速度和用时。
func (d *downloader) printSummary() {
	if d.opt.Quiet {
		return
	}
	total := d.downloaded.Load()
	if d.meta.Size >= 0 {
		total = d.meta.Size
	}
	elapsed := time.Since(d.started)
	session := d.downloaded.Load() - d.initialDone
	if session < 0 {
		session = 0
	}
	if session == 0 || elapsed <= 0 {
		d.printer.println(fmt.Sprintf("完成: %s (%s)", d.finalPath, humanBytes(total)))
		return
	}
	avg := int64(float64(session) / elapsed.Seconds())
	d.printer.println(fmt.Sprintf("完成: %s (%s, 平均 %s/s, 用时 %s)",
		d.finalPath, humanBytes(total), humanBytes(avg), formatDuration(elapsed)))
}

// printf 在非静默模式下打印一行普通信息。
func (d *downloader) printf(format string, args ...any) {
	if d.opt.Quiet {
		return
	}
	d.printer.println(fmt.Sprintf(format, args...))
}

// warn 打印一行警告。静默模式下也会输出。
func (d *downloader) warn(format string, args ...any) {
	d.printer.println(fmt.Sprintf(format, args...))
}

// newClient 创建按并发数限制连接的 HTTP 客户端，并关闭 HTTP/2。
func newClient(concurrency int) *http.Client {
	if concurrency < 1 {
		concurrency = 1
	}
	tr := &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:   15 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		// 空表会关掉 HTTP/2，这样每个分块使用独立连接，而不是挤在一条多路复用连接上。
		ForceAttemptHTTP2:     false,
		TLSNextProto:          map[string]func(string, *tls.Conn) http.RoundTripper{},
		MaxIdleConns:          concurrency * 2,
		MaxIdleConnsPerHost:   concurrency,
		MaxConnsPerHost:       concurrency,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   15 * time.Second,
		ResponseHeaderTimeout: 30 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
		DisableCompression:    true,
		TLSClientConfig: &tls.Config{
			MinVersion: tls.VersionTLS12,
		},
	}
	return &http.Client{
		Transport: tr,
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			if len(via) >= 10 {
				return errors.New("重定向次数过多")
			}
			if req.URL.Scheme != "http" && req.URL.Scheme != "https" {
				return fmt.Errorf("拒绝跳转到 %s", req.URL.Scheme)
			}
			return nil
		},
	}
}

// applyHeaders 设置默认头和用户附加头。用户传入的 Range 与 If-Range 会被忽略。
func applyHeaders(req *http.Request, headers []string) error {
	req.Header.Set("User-Agent", userAgent)
	req.Header.Set("Accept", "*/*")
	for _, h := range headers {
		name, value, err := splitHeader(h)
		if err != nil {
			return err
		}
		if strings.EqualFold(name, "Range") || strings.EqualFold(name, "If-Range") {
			continue
		}
		req.Header.Set(name, value)
	}
	return nil
}

// checkHeader 检查一条请求头的格式是否为 Key: Value。
func checkHeader(h string) error {
	_, _, err := splitHeader(h)
	return err
}

// splitHeader 把 Key: Value 拆成名称和值，并拒绝换行等非法字符。
func splitHeader(h string) (string, string, error) {
	name, value, ok := strings.Cut(h, ":")
	name = strings.TrimSpace(name)
	value = strings.TrimSpace(value)
	if !ok || name == "" {
		return "", "", fmt.Errorf("请求头格式应为 Key: Value，收到 %q", h)
	}
	if strings.ContainsAny(name, " \t\r\n") || strings.ContainsAny(value, "\r\n") {
		return "", "", fmt.Errorf("请求头包含非法字符: %q", h)
	}
	return name, value, nil
}

type statusError struct{ Code int }

// Error 返回带状态码和状态文本的错误信息。
func (e *statusError) Error() string {
	if text := http.StatusText(e.Code); text != "" {
		return fmt.Sprintf("HTTP %d %s", e.Code, text)
	}
	return fmt.Sprintf("HTTP %d", e.Code)
}

type temporaryError struct{ err error }

// temporary 把错误标成可重试。传入 nil 时仍返回 nil。
func temporary(err error) error {
	if err == nil {
		return nil
	}
	return &temporaryError{err: err}
}

// Error 返回被包装错误的文本。
func (e *temporaryError) Error() string { return e.err.Error() }

// Unwrap 返回被包装的原始错误。
func (e *temporaryError) Unwrap() error { return e.err }

// retryable 判断错误是否值得重试。取消、超时和多数 4xx 不重试。
func retryable(err error) bool {
	if err == nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}
	var te *temporaryError
	if errors.As(err, &te) {
		return true
	}
	var se *statusError
	if errors.As(err, &se) {
		return se.Code == http.StatusRequestTimeout || se.Code == http.StatusTooManyRequests || se.Code >= 500
	}
	return false
}

// readWithIdle 读取一次数据。空闲超过期限或上下文取消时关闭响应体。
func readWithIdle(ctx context.Context, rc io.ReadCloser, buf []byte, idle time.Duration) (int, error) {
	type result struct {
		n   int
		err error
	}
	ch := make(chan result, 1)
	go func() {
		n, err := rc.Read(buf)
		ch <- result{n, err}
	}()
	timer := time.NewTimer(idle)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		_ = rc.Close()
		r := <-ch
		if r.n > 0 && r.err == nil {
			return r.n, nil
		}
		return 0, ctx.Err()
	case <-timer.C:
		_ = rc.Close()
		<-ch
		return 0, &idleError{d: idle}
	case r := <-ch:
		return r.n, r.err
	}
}

type idleError struct{ d time.Duration }

// Error 说明连接空闲超过了给定时间。
func (e *idleError) Error() string {
	return fmt.Sprintf("连接空闲超过 %s", e.d.Round(time.Second))
}

// sleepCtx 等待一段时间。上下文取消时立即返回。
func sleepCtx(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return nil
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

// backoff 按尝试次数计算退避时间，从 500ms 起每次翻倍，最长 5 秒。
func backoff(attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	d := 500 * time.Millisecond
	for i := 1; i < attempt; i++ {
		if d >= 5*time.Second {
			return 5 * time.Second
		}
		d *= 2
	}
	if d > 5*time.Second {
		return 5 * time.Second
	}
	return d
}

// planChunks 按文件大小、并发数和分块大小生成各块区间。
func planChunks(size int64, conns int, block int64) ([]chunk, string) {
	if size < 0 {
		return []chunk{{Start: 0, End: -1}}, ""
	}
	if size == 0 {
		return nil, ""
	}
	if block > 0 {
		minBlock := ceilDiv(size, maxChunks)
		note := ""
		if block < minBlock {
			note = fmt.Sprintf("分块过多，已将分块大小从 %s 调整为 %s", humanBytes(block), humanBytes(minBlock))
			block = minBlock
		}
		return chunksFromSpans(rangesFromBlock(size, block)), note
	}
	parts := conns
	note := ""
	if size < autoMultiMin && parts > 1 {
		parts = 1
		note = "文件较小，改为单连接下载"
	}
	if int64(parts) > size {
		parts = int(size)
	}
	if parts < 1 {
		parts = 1
	}
	return chunksFromSpans(evenSplit(size, parts)), note
}

// chunksFromSpans 把字节区间转成分块记录。
func chunksFromSpans(spans []span) []chunk {
	out := make([]chunk, len(spans))
	for i, s := range spans {
		out[i] = chunk{Start: s.start, End: s.end}
	}
	return out
}

// rangesFromBlock 从 0 开始按固定大小切出闭区间。
func rangesFromBlock(size, block int64) []span {
	if block < 1 {
		block = 1
	}
	out := make([]span, 0, int(ceilDiv(size, block)))
	for start := int64(0); start < size; start += block {
		end := start + block - 1
		if end >= size || end < 0 {
			end = size - 1
		}
		out = append(out, span{start: start, end: end})
	}
	return out
}

// evenSplit 把文件切成 parts 段，余数分给前面的段。
func evenSplit(size int64, parts int) []span {
	if parts < 1 {
		parts = 1
	}
	base := size / int64(parts)
	rem := size % int64(parts)
	out := make([]span, 0, parts)
	var start int64
	for i := 0; i < parts; i++ {
		length := base
		if int64(i) < rem {
			length++
		}
		end := start + length - 1
		out = append(out, span{start: start, end: end})
		start = end + 1
	}
	return out
}

// ceilDiv 返回 a 除以 b 的向上取整。b 不大于 0 时返回 a。
func ceilDiv(a, b int64) int64 {
	if b <= 0 {
		return a
	}
	return (a + b - 1) / b
}

// coverageOK 检查各块是否首尾相接并覆盖整个文件。
func coverageOK(chunks []chunk, size int64) bool {
	if size < 0 {
		return len(chunks) == 1 && chunks[0].Start == 0 && chunks[0].End < 0 && chunks[0].Downloaded >= 0
	}
	if size == 0 {
		return len(chunks) == 0
	}
	if len(chunks) == 0 || chunks[0].Start != 0 {
		return false
	}
	for i, c := range chunks {
		if c.Start < 0 || c.End < c.Start || c.Downloaded < 0 {
			return false
		}
		if c.Downloaded > c.length() {
			return false
		}
		if i > 0 && c.Start != chunks[i-1].End+1 {
			return false
		}
	}
	return chunks[len(chunks)-1].End == size-1
}

// loadState 读取并解析进度文件。
func loadState(path string) (savedState, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return savedState{}, err
	}
	var st savedState
	if err := json.Unmarshal(b, &st); err != nil {
		return savedState{}, err
	}
	return st, nil
}

// writeJSONAtomic 先写入临时文件再改名，避免进度文件停在写了一半的状态。
func writeJSONAtomic(path string, v any) error {
	data, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	data = append(data, '\n')
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

// renameOrCopy 把 src 改名为 dst。跨设备时改为复制后删除源文件。
func renameOrCopy(src, dst string) error {
	if err := os.Rename(src, dst); err == nil || !errors.Is(err, syscall.EXDEV) {
		return err
	}
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()
	out, err := os.OpenFile(dst, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	_, copyErr := io.Copy(out, in)
	syncErr := out.Sync()
	closeErr := out.Close()
	if copyErr != nil {
		return copyErr
	}
	if syncErr != nil {
		return syncErr
	}
	if closeErr != nil {
		return closeErr
	}
	return os.Remove(src)
}

// parseContentRange 解析 bytes start-end/total。total 为 * 时表示没有总长度。
func parseContentRange(v string) (start, end, total int64, hasTotal bool, err error) {
	v = strings.TrimSpace(v)
	if len(v) < len("bytes ") || !strings.EqualFold(v[:len("bytes ")], "bytes ") {
		return 0, 0, 0, false, fmt.Errorf("无效的 Content-Range: %s", v)
	}
	rest := strings.TrimSpace(v[len("bytes "):])
	rangePart, totalPart, ok := strings.Cut(rest, "/")
	if !ok {
		return 0, 0, 0, false, fmt.Errorf("无效的 Content-Range: %s", v)
	}
	rangePart = strings.TrimSpace(rangePart)
	totalPart = strings.TrimSpace(totalPart)
	a, b, ok := strings.Cut(rangePart, "-")
	if !ok || a == "" || b == "" || a == "*" {
		return 0, 0, 0, false, fmt.Errorf("无效的 Content-Range: %s", v)
	}
	start, err = strconv.ParseInt(a, 10, 64)
	if err != nil {
		return 0, 0, 0, false, fmt.Errorf("无效的 Content-Range: %s", v)
	}
	end, err = strconv.ParseInt(b, 10, 64)
	if err != nil || end < start {
		return 0, 0, 0, false, fmt.Errorf("无效的 Content-Range: %s", v)
	}
	if totalPart == "*" {
		return start, end, -1, false, nil
	}
	total, err = strconv.ParseInt(totalPart, 10, 64)
	if err != nil || total < 0 {
		return 0, 0, 0, false, fmt.Errorf("无效的 Content-Range: %s", v)
	}
	return start, end, total, true, nil
}

// parseSize 把 4MB、1.5KB 这类文本换成字节数。
func parseSize(s string) (int64, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0, errors.New("分块大小为空")
	}
	upper := strings.ToUpper(strings.ReplaceAll(s, " ", ""))
	type suf struct {
		text string
		mult int64
	}
	suffixes := []suf{
		{"KIB", 1 << 10}, {"MIB", 1 << 20}, {"GIB", 1 << 30}, {"TIB", 1 << 40},
		{"KB", 1 << 10}, {"MB", 1 << 20}, {"GB", 1 << 30}, {"TB", 1 << 40},
		{"K", 1 << 10}, {"M", 1 << 20}, {"G", 1 << 30}, {"T", 1 << 40},
		{"B", 1},
	}
	mult := int64(1)
	for _, item := range suffixes {
		if strings.HasSuffix(upper, item.text) && len(upper) > len(item.text) {
			mult = item.mult
			upper = strings.TrimSuffix(upper, item.text)
			break
		}
	}
	f, err := strconv.ParseFloat(upper, 64)
	if err != nil || f <= 0 {
		return 0, fmt.Errorf("无效的分块大小: %s", s)
	}
	if f > float64(int64(^uint64(0)>>1))/float64(mult) {
		return 0, fmt.Errorf("分块大小过大: %s", s)
	}
	n := int64(f * float64(mult))
	if n < 1 {
		return 0, fmt.Errorf("分块大小过小: %s", s)
	}
	return n, nil
}

// filenameFromDisposition 从 Content-Disposition 取出文件名，优先使用 filename*。
func filenameFromDisposition(v string) string {
	if strings.TrimSpace(v) == "" {
		return ""
	}
	_, params, err := mime.ParseMediaType(v)
	if err != nil {
		return ""
	}
	if raw := params["filename*"]; raw != "" {
		if name := decodeRFC5987(raw); name != "" {
			return sanitizeName(name)
		}
	}
	if fn := params["filename"]; fn != "" {
		return sanitizeName(fn)
	}
	return ""
}

// decodeRFC5987 解码 filename* 里 charset”percent-encoded 形式的文件名。
func decodeRFC5987(raw string) string {
	raw = strings.TrimSpace(raw)
	if _, rest, ok := strings.Cut(raw, "''"); ok {
		raw = rest
	}
	dec, err := url.PathUnescape(raw)
	if err != nil || strings.TrimSpace(dec) == "" {
		return ""
	}
	return dec
}

// filenameFromURL 取 URL 路径的最后一段作为文件名，取不到时用 download。
func filenameFromURL(raw string) string {
	u, err := url.Parse(raw)
	if err != nil {
		return "download"
	}
	name := sanitizeName(path.Base(u.Path))
	if name == "" {
		return "download"
	}
	return name
}

// sanitizeName 去掉路径和不可见字符，并拒绝空名、. 和 ..。
func sanitizeName(name string) string {
	name = strings.ReplaceAll(name, "\\", "/")
	name = path.Base(name)
	name = strings.Map(func(r rune) rune {
		if r < 32 || r == 127 {
			return -1
		}
		return r
	}, name)
	name = strings.TrimSpace(name)
	if name == "" || name == "." || name == ".." || strings.Trim(name, "/") == "" {
		return ""
	}
	return name
}
