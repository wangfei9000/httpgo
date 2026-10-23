package main

import (
	"bytes"
	"context"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestPlanChunks(t *testing.T) {
	got := evenSplit(10, 3)
	want := []span{{0, 3}, {4, 6}, {7, 9}}
	if len(got) != len(want) {
		t.Fatalf("split %#v", got)
	}
	var sum int64
	for i, s := range got {
		if s != want[i] {
			t.Fatalf("span %d = %#v, want %#v", i, s, want[i])
		}
		sum += s.end - s.start + 1
	}
	if sum != 10 {
		t.Fatalf("sum %d", sum)
	}

	blocks := rangesFromBlock(10, 4)
	if len(blocks) != 3 || blocks[0] != (span{0, 3}) || blocks[1] != (span{4, 7}) || blocks[2] != (span{8, 9}) {
		t.Fatalf("blocks %#v", blocks)
	}

	chunks, note := planChunks(10*1024*1024, 4, 0)
	if note != "" || len(chunks) != 4 || chunks[0].Start != 0 || chunks[3].End != 10*1024*1024-1 {
		t.Fatalf("plan %#v %q", chunks, note)
	}
	chunks, _ = planChunks(1000, 8, 300)
	if len(chunks) != 4 || chunks[1].Start != 300 || chunks[3].End != 999 {
		t.Fatalf("block plan %#v", chunks)
	}
	small, note := planChunks(100, 8, 0)
	if len(small) != 1 || note == "" {
		t.Fatalf("small %#v %q", small, note)
	}
}

func TestParseSizeAndNames(t *testing.T) {
	n, err := parseSize("4MB")
	if err != nil || n != 4<<20 {
		t.Fatalf("4MB = %d %v", n, err)
	}
	n, err = parseSize("1.5KB")
	if err != nil || n != 1536 {
		t.Fatalf("1.5KB = %d %v", n, err)
	}
	n, err = parseSize("4MiB")
	if err != nil || n != 4<<20 {
		t.Fatalf("4MiB = %d %v", n, err)
	}
	if _, err := parseSize("0"); err == nil {
		t.Fatal("expected error")
	}
	if filenameFromURL("https://cdn.example/a/b/c.zip?x=1") != "c.zip" {
		t.Fatal(filenameFromURL("https://cdn.example/a/b/c.zip?x=1"))
	}
	if filenameFromURL("https://cdn.example/") != "download" {
		t.Fatal("empty name")
	}
	if filenameFromDisposition(`attachment; filename="a b.bin"`) != "a b.bin" {
		t.Fatal(filenameFromDisposition(`attachment; filename="a b.bin"`))
	}
	if got := filenameFromDisposition(`attachment; filename*=UTF-8''%E4%B8%AD%E6%96%87.zip`); got != "中文.zip" {
		t.Fatalf("filename* = %q", got)
	}
	if filenameFromDisposition(`attachment; filename="../etc/passwd"`) != "passwd" {
		t.Fatal(filenameFromDisposition(`attachment; filename="../etc/passwd"`))
	}
	start, end, total, has, err := parseContentRange("bytes 0-0/1000")
	if err != nil || start != 0 || end != 0 || total != 1000 || !has {
		t.Fatalf("range %d-%d/%d %v %v", start, end, total, has, err)
	}
}

func TestDownloadHTTPS(t *testing.T) {
	data := make([]byte, 2<<20)
	for i := range data {
		data[i] = byte(i)
	}
	var inflight, maxIn atomic.Int32
	var headerOK atomic.Bool
	headerOK.Store(true)
	srv := newFileServer(t, data, `"v1"`, func(r *http.Request) {
		if r.Header.Get("X-Test") != "yes" {
			headerOK.Store(false)
		}
		if r.Method == http.MethodGet && r.Header.Get("Range") != "" && r.Header.Get("Range") != "bytes=0-0" {
			cur := inflight.Add(1)
			for {
				old := maxIn.Load()
				if cur <= old || maxIn.CompareAndSwap(old, cur) {
					break
				}
			}
			time.Sleep(200 * time.Millisecond)
			inflight.Add(-1)
		}
	})
	defer srv.Close()

	dir := t.TempDir()
	out := filepath.Join(dir, "out.bin")
	opt := Option{
		URL:         srv.URL + "/file.bin",
		Output:      out,
		Concurrency: 4,
		BlockSize:   256 * 1024,
		Retries:     2,
		IdleTimeout: 5 * time.Second,
		Headers:     []string{"X-Test: yes"},
		Client:      testClient(srv, 4),
		Quiet:       true,
		Stderr:      io.Discard,
	}
	if err := Download(context.Background(), opt); err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(out)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, data) {
		t.Fatalf("content mismatch %d vs %d", len(got), len(data))
	}
	if !headerOK.Load() {
		t.Fatal("custom header missing")
	}
	if maxIn.Load() < 2 {
		t.Fatalf("expected parallel downloads, max=%d", maxIn.Load())
	}
	if _, err := os.Stat(out + partSuffix); !os.IsNotExist(err) {
		t.Fatal("part file remains")
	}
	if _, err := os.Stat(out + stateSuffix); !os.IsNotExist(err) {
		t.Fatal("state file remains")
	}

	var extra atomic.Int32
	srv.Config.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet && r.Header.Get("Range") != "" && r.Header.Get("Range") != "bytes=0-0" {
			extra.Add(1)
		}
		serveBytes(w, r, data, `"v1"`)
	})
	if err := Download(context.Background(), opt); err != nil {
		t.Fatal(err)
	}
	if extra.Load() != 0 {
		t.Fatalf("redownloaded existing file, ranges=%d", extra.Load())
	}
}

func TestResume(t *testing.T) {
	data := make([]byte, 1000)
	for i := range data {
		data[i] = byte(i * 3)
	}
	var mu sync.Mutex
	var ranges []string
	srv := newFileServer(t, data, `"v1"`, func(r *http.Request) {
		rh := r.Header.Get("Range")
		if r.Method == http.MethodGet && rh != "" && rh != "bytes=0-0" {
			mu.Lock()
			ranges = append(ranges, rh)
			mu.Unlock()
		}
	})
	defer srv.Close()

	dir := t.TempDir()
	out := filepath.Join(dir, "a.bin")
	part := out + partSuffix
	if err := os.WriteFile(part, data, 0o644); err != nil {
		t.Fatal(err)
	}
	f, err := os.OpenFile(part, os.O_RDWR, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteAt(make([]byte, 1000-450), 450); err != nil {
		t.Fatal(err)
	}
	f.Close()

	st := savedState{
		URL:  srv.URL + "/file.bin",
		ETag: `"v1"`,
		Size: 1000,
		Chunks: []chunk{
			{Start: 0, End: 299, Downloaded: 300, Done: true},
			{Start: 300, End: 599, Downloaded: 150},
			{Start: 600, End: 899},
			{Start: 900, End: 999},
		},
	}
	if err := writeJSONAtomic(out+stateSuffix, st); err != nil {
		t.Fatal(err)
	}
	err = Download(context.Background(), Option{
		URL:         st.URL,
		Output:      out,
		Concurrency: 3,
		BlockSize:   300,
		Retries:     1,
		IdleTimeout: 5 * time.Second,
		Client:      testClient(srv, 3),
		Quiet:       true,
		Stderr:      io.Discard,
	})
	if err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(out)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, data) {
		t.Fatal("resumed content mismatch")
	}
	mu.Lock()
	defer mu.Unlock()
	for _, rh := range ranges {
		if rh == "bytes=0-299" {
			t.Fatalf("redownloaded finished chunk: %v", ranges)
		}
	}
	found := false
	for _, rh := range ranges {
		if rh == "bytes=450-599" {
			found = true
		}
	}
	if !found {
		t.Fatalf("missing resume range: %v", ranges)
	}
}

func TestCommitCompletedPart(t *testing.T) {
	data := []byte("already-complete")
	var downloads atomic.Int32
	srv := newFileServer(t, data, `"v1"`, func(r *http.Request) {
		if r.Method == http.MethodGet && r.Header.Get("Range") != "" && r.Header.Get("Range") != "bytes=0-0" {
			downloads.Add(1)
		}
	})
	defer srv.Close()
	dir := t.TempDir()
	out := filepath.Join(dir, "done.bin")
	if err := os.WriteFile(out+partSuffix, data, 0o644); err != nil {
		t.Fatal(err)
	}
	st := savedState{
		URL:  srv.URL + "/file.bin",
		ETag: `"v1"`,
		Size: int64(len(data)),
		Chunks: []chunk{{
			Start: 0, End: int64(len(data)) - 1, Downloaded: int64(len(data)), Done: true,
		}},
	}
	if err := writeJSONAtomic(out+stateSuffix, st); err != nil {
		t.Fatal(err)
	}
	if err := Download(context.Background(), Option{
		URL:         st.URL,
		Output:      out,
		Concurrency: 2,
		Retries:     0,
		IdleTimeout: 5 * time.Second,
		Client:      testClient(srv, 2),
		Quiet:       true,
		Stderr:      io.Discard,
	}); err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(out)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, data) {
		t.Fatalf("got %q", got)
	}
	if downloads.Load() != 0 {
		t.Fatalf("requested %d download ranges", downloads.Load())
	}
}

func TestNoRangeAndRetryAndEmpty(t *testing.T) {
	data := []byte("hello-httpgo-no-range")
	plain := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Length", strconv.Itoa(len(data)))
		if r.Method == http.MethodHead {
			w.WriteHeader(http.StatusOK)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(data)
	}))
	defer plain.Close()
	dir := t.TempDir()
	out := filepath.Join(dir, "plain.bin")
	if err := Download(context.Background(), Option{
		URL:         plain.URL + "/x",
		Output:      out,
		Concurrency: 8,
		Retries:     1,
		IdleTimeout: 5 * time.Second,
		Client:      testClient(plain, 4),
		Quiet:       true,
		Stderr:      io.Discard,
	}); err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(out)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, data) {
		t.Fatalf("got %q", got)
	}

	payload := bytes.Repeat([]byte("abc"), 300)
	var hits atomic.Int32
	flaky := newFileServer(t, payload, `"v1"`, func(r *http.Request) {})
	flaky.Config.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet && r.Header.Get("Range") != "" && r.Header.Get("Range") != "bytes=0-0" {
			if hits.Add(1) == 1 {
				http.Error(w, "boom", http.StatusInternalServerError)
				return
			}
		}
		serveBytes(w, r, payload, `"v1"`)
	})
	defer flaky.Close()
	out = filepath.Join(dir, "retry.bin")
	if err := Download(context.Background(), Option{
		URL:         flaky.URL + "/file.bin",
		Output:      out,
		Concurrency: 2,
		BlockSize:   200,
		Retries:     2,
		IdleTimeout: 5 * time.Second,
		Client:      testClient(flaky, 2),
		Quiet:       true,
		Stderr:      io.Discard,
	}); err != nil {
		t.Fatal(err)
	}
	got, err = os.ReadFile(out)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatal("retry content mismatch")
	}
	if hits.Load() < 2 {
		t.Fatalf("expected a retry, hits=%d", hits.Load())
	}

	emptySrv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Length", "0")
		w.WriteHeader(http.StatusOK)
	}))
	defer emptySrv.Close()
	emptyOut := filepath.Join(dir, "empty.bin")
	if err := Download(context.Background(), Option{
		URL:         emptySrv.URL + "/empty",
		Output:      emptyOut,
		Concurrency: 2,
		Client:      testClient(emptySrv, 1),
		Quiet:       true,
		Stderr:      io.Discard,
	}); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(emptyOut)
	if err != nil {
		t.Fatal(err)
	}
	if info.Size() != 0 {
		t.Fatalf("size %d", info.Size())
	}
}

func TestCancelThenResume(t *testing.T) {
	data := bytes.Repeat([]byte{7}, 64*1024)
	entered := make(chan struct{})
	var once sync.Once
	var block atomic.Bool
	block.Store(true)
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rh := r.Header.Get("Range")
		if block.Load() && r.Method == http.MethodGet && rh != "" && rh != "bytes=0-0" {
			once.Do(func() { close(entered) })
			<-r.Context().Done()
			return
		}
		serveBytes(w, r, data, `"v1"`)
	}))
	defer srv.Close()

	dir := t.TempDir()
	out := filepath.Join(dir, "stop.bin")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	errCh := make(chan error, 1)
	opt := Option{
		URL:         srv.URL + "/file.bin",
		Output:      out,
		Concurrency: 1,
		Retries:     0,
		IdleTimeout: 5 * time.Second,
		Client:      testClient(srv, 1),
		Quiet:       true,
		Stderr:      io.Discard,
	}
	go func() {
		errCh <- Download(ctx, opt)
	}()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for download")
	}
	cancel()
	select {
	case err := <-errCh:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("cancel err %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for cancel")
	}
	st, err := loadState(out + stateSuffix)
	if err != nil {
		t.Fatal(err)
	}
	if st.URL != opt.URL || st.Size != int64(len(data)) || len(st.Chunks) == 0 {
		t.Fatalf("state %#v", st)
	}

	block.Store(false)
	if err := Download(context.Background(), opt); err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(out)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, data) {
		t.Fatal("resumed content mismatch")
	}
}

func TestProcessLock(t *testing.T) {
	if os.Getenv("HTTPGO_LOCK_CHILD") == "1" {
		f, err := os.OpenFile(os.Getenv("HTTPGO_LOCK_FILE"), os.O_CREATE|os.O_RDWR, 0o644)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(2)
		}
		if err := lockFile(f); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(3)
		}
		fmt.Fprintln(os.Stderr, "locked")
		time.Sleep(20 * time.Second)
		return
	}

	dir := t.TempDir()
	path := filepath.Join(dir, "a.part")
	cmd := exec.Command(os.Args[0], "-test.run=^TestProcessLock$", "-test.count=1")
	cmd.Env = append(os.Environ(), "HTTPGO_LOCK_CHILD=1", "HTTPGO_LOCK_FILE="+path)
	var output safeBuf
	cmd.Stdout = &output
	cmd.Stderr = &output
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if cmd.Process != nil {
			_ = cmd.Process.Kill()
		}
		_ = cmd.Wait()
	}()

	deadline := time.Now().Add(10 * time.Second)
	for !strings.Contains(output.String(), "locked") {
		if time.Now().After(deadline) {
			t.Fatalf("child did not lock: %s", output.String())
		}
		time.Sleep(20 * time.Millisecond)
	}
	f, err := os.OpenFile(path, os.O_RDWR, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	err = lockFile(f)
	if err == nil || !strings.Contains(err.Error(), "另一个") {
		t.Fatalf("expected lock error, got %v", err)
	}
}

type safeBuf struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *safeBuf) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func (s *safeBuf) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.String()
}

func TestRunFlags(t *testing.T) {
	if err := run(nil); err == nil {
		t.Fatal("expected usage error")
	}
	if err := run([]string{"-c", "0", "https://example.com/a"}); err == nil {
		t.Fatal("expected concurrency error")
	}
	if err := run([]string{"-h"}); err != nil {
		t.Fatal(err)
	}
	if err := run([]string{"ftp://example.com/a"}); err == nil {
		t.Fatal("expected scheme error")
	}
}

func newFileServer(t *testing.T, data []byte, etag string, hook func(*http.Request)) *httptest.Server {
	t.Helper()
	return httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if hook != nil {
			hook(r)
		}
		serveBytes(w, r, data, etag)
	}))
}

func serveBytes(w http.ResponseWriter, r *http.Request, data []byte, etag string) {
	w.Header().Set("Accept-Ranges", "bytes")
	w.Header().Set("ETag", etag)
	w.Header().Set("Content-Disposition", `attachment; filename="demo.bin"`)
	if r.Method == http.MethodHead {
		w.Header().Set("Content-Length", strconv.Itoa(len(data)))
		w.WriteHeader(http.StatusOK)
		return
	}
	start, end := 0, len(data)-1
	status := http.StatusOK
	if rh := r.Header.Get("Range"); rh != "" {
		s, e, err := parseSingleRange(rh, int64(len(data)))
		if err != nil {
			http.Error(w, err.Error(), http.StatusRequestedRangeNotSatisfiable)
			return
		}
		start, end = int(s), int(e)
		status = http.StatusPartialContent
		w.Header().Set("Content-Range", "bytes "+strconv.Itoa(start)+"-"+strconv.Itoa(end)+"/"+strconv.Itoa(len(data)))
	}
	if end < start {
		w.Header().Set("Content-Length", "0")
		w.WriteHeader(status)
		return
	}
	w.Header().Set("Content-Length", strconv.Itoa(end-start+1))
	w.WriteHeader(status)
	_, _ = w.Write(data[start : end+1])
}

func parseSingleRange(h string, size int64) (int64, int64, error) {
	h = strings.TrimSpace(strings.TrimPrefix(h, "bytes="))
	a, b, ok := strings.Cut(h, "-")
	if !ok {
		return 0, 0, errors.New("bad range")
	}
	start, err := strconv.ParseInt(a, 10, 64)
	if err != nil {
		return 0, 0, err
	}
	var end int64
	if b == "" {
		end = size - 1
	} else {
		end, err = strconv.ParseInt(b, 10, 64)
		if err != nil {
			return 0, 0, err
		}
	}
	if start < 0 || end < start || end >= size {
		return 0, 0, errors.New("out of range")
	}
	return start, end, nil
}

func testClient(srv *httptest.Server, n int) *http.Client {
	c := newClient(n)
	tr := c.Transport.(*http.Transport)
	pool := x509.NewCertPool()
	pool.AddCert(srv.Certificate())
	tr.TLSClientConfig.RootCAs = pool
	return c
}
