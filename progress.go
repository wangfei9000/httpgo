package main

import (
	"fmt"
	"io"
	"math"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type printer struct {
	mu        sync.Mutex
	w         io.Writer
	tty       bool
	last      int
	lastPrint time.Time
}

// newPrinter 创建进度输出器。目标是终端时用回车覆盖同一行，否则按普通文本输出。
func newPrinter(w io.Writer) *printer {
	if w == nil {
		w = os.Stderr
	}
	p := &printer{w: w}
	f, ok := w.(*os.File)
	if !ok {
		return p
	}
	st, err := f.Stat()
	if err == nil && st.Mode()&os.ModeCharDevice != 0 {
		p.tty = true
	}
	return p
}

// update 刷新进度行。终端用回车覆盖同一行，非终端最多每秒打印一次。
func (p *printer) update(line string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if !p.tty {
		if time.Since(p.lastPrint) < time.Second {
			return
		}
		p.lastPrint = time.Now()
		fmt.Fprintln(p.w, line)
		return
	}
	pad := ""
	if n := p.last - len(line); n > 0 {
		pad = strings.Repeat(" ", n)
	}
	fmt.Fprintf(p.w, "\r%s%s", line, pad)
	if len(line) > p.last {
		p.last = len(line)
	}
}

// println 先清掉进度行，再输出一行完整文本。
func (p *printer) println(line string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.tty && p.last > 0 {
		fmt.Fprint(p.w, "\r"+strings.Repeat(" ", p.last)+"\r")
		p.last = 0
	}
	fmt.Fprintln(p.w, line)
}

// clearLine 在终端上结束当前进度行。
func (p *printer) clearLine() {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.tty && p.last > 0 {
		fmt.Fprintln(p.w)
		p.last = 0
	}
}

type meter struct {
	printer *printer
	total   int64
	done    *atomic.Int64
	chunks  func() (int, int)
	lastN   int64
	lastT   time.Time
	rate    float64
}

// loop 定时刷新进度，直到下载结束。
func (m *meter) loop(ctxDone <-chan struct{}) {
	t := time.NewTicker(200 * time.Millisecond)
	defer t.Stop()
	defer m.printer.clearLine()
	m.render()
	for {
		select {
		case <-ctxDone:
			m.render()
			return
		case <-t.C:
			m.render()
		}
	}
}

// render 根据已下载字节和速度生成一行进度。
func (m *meter) render() {
	now := time.Now()
	n := m.done.Load()
	if dt := now.Sub(m.lastT).Seconds(); dt >= 0.2 {
		m.rate = float64(n-m.lastN) / dt
		if m.rate < 0 {
			m.rate = 0
		}
		m.lastN = n
		m.lastT = now
	}
	doneChunks, totalChunks := m.chunks()
	m.printer.update(formatProgress(n, m.total, m.rate, doneChunks, totalChunks))
}

// formatProgress 拼出百分比、进度条、速度、剩余时间和分块进度。
func formatProgress(done, total int64, speed float64, chunkDone, chunkTotal int) string {
	speedText := humanBytes(int64(speed)) + "/s"
	chunks := fmt.Sprintf("块 %d/%d", chunkDone, chunkTotal)
	if total <= 0 {
		return fmt.Sprintf("%s  %s  %s", humanBytes(done), speedText, chunks)
	}
	pct := float64(done) / float64(total)
	if pct > 1 {
		pct = 1
	}
	const width = 24
	filled := int(pct * width)
	if filled > width {
		filled = width
	}
	bar := strings.Repeat("=", filled)
	if filled < width {
		bar += ">"
		if width-filled-1 > 0 {
			bar += strings.Repeat(" ", width-filled-1)
		}
	}
	eta := "--:--"
	if speed > 1 && done < total {
		eta = formatETA(time.Duration(float64(total-done) / speed * float64(time.Second)))
	}
	return fmt.Sprintf("%6.1f%% [%s] %s/%s  %s  eta %s  %s",
		pct*100, bar, humanBytes(done), humanBytes(total), speedText, eta, chunks)
}

// formatETA 把剩余时间格式化成秒、分秒或时分。
func formatETA(d time.Duration) string {
	if d < 0 {
		d = 0
	}
	d = d.Round(time.Second)
	h := int(d.Hours())
	m := int(d.Minutes()) % 60
	s := int(d.Seconds()) % 60
	if h > 0 {
		return fmt.Sprintf("%dh%02dm", h, m)
	}
	if m > 0 {
		return fmt.Sprintf("%dm%02ds", m, s)
	}
	return fmt.Sprintf("%ds", s)
}

// humanBytes 把字节数换成 B、KB、MB 等可读单位。
func humanBytes(n int64) string {
	if n < 0 {
		n = 0
	}
	units := [...]string{"B", "KB", "MB", "GB", "TB", "PB"}
	f := float64(n)
	i := 0
	for f >= 1024 && i < len(units)-1 {
		f /= 1024
		i++
	}
	if i == 0 {
		return fmt.Sprintf("%dB", n)
	}
	if f >= 100 || f == math.Trunc(f) {
		return fmt.Sprintf("%.0f%s", f, units[i])
	}
	return fmt.Sprintf("%.1f%s", f, units[i])
}

// formatDuration 把用时收成毫秒或秒。
func formatDuration(d time.Duration) string {
	if d < time.Second {
		return d.Round(time.Millisecond).String()
	}
	return d.Round(time.Second).String()
}
