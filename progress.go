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
	"unicode/utf8"
)

type printer struct {
	mu        sync.Mutex
	w         io.Writer
	tty       bool
	last      int
	lines     int // 终端上矩阵和底部进度条一共占多少行
	lastPrint time.Time
}

// newPrinter 创建进度输出器。目标是终端时原地刷新，否则按普通文本输出。
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

// paint 在终端上重画矩阵和底部进度条。光标停在最后一行末尾，方便下次覆盖。
func (p *printer) paint(lines []string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if !p.tty || len(lines) == 0 {
		return
	}
	if p.lines > 1 {
		fmt.Fprintf(p.w, "\033[%dA", p.lines-1)
	}
	for i, line := range lines {
		fmt.Fprintf(p.w, "\r\033[2K%s", line)
		if i+1 < len(lines) {
			fmt.Fprint(p.w, "\n")
		}
	}
	if extra := p.lines - len(lines); extra > 0 {
		for i := 0; i < extra; i++ {
			fmt.Fprint(p.w, "\n\r\033[2K")
		}
		fmt.Fprintf(p.w, "\033[%dA", extra)
	}
	p.lines = len(lines)
	p.last = 0
}

// wipeLocked 清掉当前进度区域。调用方必须已持有 p.mu。
func (p *printer) wipeLocked() {
	if !p.tty {
		return
	}
	if p.lines > 1 {
		fmt.Fprintf(p.w, "\033[%dA", p.lines-1)
	}
	if p.lines > 0 || p.last > 0 {
		fmt.Fprint(p.w, "\r\033[J")
	}
	p.lines = 0
	p.last = 0
}

// println 先清掉进度区域，再输出一行完整文本。
func (p *printer) println(line string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.wipeLocked()
	fmt.Fprintln(p.w, line)
}

// clearLine 在终端上结束进度区域，保留已经画出的矩阵和进度条。
func (p *printer) clearLine() {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.tty && (p.lines > 0 || p.last > 0) {
		fmt.Fprintln(p.w)
		p.lines = 0
		p.last = 0
	}
}

type meter struct {
	printer *printer
	total   int64
	done    *atomic.Int64
	chunks  func() []bool
	left    []string
	lastN   int64
	lastT   time.Time
	rate    float64
}

// loop 定时刷新进度，直到下载结束。第一帧由 startProgress 先画出来。
func (m *meter) loop(ctxDone <-chan struct{}) {
	t := time.NewTicker(200 * time.Millisecond)
	defer t.Stop()
	defer m.printer.clearLine()
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

// render 刷新分块矩阵，并在最下面保留原来的进度条。
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
	var flags []bool
	if m.chunks != nil {
		flags = m.chunks()
	}
	doneChunks := 0
	for _, ok := range flags {
		if ok {
			doneChunks++
		}
	}
	bar := formatProgress(n, m.total, m.rate, doneChunks, len(flags))
	if !m.printer.tty {
		m.printer.update(bar)
		return
	}
	board := formatBoard(m.left, flags, terminalColumns(m.printer.w))
	lines := make([]string, 0, len(board)+1)
	lines = append(lines, board...)
	lines = append(lines, bar)
	m.printer.paint(lines)
}

// formatBoard 第一行是下载信息，下面每行最多 12 个小方块，最后由调用方接上进度条。
func formatBoard(left []string, done []bool, termWidth int) []string {
	out := make([]string, 0, 1+len(done))
	for _, line := range left {
		if termWidth > 0 {
			line = fitDisplay(line, termWidth)
		}
		out = append(out, line)
	}
	out = append(out, formatChunkMatrix(done)...)
	return out
}

// 未完成是空心方块，下载完成变成实心方块。颜色跟终端字体一样，为白色。
// 方块按两列计，中间空两列。
const (
	blockPending   = "\033[97m□\033[0m"
	blockDone      = "\033[97m■\033[0m"
	blockGap       = "  "
	blockCellWidth = 2
	blockGapWidth  = 2
)

// squaresPerRow 是每行小方块个数。不满一行就只排一行，满了再换行。
// maxSquares 是小方块数量上限。
const (
	squaresPerRow = 12
	maxSquares    = 36
)

// formatChunkMatrix 按每行 12 个排小方块，每行用方括号包住。
func formatChunkMatrix(done []bool) []string {
	n := len(done)
	if n == 0 {
		return nil
	}
	shown := done
	if n > maxSquares {
		shown = groupSquares(done, maxSquares)
	}
	rows := make([]string, 0, (len(shown)+squaresPerRow-1)/squaresPerRow)
	for i := 0; i < len(shown); i += squaresPerRow {
		end := i + squaresPerRow
		if end > len(shown) {
			end = len(shown)
		}
		rows = append(rows, formatSquareRow(shown[i:end]))
	}
	return rows
}

// formatSquareRow 把一行小方块包进方括号。
func formatSquareRow(cells []bool) string {
	var b strings.Builder
	b.WriteString("[ ")
	for i, ok := range cells {
		if i > 0 {
			b.WriteString(blockGap)
		}
		if ok {
			b.WriteString(blockDone)
		} else {
			b.WriteString(blockPending)
		}
	}
	b.WriteString(" ]")
	return b.String()
}

// groupSquares 把多出来的分块均分到有限个小方块上。一组里的块全部下完，方块才变实心。
func groupSquares(done []bool, limit int) []bool {
	n := len(done)
	if limit < 1 {
		limit = 1
	}
	if n <= limit {
		return done
	}
	out := make([]bool, limit)
	for s := 0; s < limit; s++ {
		start := s * n / limit
		end := (s + 1) * n / limit
		ok := start < end
		for i := start; i < end; i++ {
			if !done[i] {
				ok = false
				break
			}
		}
		out[s] = ok
	}
	return out
}

// matrixColumns 返回这一批小方块实际占用的列数，最多 squaresPerRow。
func matrixColumns(n int) int {
	if n <= 0 {
		return 0
	}
	if n > maxSquares {
		n = maxSquares
	}
	if n > squaresPerRow {
		return squaresPerRow
	}
	return n
}

// displayWidth 计算终端上的列宽。中文和方块按两列，边框按一列，颜色码不占列。
func displayWidth(s string) int {
	n := 0
	for i := 0; i < len(s); {
		if s[i] == 0x1b {
			i++
			for i < len(s) && s[i] != 'm' {
				i++
			}
			if i < len(s) {
				i++
			}
			continue
		}
		r, size := utf8.DecodeRuneInString(s[i:])
		if size <= 0 {
			break
		}
		i += size
		n += runeWidth(r)
	}
	return n
}

func runeWidth(r rune) int {
	if r < 0x20 || r == 0x7f {
		return 0
	}
	if r < 0x7f {
		return 1
	}
	if r == '□' || r == '■' {
		return blockCellWidth
	}
	if isWideRune(r) {
		return 2
	}
	return 1
}

func isWideRune(r rune) bool {
	switch {
	case r >= 0x1100 && r <= 0x115F,
		r >= 0x2E80 && r <= 0xA4CF,
		r >= 0xAC00 && r <= 0xD7A3,
		r >= 0xF900 && r <= 0xFAFF,
		r >= 0xFE10 && r <= 0xFE19,
		r >= 0xFE30 && r <= 0xFE6F,
		r >= 0xFF00 && r <= 0xFF60,
		r >= 0xFFE0 && r <= 0xFFE6,
		r >= 0x20000 && r <= 0x3FFFD:
		return true
	}
	return false
}

func fitDisplay(s string, width int) string {
	if width <= 0 || displayWidth(s) <= width {
		return s
	}
	if width <= 3 {
		return strings.Repeat(".", width)
	}
	var b strings.Builder
	n := 0
	for _, r := range s {
		w := runeWidth(r)
		if n+w > width-3 {
			break
		}
		b.WriteRune(r)
		n += w
	}
	b.WriteString("...")
	return b.String()
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
	return fmt.Sprintf("%.1f%% [%s] %s/%s  %s  eta %s  %s",
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
