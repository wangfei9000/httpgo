package main

import (
	"strings"
	"testing"
)

func TestProgressLeftAligned(t *testing.T) {
	s := formatProgress(50, 1000, 10, 1, 8)
	if strings.HasPrefix(s, " ") {
		t.Fatalf("progress is indented: %q", s)
	}
	if !strings.HasPrefix(s, "5.0%") {
		t.Fatalf("progress: %q", s)
	}
}

func TestFormatChunkMatrix(t *testing.T) {
	done := []bool{true, false, true, false, false}
	rows := formatChunkMatrix(done)
	if len(rows) != 1 {
		t.Fatalf("rows=%d, want 1", len(rows))
	}
	if !strings.HasPrefix(rows[0], "[ ") || !strings.HasSuffix(rows[0], " ]") {
		t.Fatalf("brackets: %q", rows[0])
	}
	body := rows[0]
	if strings.Count(body, blockDone) != 2 || strings.Count(body, blockPending) != 3 {
		t.Fatalf("squares: %q", body)
	}
	if !strings.Contains(body, blockDone+blockGap+blockPending) {
		t.Fatalf("order: %q", body)
	}
}

func TestFormatChunkMatrixWrapsAtTwelve(t *testing.T) {
	done := make([]bool, squaresPerRow+1)
	rows := formatChunkMatrix(done)
	if len(rows) != 2 {
		t.Fatalf("rows=%d, want 2", len(rows))
	}
	if strings.Count(rows[0], "□")+strings.Count(rows[0], "■") != squaresPerRow {
		t.Fatalf("first row: %q", rows[0])
	}
	if strings.Count(rows[1], "□")+strings.Count(rows[1], "■") != 1 {
		t.Fatalf("second row: %q", rows[1])
	}
	if !strings.HasPrefix(rows[0], "[ ") || !strings.HasSuffix(rows[1], " ]") {
		t.Fatalf("brackets: %#v", rows)
	}
}

func TestFormatChunkMatrixEmpty(t *testing.T) {
	if rows := formatChunkMatrix(nil); rows != nil {
		t.Fatalf("got %#v", rows)
	}
}

func TestMatrixColumns(t *testing.T) {
	if matrixColumns(0) != 0 {
		t.Fatal("empty")
	}
	if matrixColumns(1) != 1 {
		t.Fatal("one")
	}
	if matrixColumns(8) != 8 {
		t.Fatal("eight stays on one row")
	}
	if matrixColumns(squaresPerRow+5) != squaresPerRow {
		t.Fatal("wrap uses a full row")
	}
	if matrixColumns(maxSquares+10) != squaresPerRow {
		t.Fatal("cap")
	}
}

func TestGroupSquaresKeepsRowsOfTwelve(t *testing.T) {
	done := make([]bool, maxSquares+12)
	for i := range done {
		done[i] = true
	}
	done[len(done)-1] = false
	rows := formatChunkMatrix(done)
	if len(rows) != maxSquares/squaresPerRow {
		t.Fatalf("rows=%d", len(rows))
	}
	n := 0
	for _, row := range rows {
		n += strings.Count(row, "□") + strings.Count(row, "■")
		if strings.Count(row, "□")+strings.Count(row, "■") != squaresPerRow {
			t.Fatalf("row width: %q", row)
		}
	}
	if n != maxSquares {
		t.Fatalf("squares=%d, want %d", n, maxSquares)
	}
	if !strings.Contains(rows[len(rows)-1], "□") {
		t.Fatalf("last incomplete chunk should keep a hollow square:\n%s", strings.Join(rows, "\n"))
	}
}

func TestFormatBoard(t *testing.T) {
	left := []string{"大小 62.1MB  ·  分块 8  ·  并发 8  ·  保存 abc"}
	done := make([]bool, 8)
	done[0] = true
	rows := formatBoard(left, done, 80)
	if len(rows) != 2 {
		t.Fatalf("rows=%d\n%s", len(rows), strings.Join(rows, "\n"))
	}
	if rows[0] != left[0] {
		t.Fatalf("info: %q", rows[0])
	}
	if strings.Count(rows[1], blockDone) != 1 || strings.Count(rows[1], blockPending) != 7 {
		t.Fatalf("squares: %q", rows[1])
	}
	if !strings.HasPrefix(rows[1], "[ ") || !strings.HasSuffix(rows[1], " ]") {
		t.Fatalf("brackets: %q", rows[1])
	}
	if strings.Contains(rows[0], "┌") || strings.Contains(rows[1], "│") {
		t.Fatal("big box should be gone")
	}
}

func TestBoardSquaresIgnoreLeftWidth(t *testing.T) {
	done := make([]bool, 24)
	for i := range done {
		done[i] = i%2 == 0
	}
	short := formatBoard([]string{"大小 1MB  ·  分块 24  ·  并发 8  ·  保存 a"}, done, 80)
	long := formatBoard([]string{"大小 1MB  ·  分块 24  ·  并发 8  ·  保存 " + strings.Repeat("很长的目录/", 20) + "video.mp4"}, done, 80)
	if strings.Join(short[1:], "\n") != strings.Join(long[1:], "\n") {
		t.Fatalf("path length reshaped the squares\nshort:\n%s\nlong:\n%s", strings.Join(short, "\n"), strings.Join(long, "\n"))
	}
	for i, row := range long {
		if w := displayWidth(row); w > 80 {
			t.Fatalf("line %d width %d\n%s", i, w, strings.Join(long, "\n"))
		}
	}
}
