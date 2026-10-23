//go:build !unix

package main

import "io"

// terminalColumns 在非 Unix 系统上使用固定宽度排矩阵。
func terminalColumns(w io.Writer) int {
	return 80
}
