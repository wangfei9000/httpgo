//go:build unix

package main

import (
	"io"
	"os"
	"syscall"
	"unsafe"
)

// terminalColumns 读取终端宽度，读不到时按 80 列排矩阵。
func terminalColumns(w io.Writer) int {
	f, ok := w.(*os.File)
	if !ok {
		return 80
	}
	var ws struct {
		Row    uint16
		Col    uint16
		Xpixel uint16
		Ypixel uint16
	}
	_, _, errno := syscall.Syscall(syscall.SYS_IOCTL, f.Fd(), uintptr(syscall.TIOCGWINSZ), uintptr(unsafe.Pointer(&ws)))
	if errno != 0 || ws.Col < 2 {
		return 80
	}
	return int(ws.Col)
}
