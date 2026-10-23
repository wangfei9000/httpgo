//go:build unix

package main

import (
	"fmt"
	"os"
	"syscall"
)

// lockFile 用 flock 保证两个 httpgo 进程不会同时写同一个目标。
// 这个锁按进程生效，同一进程里的第二次加锁不会失败。
func lockFile(f *os.File) error {
	err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
	if err == nil {
		return nil
	}
	if err == syscall.EAGAIN || err == syscall.EWOULDBLOCK {
		return fmt.Errorf("已有另一个 httpgo 正在下载该文件")
	}
	return fmt.Errorf("锁定文件失败: %w", err)
}
