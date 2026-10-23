//go:build !unix

package main

import "os"

// lockFile 在非 Unix 系统上不锁文件。Windows 因此允许编译，但两个进程仍可能同时写同一个目标。
func lockFile(f *os.File) error { return nil }
