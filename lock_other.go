//go:build !unix

package main

import "os"

func lockFile(f *os.File) error { return nil }
