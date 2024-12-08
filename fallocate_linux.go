package tinydb

import (
	"os"
	"syscall"
)

func fallocate(file *os.File, offset, length int64) error {
	if length == 0 {
		return nil
	}

	return syscall.Fallocate(int(file.Fd()), 0, offset, length)
}
