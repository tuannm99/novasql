package util

import (
	"log/slog"
	"os"
)

func CloseFileFunc(f *os.File) {
	err := f.Close()
	if err != nil {
		slog.Error("close file", "err", err)
	}
}
