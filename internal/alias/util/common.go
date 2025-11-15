package util

import (
	"fmt"
	"os"
)

func CloseFileFunc(f *os.File) {
	err := f.Close()
	if err != nil {
		fmt.Println(err)
	}
}
