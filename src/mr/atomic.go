package mr

import (
	"io"
	"os"
	"path/filepath"
)

func atomicWriteFile(filename string, r io.Reader) error {
	dir, file := filepath.Split(filename)
	if dir == "" {
		dir = "."
	}
	f, err := os.CreateTemp(dir, file)
	if err != nil {
		_ = os.Remove(f.Name())
		return err
	}
	defer f.Close()

	name := f.Name()
	if _, err := io.Copy(f, r); err != nil {
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}

	info, err := os.Stat(filename)
	if os.IsNotExist(err) {

	} else if err != nil {
		return err
	} else {
		if err := os.Chmod(name, info.Mode()); err != nil {
			return err
		}
	}

	if err := os.Rename(name, filename); err != nil {
		return err
	}
	return nil
}
