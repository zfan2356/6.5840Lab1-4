package mr

import (
	"io"
	"os"
	"path/filepath"
)

// 文件的原子读写操作, 一旦失败就立即撤回, 里面涉及一些io包和os包的函数
func atomicWriteFile(filename string, r io.Reader) error {
	dir, file := filepath.Split(filename)
	if dir == "" {
		dir = "."
	}
	f, err := os.CreateTemp(dir, file) // 这里是创建了一个临时的文件, 在dir目录下, 文件的名字为file+一个随机字符串
	if err != nil {
		_ = os.Remove(f.Name())
		return err
	}
	defer f.Close()

	name := f.Name()
	if _, err := io.Copy(f, r); err != nil {
		return err
	} // 拷贝文件, 将r ——> f 中
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
