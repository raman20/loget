//go:build unix

package runfile

import (
	"os"

	"golang.org/x/sys/unix"
)

const supported = true

func mmap(f *os.File, size int) ([]byte, error) {
	return unix.Mmap(int(f.Fd()), 0, size, unix.PROT_READ|unix.PROT_WRITE, unix.MAP_SHARED)
}

func seal(b []byte) error {
	if err := unix.Mprotect(b, unix.PROT_READ); err != nil {
		return err
	}
	return unix.Madvise(b, unix.MADV_RANDOM)
}

func unmap(b []byte) { _ = unix.Munmap(b) }
