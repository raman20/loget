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
	if err := unix.Madvise(b, unix.MADV_RANDOM); err != nil {
		return err
	}
	// Building the run touched every page; drop them from this process's
	// page tables (a shared file mapping keeps them, dirty or not, in the
	// page cache). Lookups fault back only the pages they read, so RSS
	// counts the index's working set, not every run ever built (design 25).
	return unix.Madvise(b, unix.MADV_DONTNEED)
}

// drop removes b's pages from this process's page tables; a later read
// faults them back from the page cache.
func drop(b []byte) error { return unix.Madvise(b, unix.MADV_DONTNEED) }

func unmap(b []byte) { _ = unix.Munmap(b) }
