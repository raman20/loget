//go:build linux

package runfile

import (
	"errors"
	"os"

	"golang.org/x/sys/unix"
)

// createAnon opens a file in dir that never has a name (O_TMPFILE), so a
// SIGKILL cannot leave it behind. File systems without O_TMPFILE fall
// back to create-then-unlink.
func createAnon(dir string) (*os.File, error) {
	fd, err := unix.Open(dir, unix.O_TMPFILE|unix.O_RDWR|unix.O_CLOEXEC, 0o600)
	if err != nil {
		if errors.Is(err, unix.EOPNOTSUPP) || errors.Is(err, unix.EISDIR) || errors.Is(err, unix.EINVAL) {
			return createUnlinked(dir)
		}
		return nil, &os.PathError{Op: "open O_TMPFILE", Path: dir, Err: err}
	}
	return os.NewFile(uintptr(fd), dir+"/(anonymous run file)"), nil
}
