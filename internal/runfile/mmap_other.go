//go:build !unix

package runfile

import "os"

const supported = false

func mmap(*os.File, int) ([]byte, error) { return nil, ErrUnsupported }
func seal([]byte) error                  { return ErrUnsupported }
func drop([]byte) error                  { return ErrUnsupported }
func unmap([]byte)                       {}
