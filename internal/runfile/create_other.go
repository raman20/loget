//go:build !linux

package runfile

import "os"

func createAnon(dir string) (*os.File, error) { return createUnlinked(dir) }
