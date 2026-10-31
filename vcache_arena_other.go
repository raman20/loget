//go:build !(linux || darwin || freebsd || netbsd || openbsd || dragonfly)

package loget

// allocArena falls back to the Go heap where anonymous mmap is not used.
func allocArena(n int) ([]byte, func()) { return make([]byte, n), nil }
