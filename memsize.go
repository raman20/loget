package loget

import (
	"os"
	"strconv"
	"strings"

	"github.com/raman20/loget/store"
)

// defaultValueCacheBytes sizes the value cache from the memory available
// to the process: 1/16 of it, at most 256 MiB and at least 16 MiB. On a
// 4 GiB+ machine that is 256 MiB (the earlier fixed default); in a 1 GiB
// container it is 64 MiB. Unknown memory (non-Linux) keeps 256 MiB.
//
// On a local store the cache is capped at localValueCache: segment reads
// there are page-cache hits, so a large cache mostly duplicates the OS
// page cache in anonymous memory (design 25: at 10M keys, 256 MiB cost
// ~250 MiB RSS plus GC headroom for ~8% more read throughput).
func defaultValueCacheBytes(local bool) int {
	n := cacheForMem(availableMem())
	if local {
		n = min(n, localValueCache)
	}
	return n
}

const localValueCache = 32 << 20

// localStore reports whether st keeps its objects on a local file system
// (or in memory). Wrapped or unknown stores count as remote.
func localStore(st store.Store) bool {
	switch st.(type) {
	case *store.FS, *store.Journal, *store.Mem:
		return true
	}
	return false
}

func cacheForMem(mem int64) int {
	const hi, lo = defaultValueCache, 16 << 20
	if mem <= 0 {
		return hi
	}
	return int(min(max(mem/16, lo), hi))
}

// availableMem returns the cgroup v2 memory limit if one is set, else the
// machine's MemTotal, else 0.
func availableMem() int64 {
	if b, err := os.ReadFile("/sys/fs/cgroup/memory.max"); err == nil {
		if v, err := strconv.ParseInt(strings.TrimSpace(string(b)), 10, 64); err == nil && v > 0 {
			return v
		}
	}
	if b, err := os.ReadFile("/proc/meminfo"); err == nil {
		for _, l := range strings.Split(string(b), "\n") {
			if f := strings.Fields(l); len(f) >= 2 && f[0] == "MemTotal:" {
				if v, err := strconv.ParseInt(f[1], 10, 64); err == nil {
					return v << 10
				}
			}
		}
	}
	return 0
}
