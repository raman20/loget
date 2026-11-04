package loget

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/raman20/loget/index"
	"github.com/raman20/loget/internal/runfile"
)

// runFiles adapts runfile.Dir to index.RunFiles.
type runFiles struct{ d *runfile.Dir }

func (f runFiles) Create() (index.RunFile, error) {
	rf, err := f.d.Create()
	if err != nil {
		return nil, err
	}
	return rf, nil
}

// openRunFiles returns where the KV index keeps its large runs (Options.
// IndexDir), or nil for the Go heap.
func openRunFiles(o Options) (index.RunFiles, error) {
	switch {
	case o.IndexFiles != nil:
		return o.IndexFiles, nil
	case o.IndexInMemory:
		return nil, nil
	case o.IndexDir != "":
		d, err := runfile.New(o.IndexDir)
		if err != nil {
			return nil, fmt.Errorf("loget: index dir: %w", err)
		}
		return runFiles{d}, nil
	case !runfile.Supported:
		return nil, nil
	}
	d, err := runfile.New(os.TempDir())
	if err != nil {
		o.Logf("loget: KV index stays in memory: %v", err)
		return nil, nil
	}
	return runFiles{d}, nil
}

// kvOptions are the options of every KV index of the database.
func (db *DB) kvOptions() index.KVOptions {
	o := index.KVOptions{Inline: db.opts.InlineMax, Files: db.runFiles, OnFileError: db.kvFileError}
	if db.opts.IndexFiles != nil {
		o.MinFileRun = 1
	}
	return o
}

// kvFileError counts and logs a run-file failure; the KV that hit it keeps
// its runs on the heap from then on (design 21).
func (db *DB) kvFileError(err error) {
	db.met.kvFileErrors.Add(1)
	db.opts.Logf("loget: KV index run file failed, using memory for this index: %v", err)
}

// boundEvery is how often IndexResidentBytes is checked.
const boundEvery = 100 * time.Millisecond

// boundResident starts the Options.IndexResidentBytes check (design 25)
// and returns an idempotent stop; a no-op where it does not apply.
func boundResident(files index.RunFiles, budget int64) func() {
	rf, ok := files.(runFiles)
	if budget <= 0 || !ok || fileRSS() < 0 {
		return func() {}
	}
	done := make(chan struct{})
	go func() {
		t := time.NewTicker(boundEvery)
		defer t.Stop()
		for {
			select {
			case <-done:
				return
			case <-t.C:
				if fileRSS() > budget {
					rf.d.DropAll()
				}
			}
		}
	}()
	var once sync.Once
	return func() { once.Do(func() { close(done) }) }
}

// fileRSS returns the process's file-backed resident bytes (RssFile in
// /proc/self/status), or -1 where that is unavailable.
func fileRSS() int64 {
	b, err := os.ReadFile("/proc/self/status")
	if err != nil {
		return -1
	}
	for _, l := range strings.Split(string(b), "\n") {
		if f := strings.Fields(l); len(f) >= 2 && f[0] == "RssFile:" {
			if v, err := strconv.ParseInt(f[1], 10, 64); err == nil {
				return v << 10
			}
		}
	}
	return -1
}
