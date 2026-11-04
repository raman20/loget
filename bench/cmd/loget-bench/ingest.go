package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/cockroachdb/pebble/v2"
	"github.com/cockroachdb/pebble/v2/objstorage/objstorageprovider"
	"github.com/cockroachdb/pebble/v2/sstable"
	"github.com/cockroachdb/pebble/v2/vfs"
	"github.com/raman20/loget"
	"github.com/raman20/loget/store"
)

// cmdIngest measures bulk loading n sorted keys (mode bulk: LogEt's
// Ingest, Pebble's sstable write + Ingest) or writing them in atomic
// batches (mode batch: LogEt's Batch/Apply, Pebble's Batch.Commit), with
// fsync, one system and mode per process so peak RSS (VmHWM) is its own.
func cmdIngest(args []string) {
	fs := flag.NewFlagSet("ingest", flag.ExitOnError)
	system := fs.String("system", "loget", "loget or pebble")
	mode := fs.String("mode", "bulk", "bulk or batch")
	dir := fs.String("dir", "/tmp/loget-ingest", "database directory (wiped)")
	n := fs.Int("n", 1_000_000, "keys")
	vsize := fs.Int("value", 100, "value bytes")
	bsize := fs.Int("batch", 1000, "keys per batch (mode batch)")
	fs.Parse(args)
	os.RemoveAll(*dir)
	if err := os.MkdirAll(*dir, 0o755); err != nil {
		fatal(err)
	}
	val := []byte(strings.Repeat("v", *vsize))
	key := func(i int) string { return fmt.Sprintf("key%012d", i) }
	ctx := context.Background()
	start := time.Now()
	switch *system + "/" + *mode {
	case "loget/bulk", "loget/batch":
		st, err := store.OpenLocal(*dir)
		if err != nil {
			fatal(err)
		}
		db, err := loget.OpenStore(ctx, st, loget.WithLogf(func(string, ...any) {}))
		if err != nil {
			fatal(err)
		}
		start = time.Now()
		if *mode == "bulk" {
			_, err = db.Ingest(ctx, func(add func(string, []byte) error) error {
				for i := 0; i < *n; i++ {
					if err := add(key(i), val); err != nil {
						return err
					}
				}
				return nil
			})
		} else {
			var b loget.Batch
			for i := 0; i < *n && err == nil; i++ {
				if err = b.Put(key(i), val); err != nil {
					break
				}
				if b.Len() == *bsize || i == *n-1 {
					_, err = db.Apply(ctx, &b)
					b.Reset()
				}
			}
		}
		if err != nil {
			fatal(err)
		}
		el := time.Since(start)
		checkLast(func(k string) ([]byte, error) {
			var v []byte
			err := db.View(ctx, func(tx *loget.Tx) (err error) { v, err = tx.Get(k); return err })
			return v, err
		}, key(*n-1))
		reportIngest(*system, *mode, *n, *bsize, el)
		db.Close()
	case "pebble/bulk", "pebble/batch":
		db, err := pebble.Open(*dir, &pebble.Options{Logger: quietLogger{}})
		if err != nil {
			fatal(err)
		}
		start = time.Now()
		if *mode == "bulk" {
			path := filepath.Join(*dir, "ingest.sst")
			f, err := vfs.Default.Create(path, vfs.WriteCategoryUnspecified)
			if err != nil {
				fatal(err)
			}
			w := sstable.NewWriter(objstorageprovider.NewFileWritable(f), sstable.WriterOptions{TableFormat: db.FormatMajorVersion().MaxTableFormat()})
			for i := 0; i < *n; i++ {
				if err := w.Set([]byte(key(i)), val); err != nil {
					fatal(err)
				}
			}
			if err := w.Close(); err != nil {
				fatal(err)
			}
			err = db.Ingest(ctx, []string{path})
		} else {
			b := db.NewBatch()
			for i := 0; i < *n && err == nil; i++ {
				b.Set([]byte(key(i)), val, nil)
				if int(b.Count()) == *bsize || i == *n-1 {
					err = b.Commit(pebble.Sync)
					b.Close()
					b = db.NewBatch()
				}
			}
		}
		if err != nil {
			fatal(err)
		}
		el := time.Since(start)
		checkLast(func(k string) ([]byte, error) {
			v, c, err := db.Get([]byte(k))
			if err != nil {
				return nil, err
			}
			defer c.Close()
			return append([]byte(nil), v...), nil
		}, key(*n-1))
		reportIngest(*system, *mode, *n, *bsize, el)
		db.Close()
	default:
		fatal(fmt.Errorf("unknown system/mode %s/%s", *system, *mode))
	}
}

func checkLast(get func(string) ([]byte, error), k string) {
	if _, err := get(k); err != nil {
		fatal(fmt.Errorf("last key %s: %v", k, err))
	}
}

func reportIngest(system, mode string, n, bsize int, el time.Duration) {
	st := map[string]int64{}
	if b, err := os.ReadFile("/proc/self/status"); err == nil {
		for _, l := range strings.Split(string(b), "\n") {
			if f := strings.Fields(l); len(f) >= 2 {
				st[f[0]], _ = strconv.ParseInt(f[1], 10, 64)
			}
		}
	}
	fmt.Printf("{\"system\":%q,\"mode\":%q,\"keys\":%d,\"batch\":%d,\"secs\":%.2f,\"keys_per_sec\":%.0f,\"peak_rss_mib\":%d,\"end_anon_mib\":%d,\"end_file_mib\":%d}\n",
		system, mode, n, bsize, el.Seconds(), float64(n)/el.Seconds(), st["VmHWM:"]/1024, st["RssAnon:"]/1024, st["RssFile:"]/1024)
}
