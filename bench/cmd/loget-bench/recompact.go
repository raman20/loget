package main

import (
	"context"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"runtime"
	"time"

	"github.com/raman20/loget"
)

// cmdRecompact measures repeated compactions of a large database with a
// little churn between them (design 28): load -keys values, compact once,
// then -cycles times overwrite -churn of the keys at random and compact.
// -copyall disables chunk sharing (every compaction copies every value,
// as before design 28). Peak RSS is reset before each compaction
// (/proc/self/clear_refs) so it covers that compaction only.
func cmdRecompact(args []string) {
	fl := flag.NewFlagSet("recompact", flag.ExitOnError)
	dir := fl.String("dir", "/tmp/loget-bench/recompact", "database directory (wiped first)")
	url := fl.String("url", "", "database URL (default: -dir)")
	keys := fl.Int("keys", 1000000, "keys")
	vsize := fl.Int("value", 100, "value bytes")
	churn := fl.Float64("churn", 0.01, "fraction of keys overwritten between compactions")
	cycles := fl.Int("cycles", 3, "compactions after the first")
	copyAll := fl.Bool("copyall", false, "copy every value (no chunk sharing)")
	inline := fl.Int("inline", 0, "WithInlineMax (0: default)")
	out := fl.String("out", "results", "output directory")
	fl.Parse(args)
	if *url == "" {
		*url = *dir
	}
	wipe(*dir)
	ctx := context.Background()
	opts := []loget.Option{loget.WithLogf(func(string, ...any) {})}
	if *copyAll {
		opts = append(opts, loget.WithCompactCopyAll())
	}
	if *inline > 0 {
		opts = append(opts, loget.WithInlineMax(*inline))
	}
	db, err := loget.Open(ctx, *url, opts...)
	if err != nil {
		fatal(err)
	}
	defer db.Close()
	put := func(ks []int, r int) {
		const batch = 1000
		for i := 0; i < len(ks); i += batch {
			err := db.Update(ctx, func(tx *loget.Tx) error {
				for _, k := range ks[i:min(i+batch, len(ks))] {
					v := make([]byte, *vsize)
					copy(v, fmt.Sprintf("r%d-k%d-", r, k))
					if err := tx.Put(fmt.Sprintf("key%09d", k), v); err != nil {
						return err
					}
				}
				return nil
			})
			if err != nil {
				fatal(err)
			}
		}
	}
	all := make([]int, *keys)
	for i := range all {
		all[i] = i
	}
	t := time.Now()
	put(all, 0)
	logf("recompact: loaded %d keys x %d B in %.1fs", *keys, *vsize, time.Since(t).Seconds())
	res := &Result{Suite: "recompact", Env: envInfo(*dir), Params: map[string]any{
		"keys": *keys, "value": *vsize, "churn": *churn, "cycles": *cycles, "copyall": *copyAll}}
	rng := rand.New(rand.NewSource(1))
	var runs []any
	for c := 0; c <= *cycles; c++ {
		if c > 0 {
			ks := make([]int, int(float64(*keys)**churn))
			for i := range ks {
				ks[i] = rng.Intn(*keys)
			}
			put(ks, c)
		}
		os.WriteFile("/proc/self/clear_refs", []byte("5"), 0) // reset VmHWM
		runtime.GC()
		t := time.Now()
		st, err := db.Compact(ctx, loget.CompactOptions{})
		if err != nil {
			fatal(err)
		}
		ms := float64(time.Since(t).Microseconds()) / 1000
		logf("recompact %d: %8.0f ms  written %7.1f MiB  shared %d chunks (%.1f MiB live)  peak RSS %s",
			c, ms, float64(st.BaseBytes)/(1<<20), st.SharedChunks, float64(st.SharedBytes)/(1<<20), peakRSS())
		runs = append(runs, map[string]any{"cycle": c, "ms": ms, "written_bytes": st.BaseBytes,
			"shared_chunks": st.SharedChunks, "shared_bytes": st.SharedBytes, "peak_rss": peakRSS()})
	}
	res.Runs = runs
	writeResult(*out, "recompact", res)
}
