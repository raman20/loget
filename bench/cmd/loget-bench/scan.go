package main

import (
	"flag"
	"fmt"
	"math/rand/v2"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/cockroachdb/pebble/v2"
	"github.com/raman20/loget"
)

// scanner is the range-read surface of the scan suite: an ordered visit of
// the keys in [start, end) ("" end = unbounded), descending if reverse,
// stopping after limit keys (0 = all). Engines that only have prefix scans
// implement prefixScanner instead.
type scanner interface {
	ScanRange(start, end string, limit int, reverse bool, fn func(k string, v []byte)) (int, error)
}

type prefixScanner interface {
	ScanPrefix(prefix string, fn func(k string, v []byte)) (int, error)
}

func (p *pebbleKV) ScanRange(start, end string, limit int, reverse bool, fn func(k string, v []byte)) (int, error) {
	o := &pebble.IterOptions{LowerBound: []byte(start)}
	if end != "" {
		o.UpperBound = []byte(end)
	}
	it, err := p.db.NewIter(o)
	if err != nil {
		return 0, err
	}
	n := 0
	ok := it.First()
	if reverse {
		ok = it.Last()
	}
	for ; ok && (limit == 0 || n < limit); n++ {
		fn(string(it.Key()), it.Value()) // valid until the next step, like any Pebble iterator
		if reverse {
			ok = it.Prev()
		} else {
			ok = it.Next()
		}
	}
	return n, it.Close()
}

func (p *pebbleKV) ScanPrefix(prefix string, fn func(k string, v []byte)) (int, error) {
	return p.ScanRange(prefix, prefixEnd(prefix), 0, false, fn)
}

// ScanPrefix uses Tx.Scan, which returns the whole result as a slice.
func (f *logetKV) ScanPrefix(prefix string, fn func(k string, v []byte)) (int, error) {
	n := 0
	err := f.db.View(bg, func(tx *loget.Tx) error {
		kvs, err := tx.Scan(prefix)
		for _, p := range kvs {
			fn(p.Key, p.Value)
		}
		n = len(kvs)
		return err
	})
	return n, err
}

// ScanRange uses Tx.Iter: one page of keys at a time, values read lazily.
func (f *logetKV) ScanRange(start, end string, limit int, reverse bool, fn func(k string, v []byte)) (int, error) {
	n := 0
	err := f.db.View(bg, func(tx *loget.Tx) error {
		it := tx.Iter(loget.IterOptions{Start: start, End: end, Reverse: reverse, Limit: limit})
		for it.Next() {
			fn(it.Key(), it.Value())
			n++
		}
		return it.Err()
	})
	return n, err
}

func prefixEnd(p string) string {
	b := []byte(p)
	for i := len(b) - 1; i >= 0; i-- {
		if b[i] < 0xff {
			b[i]++
			return string(b[:i+1])
		}
	}
	return ""
}

type scanRun struct {
	Phase    string  `json:"phase"`
	Desc     string  `json:"desc"`
	Clients  int     `json:"clients"`
	Closed   Phase   `json:"closed_loop"`
	KeysSec  float64 `json:"keys_per_sec"`
	RSSStart int64   `json:"rss_start_mib,omitempty"`
	RSSPeak  int64   `json:"rss_peak_mib,omitempty"`
	HeapPeak int64   `json:"heap_objects_peak_mib,omitempty"`
}

type scanParams struct {
	System        string  `json:"system"`
	Keys          int     `json:"keys"`
	ValueBytes    int     `json:"value_bytes"`
	Clients       []int   `json:"clients"`
	Duration      string  `json:"duration"`
	Seed          uint64  `json:"seed"`
	Compact       bool    `json:"compact_after_load,omitempty"`
	LoadSecs      float64 `json:"load_seconds"`
	PeakRSS       string  `json:"peak_rss"`
	Memory        any     `json:"memory,omitempty"`
	IndexInMemory bool    `json:"index_in_memory,omitempty"`
}

func cmdScan(args []string) {
	fs := flag.NewFlagSet("scan", flag.ExitOnError)
	system := fs.String("system", "loget", "loget|pebble")
	keys := fs.Int("keys", 1_000_000, "records loaded before the run")
	vsize := fs.Int("value", 1024, "value size in bytes")
	clientsF := fs.String("clients", "1,8", "concurrent clients for the short-scan phases")
	dur := fs.Duration("duration", 5*time.Second, "measurement per phase")
	warm := fs.Duration("warmup", time.Second, "warm-up per phase (discarded)")
	seed := fs.Uint64("seed", 42, "RNG seed")
	phases := fs.String("phases", "prefix100,range50,reverse50,full,full-iter,full-reverse", "phases to run (skipped where the engine lacks the API)")
	compact := fs.Bool("compact", false, "loget only: Compact after the load (keys move to the base)")
	dataDir := fs.String("dir", "/tmp/loget-bench", "data directory (wiped)")
	out := fs.String("out", "results", "output directory")
	fs.BoolVar(&indexInMemory, "index-in-memory", false, "loget only: keep the whole KV index on the Go heap")
	fs.Parse(args)
	clients := parseInts(*clientsF)

	dir := fmt.Sprintf("%s/scan-%s", *dataDir, *system)
	kv, err := openKV(*system, dir, false)
	if err != nil {
		fatal(err)
	}
	defer wipe(dir)
	pool := make([]byte, 1<<20+*vsize)
	r0 := rand.New(rand.NewPCG(*seed, 0))
	for i := range pool {
		pool[i] = byte(r0.Uint32())
	}
	lr := rand.New(rand.NewPCG(*seed, 1))
	memS = startMem("")
	memS.setPhase("load")
	logf("scan %s: loading %d x %dB", *system, *keys, *vsize)
	t0 := time.Now()
	if err := kv.Load(*keys, func(i int) string { return keyName(uint64(i)) }, func(int) []byte {
		o := lr.IntN(1 << 20)
		return append([]byte(nil), pool[o:o+*vsize]...)
	}); err != nil {
		fatal(err)
	}
	loadSecs := time.Since(t0).Seconds()
	if fk, ok := kv.(*logetKV); ok && *compact {
		t := time.Now()
		if _, err := fk.db.Compact(bg, loget.CompactOptions{}); err != nil {
			fatal(err)
		}
		logf("compacted in %.1fs", time.Since(t).Seconds())
	}
	memS.mark("after load")
	n := uint64(*keys)
	z := newZipfian(n/100, 0.99)
	zk := newZipfian(n, 0.99)
	res := &Result{Suite: "scan", Env: envInfo(*dataDir)}
	rs, _ := kv.(scanner)
	ps, _ := kv.(prefixScanner)
	sink := 0
	visit := func(k string, v []byte) { sink += len(k) + len(v) }

	for _, ph := range strings.Split(*phases, ",") {
		ph = strings.TrimSpace(ph)
		var op func(r *rand.Rand) (int, error)
		desc := ""
		switch ph {
		case "prefix100":
			if ps == nil {
				continue
			}
			desc = "all 100 keys under a random (zipfian) 21-byte prefix"
			op = func(r *rand.Rand) (int, error) {
				b := scrambled(z.next(r), n/100)
				return ps.ScanPrefix(keyName(b * 100)[:21], func(string, []byte) {})
			}
		case "range50", "reverse50":
			if rs == nil {
				continue
			}
			rev := ph == "reverse50"
			desc = "uniform 1..100 keys from a random (zipfian) start key, unbounded end (YCSB E scan)"
			if rev {
				desc = "uniform 1..100 keys descending from a random (zipfian) end key"
			}
			op = func(r *rand.Rand) (int, error) {
				k := keyName(scrambled(zk.next(r), n))
				l := 1 + r.IntN(100)
				if rev {
					return rs.ScanRange("", k, l, true, func(string, []byte) {})
				}
				return rs.ScanRange(k, "", l, false, func(string, []byte) {})
			}
		case "full", "full-iter", "full-reverse":
			// full: the prefix-scan API (loget: Tx.Scan, a slice);
			// full-iter / full-reverse: the range iterator
			if (ph == "full" && ps == nil) || (ph != "full" && rs == nil) {
				continue
			}
			desc = "one scan of every key, one client; RSS and heap sampled every 20 ms"
			runtime.GC()
			st := memS.sample()
			var mu sync.Mutex
			peak, heap := st.RSSMiB, st.HeapObjMiB
			stop := make(chan struct{})
			done := make(chan struct{})
			go func() {
				defer close(done)
				tk := time.NewTicker(20 * time.Millisecond)
				defer tk.Stop()
				for {
					select {
					case <-stop:
						return
					case <-tk.C:
						s := memS.sample()
						mu.Lock()
						peak, heap = max(peak, s.RSSMiB), max(heap, s.HeapObjMiB)
						mu.Unlock()
					}
				}
			}()
			t := time.Now()
			var cnt int
			switch ph {
			case "full":
				cnt, err = ps.ScanPrefix("user", visit)
			case "full-iter":
				cnt, err = rs.ScanRange("user", "user~", 0, false, visit)
			default:
				cnt, err = rs.ScanRange("user", "user~", 0, true, visit)
			}
			el := time.Since(t).Seconds()
			close(stop)
			<-done
			if err != nil {
				fatal(err)
			}
			if cnt != *keys {
				fatal(fmt.Errorf("%s: scanned %d keys, want %d", ph, cnt, *keys))
			}
			run := scanRun{Phase: ph, Desc: desc, Clients: 1, KeysSec: round(float64(cnt)/el, 0), RSSStart: st.RSSMiB, RSSPeak: peak, HeapPeak: heap,
				Closed: Phase{Mode: "single", Seconds: round(el, 3), Ops: 1}}
			res.Runs = append(res.Runs, run)
			logf("%s %s: %d keys in %.2fs (%.0f keys/s) RSS %d -> peak %d MiB, heap peak %d MiB", *system, ph, cnt, el, run.KeysSec, st.RSSMiB, peak, heap)
			continue
		default:
			fatal(fmt.Errorf("unknown phase %q", ph))
		}
		for _, c := range clients {
			var keysN, mu = 0, sync.Mutex{}
			mk := func(salt uint64, count bool) func(id int) opFunc {
				return func(id int) opFunc {
					r := rand.New(rand.NewPCG(*seed, salt*1000+uint64(id)+7))
					return func() error {
						k, err := op(r)
						if count {
							mu.Lock()
							keysN += k
							mu.Unlock()
						}
						return err
					}
				}
			}
			runClosed(c, *warm, mk(1, false))
			p := runClosed(c, *dur, mk(2, true))
			run := scanRun{Phase: ph, Desc: desc, Clients: c, Closed: p, KeysSec: round(float64(keysN)/p.Seconds, 0)}
			res.Runs = append(res.Runs, run)
			logf("%s %s c=%d: %.0f scans/s, %.0f keys/s, p50 %.1fus p99 %.1fus, errs %d %s", *system, ph, c, p.OpsPerSec, run.KeysSec, p.Latency.P50Us, p.Latency.P99Us, p.Errors, p.FirstError)
		}
	}
	memS.mark("end")
	_ = sink
	kv.Close()
	res.Params = scanParams{System: *system, Keys: *keys, ValueBytes: *vsize, Clients: clients, Duration: dur.String(), Seed: *seed,
		Compact: *compact, LoadSecs: round(loadSecs, 2), PeakRSS: peakRSS(), Memory: memS.result(), IndexInMemory: indexInMemory}
	writeResult(*out, fmt.Sprintf("scan-%s", *system), res)
}
