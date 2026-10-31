package main

import (
	"encoding/binary"
	"errors"
	"flag"
	"fmt"
	"math/rand/v2"
	"path/filepath"
	"sync/atomic"
	"time"

	badger "github.com/dgraph-io/badger/v4"

	"github.com/raman20/loget"
)

type occRun struct {
	System       string  `json:"system"`
	Fsync        bool    `json:"fsync"`
	Clients      int     `json:"clients"`
	HotKeys      int     `json:"hot_keys"`
	Commits      int64   `json:"commits"`
	CommitsPerS  float64 `json:"commits_per_sec"`
	Attempts     int64   `json:"attempts"`
	AbortRate    float64 `json:"abort_rate"` // (attempts - commits) / attempts
	GaveUp       int64   `json:"gave_up_after_16_retries"`
	Latency      Lat     `json:"latency_incl_retries"`
	InvariantOK  bool    `json:"invariant_ok"`
	InvariantMsg string  `json:"invariant"`
}

func enc(v int64) []byte { b := make([]byte, 8); binary.LittleEndian.PutUint64(b, uint64(v)); return b }
func dec(b []byte) int64 {
	if len(b) < 8 {
		return 0
	}
	return int64(binary.LittleEndian.Uint64(b))
}

func cmdTxn(args []string) {
	fs := flag.NewFlagSet("txn", flag.ExitOnError)
	fsync := fs.Bool("fsync", true, "durable commits")
	clientsF := fs.String("clients", "8,64", "concurrent clients for contended OCC")
	hotF := fs.String("hot", "1,16,256,65536", "hot-set sizes; each txn moves 1 unit between 2 random hot keys (1 = increment one counter)")
	dur := fs.Duration("duration", 5*time.Second, "per cell")
	seed := fs.Uint64("seed", 42, "RNG seed")
	dataDir := fs.String("dir", "/tmp/loget-bench", "data directory (wiped)")
	out := fs.String("out", "results", "output directory")
	fs.Parse(args)
	res := &Result{Suite: "txn", Env: envInfo(*dataDir), Params: map[string]any{"fsync": *fsync, "duration": dur.String(), "seed": *seed,
		"occ_txn": "read 2 hot keys, write both (a-1, b+1); retried on conflict up to 16 times like loget.Update"}}
	for _, sys := range []string{"loget", "badger"} {
		for _, c := range parseInts(*clientsF) {
			for _, h := range parseInts(*hotF) {
				dir := filepath.Join(*dataDir, fmt.Sprintf("txn-%s-%v", sys, *fsync))
				r := occOnce(sys, dir, *fsync, c, h, *dur, *seed)
				res.Runs = append(res.Runs, r)
				logf("occ %s fsync=%v c=%d hot=%d: %.0f commits/s abort %.1f%% gaveup %d p50 %.0fus p99 %.0fus inv=%v",
					sys, *fsync, c, h, r.CommitsPerS, 100*r.AbortRate, r.GaveUp, r.Latency.P50Us, r.Latency.P99Us, r.InvariantOK)
				wipe(dir)
			}
		}
	}
	res.Params.(map[string]any)["peak_rss"] = peakRSS()
	writeResult(*out, fmt.Sprintf("txn-fsync-%v", *fsync), res)
}

func occOnce(sys, dir string, fsync bool, clients, hot int, dur time.Duration, seed uint64) occRun {
	var attempts, gaveUp atomic.Int64
	var commit func(r *rand.Rand) error
	var sum func() (int64, error)
	var closeFn func()
	key := func(i int) string { return fmt.Sprintf("hot/%06d", i) }
	pick := func(r *rand.Rand) (string, string) {
		if hot == 1 {
			return key(0), ""
		}
		a := r.IntN(hot)
		b := r.IntN(hot - 1)
		if b >= a {
			b++
		}
		return key(a), key(b)
	}
	switch sys {
	case "loget":
		db, err := openLogEt(dir, fsync, "")
		if err != nil {
			fatal(err)
		}
		commit = func(r *rand.Rand) error {
			a, b := pick(r)
			err := db.Update(bg, func(tx *loget.Tx) error {
				attempts.Add(1)
				va, err := tx.Get(a)
				if err != nil && !errors.Is(err, loget.ErrNotFound) {
					return err
				}
				if b == "" {
					return tx.Put(a, enc(dec(va)+1))
				}
				vb, err := tx.Get(b)
				if err != nil && !errors.Is(err, loget.ErrNotFound) {
					return err
				}
				if err := tx.Put(a, enc(dec(va)-1)); err != nil {
					return err
				}
				return tx.Put(b, enc(dec(vb)+1))
			})
			if errors.Is(err, loget.ErrConflict) {
				gaveUp.Add(1)
			}
			return err
		}
		sum = func() (s int64, err error) {
			err = db.View(bg, func(tx *loget.Tx) error {
				kvs, err := tx.Scan("hot/")
				for _, kv := range kvs {
					s += dec(kv.Value)
				}
				return err
			})
			return
		}
		closeFn = func() { db.Close() }
	case "badger":
		kv, err := openBadger(dir, fsync)
		if err != nil {
			fatal(err)
		}
		db := kv.(*badgerKV).db
		get := func(txn *badger.Txn, k string) (int64, error) {
			it, err := txn.Get([]byte(k))
			if errors.Is(err, badger.ErrKeyNotFound) {
				return 0, nil
			}
			if err != nil {
				return 0, err
			}
			v, err := it.ValueCopy(nil)
			return dec(v), err
		}
		commit = func(r *rand.Rand) error {
			a, b := pick(r)
			var err error
			for i := 0; i < 16; i++ {
				err = db.Update(func(txn *badger.Txn) error {
					attempts.Add(1)
					va, err := get(txn, a)
					if err != nil {
						return err
					}
					if b == "" {
						return txn.Set([]byte(a), enc(va+1))
					}
					vb, err := get(txn, b)
					if err != nil {
						return err
					}
					if err := txn.Set([]byte(a), enc(va-1)); err != nil {
						return err
					}
					return txn.Set([]byte(b), enc(vb+1))
				})
				if !errors.Is(err, badger.ErrConflict) {
					return err
				}
			}
			gaveUp.Add(1)
			return err
		}
		sum = func() (s int64, err error) {
			err = db.View(func(txn *badger.Txn) error {
				it := txn.NewIterator(badger.IteratorOptions{Prefix: []byte("hot/")})
				defer it.Close()
				for it.Rewind(); it.Valid(); it.Next() {
					v, err := it.Item().ValueCopy(nil)
					if err != nil {
						return err
					}
					s += dec(v)
				}
				return nil
			})
			return
		}
		closeFn = func() { kv.Close() }
	}
	ph := runClosed(clients, dur, func(id int) opFunc {
		r := rand.New(rand.NewPCG(seed, uint64(id)))
		return func() error { return commit(r) }
	})
	commits := ph.Ops - ph.Errors
	s, err := sum()
	r := occRun{System: sys, Fsync: fsync, Clients: clients, HotKeys: hot, Commits: commits, CommitsPerS: round(float64(commits)/ph.Seconds, 1),
		Attempts: attempts.Load(), GaveUp: gaveUp.Load(), Latency: ph.Latency}
	if r.Attempts > 0 {
		r.AbortRate = round(float64(r.Attempts-commits)/float64(r.Attempts), 4)
	}
	want := int64(0)
	if hot == 1 {
		want = commits
	}
	r.InvariantOK = err == nil && s == want
	r.InvariantMsg = fmt.Sprintf("sum of hot keys = %d, expected %d (err=%v)", s, want, err)
	closeFn()
	return r
}
