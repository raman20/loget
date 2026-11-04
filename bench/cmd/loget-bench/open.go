package main

import (
	"flag"
	"fmt"
	"math/rand/v2"
	"os"
	"runtime/pprof"
	"time"
)

type openParams struct {
	System        string         `json:"system"`
	Dir           string         `json:"dir"`
	Keys          int            `json:"keys"`
	Reads         int            `json:"reads"`
	OpenSecs      float64        `json:"open_secs"`
	ReadSecs      float64        `json:"read_secs"` // the random reads after open (cold index pages included)
	ReadRate      float64        `json:"reads_per_sec"`
	Missing       int            `json:"missing"`
	PeakRSS       string         `json:"peak_rss"`
	IndexInMemory bool           `json:"index_in_memory,omitempty"`
	Memory        map[string]any `json:"memory"`
}

// cmdOpen opens a database left by "ycsb -keep" in a fresh process: how
// long open takes, what it holds in memory, and how fast random reads are
// right after it.
func cmdOpen(args []string) {
	fs := flag.NewFlagSet("open", flag.ExitOnError)
	system := fs.String("system", "loget", "loget|pebble|... (as ycsb)")
	dir := fs.String("dir", "", "database directory (ycsb -keep leaves <dir>/ycsb-<system>-<fsync>)")
	keys := fs.Int("keys", 1_000_000, "keys the database holds (user0..)")
	reads := fs.Int("reads", 200_000, "uniform random reads after open")
	out := fs.String("out", "results", "output directory")
	fs.BoolVar(&indexInMemory, "index-in-memory", false, "loget only: keep the whole KV index on the Go heap")
	cpuprof := fs.String("cpuprofile", "", "write a CPU profile of the open to this file")
	fs.Parse(args)
	if *dir == "" {
		fatal(fmt.Errorf("open: -dir is required"))
	}
	memS = startMem("")
	memS.setPhase("open")
	if *cpuprof != "" {
		f, err := os.Create(*cpuprof)
		if err != nil {
			fatal(err)
		}
		if err := pprof.StartCPUProfile(f); err != nil {
			fatal(err)
		}
	}
	t0 := time.Now()
	kv, err := openSystem(*system, *dir, true)
	if err != nil {
		fatal(err)
	}
	openSecs := time.Since(t0).Seconds()
	if *cpuprof != "" {
		pprof.StopCPUProfile()
	}
	memS.mark("after open")
	memS.setPhase("reads")
	r := rand.New(rand.NewPCG(42, 9))
	missing := 0
	t1 := time.Now()
	for i := 0; i < *reads; i++ {
		if _, err := kv.Get(keyName(uint64(r.IntN(*keys)))); err != nil {
			missing++
		}
	}
	readSecs := time.Since(t1).Seconds()
	memS.mark("after reads")
	logf("open %s: %.2fs, %d reads in %.2fs (%d missing)", *system, openSecs, *reads, readSecs, missing)
	kv.Close()
	p := openParams{System: *system, Dir: *dir, Keys: *keys, Reads: *reads, OpenSecs: round(openSecs, 3), ReadSecs: round(readSecs, 3),
		ReadRate: round(float64(*reads)/readSecs, 0), Missing: missing, PeakRSS: peakRSS(), IndexInMemory: indexInMemory, Memory: memS.result()}
	writeResult(*out, "open-"+*system, &Result{Suite: "open", Env: envInfo(*dir), Params: p})
	if missing > 0 { // a wrong or empty -dir, not a measurement
		fatal(fmt.Errorf("open: %d of %d keys missing in %s", missing, *reads, *dir))
	}
}
