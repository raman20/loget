package loget

import (
	"bufio"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"os/exec"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/raman20/loget/index"
	"github.com/raman20/loget/internal/runfile"
	"github.com/raman20/loget/store"
)

const crashG = 4

const crashIngestN = 3000 // keys per ingest (LOGET_CRASH_INGEST)

// crashTTL is the TTL of the ttl<g>/<i> keys (LOGET_CRASH_TTL).
const crashTTL = 20 * time.Millisecond

// crashChild is run in a subprocess: 4 goroutines commit forever, printing
// "ACK g i" only after Update returned nil (i.e. after fsync+rename).
func crashChild(loc string) {
	st, err := store.Open(loc)
	if err != nil {
		panic(err)
	}
	opts := []Option{WithLogf(func(string, ...any) {}), WithSnapshotEvery(50), WithIndex(tallyKind())}
	if d := os.Getenv("LOGET_CRASH_INDEXDIR"); d != "" {
		rd, err := runfile.New(d)
		if err != nil {
			panic(err)
		}
		opts = append(opts, WithIndexFiles(runFiles{rd})) // every run in a file
	}
	if os.Getenv("LOGET_CRASH_CKPT") != "" {
		// background index checkpoints every few KiB in small pieces:
		// SIGKILL lands mid-checkpoint too (design 24)
		index.PieceBytes = 512
		opts = append(opts, WithSnapshotEvery(0), WithCheckpointBytes(4<<10))
	}
	db, err := OpenStore(ctx, st, opts...)
	if err != nil {
		fmt.Println("OPENERR", err)
		os.Exit(3)
	}
	var mu sync.Mutex
	var wg sync.WaitGroup
	if os.Getenv("LOGET_CRASH_COMPACT") != "" {
		go func() { // compaction racing the commits: SIGKILL lands mid-compaction too
			for {
				time.Sleep(5 * time.Millisecond)
				if _, err := db.Compact(ctx, CompactOptions{}); err != nil && !errors.Is(err, ErrBusy) { // busy: an ingest
					fmt.Println("ERR compact", err)
					os.Exit(5)
				}
			}
		}()
	}
	if os.Getenv("LOGET_CRASH_INGEST") != "" {
		go func() { // bulk ingests racing the commits: all or nothing
			for {
				time.Sleep(10 * time.Millisecond) // leave compaction a turn
				id := time.Now().UnixNano()
				_, err := db.Ingest(ctx, func(add func(string, []byte) error) error {
					for j := 0; j < crashIngestN; j++ {
						if err := add(fmt.Sprintf("ing/%d/%05d", id, j), []byte("v")); err != nil {
							return err
						}
					}
					return nil
				})
				if err != nil {
					fmt.Println("ERR ingest", err)
					os.Exit(6)
				}
				mu.Lock()
				os.Stdout.WriteString(fmt.Sprintf("ING %d\n", id))
				mu.Unlock()
			}
		}()
	}
	ttl := os.Getenv("LOGET_CRASH_TTL") != ""
	for g := 0; g < crashG; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			var start int
			db.View(ctx, func(tx *Tx) error {
				v, _ := tx.Get(fmt.Sprint("cnt", g))
				start, _ = strconv.Atoi(string(v))
				return nil
			})
			for i := start + 1; ; i++ {
				err := db.Update(ctx, func(tx *Tx) error {
					if ttl {
						tx.Put(fmt.Sprintf("ttl%d/%08d", g, i), []byte("x"), TTL(crashTTL))
					}
					tx.Put(fmt.Sprintf("g%d/%08d", g, i), []byte("x"))
					tx.Put(fmt.Sprint("cnt", g), []byte(strconv.Itoa(i)))
					return tx.PutRecord("t:c", fmt.Sprintf("%d:%d", g, i), []byte(fmt.Sprint("g", g)))
				})
				if err != nil {
					fmt.Println("ERR", err)
					os.Exit(4)
				}
				mu.Lock()
				os.Stdout.WriteString(fmt.Sprintf("ACK %d %d\n", g, i))
				mu.Unlock()
			}
		}(g)
	}
	wg.Wait()
}

func TestMain(m *testing.M) {
	if d := os.Getenv("LOGET_MIGRATE_CHILD"); d != "" {
		fmt.Println("START")
		if err := store.Migrate(ctx, d, store.LayoutJournal, nil); err != nil {
			fmt.Println("ERR", err)
		}
		return
	}
	if d := os.Getenv("LOGET_CRASH_CHILD"); d != "" {
		crashChild(d)
		return
	}
	os.Exit(m.Run())
}

func TestCrashKill9PrefixConsistent(t *testing.T) { crashKill9(t, "fs://"+t.TempDir()) }

// The journal store keeps every segment in one file; same invariants.
func TestCrashKill9Journal(t *testing.T) { crashKill9(t, "journal://"+t.TempDir()) }

// Same, with the child compacting (grace 0) in a loop while it commits.
func TestCrashKill9Compact(t *testing.T) {
	crashKill9(t, "fs://"+t.TempDir(), "LOGET_CRASH_COMPACT=1")
}
func TestCrashKill9CompactJournal(t *testing.T) {
	crashKill9(t, "journal://"+t.TempDir(), "LOGET_CRASH_COMPACT=1")
}

// Same, with every KV run in a mapped run file: the kill leaves no file
// behind and recovery (reading runs from files) keeps the invariants.
// Kills during background index checkpoints (and compactions): recovery
// from whatever checkpoint survived equals a full replay.
func TestCrashKill9Ckpt(t *testing.T) {
	crashKill9(t, "fs://"+t.TempDir(), "LOGET_CRASH_COMPACT=1", "LOGET_CRASH_CKPT=1")
}

// Kills during bulk ingests (and compactions, checkpoints): each ingest
// is recovered whole or not at all, and an acknowledged one always.
func TestCrashKill9Ingest(t *testing.T) {
	crashKill9(t, "fs://"+t.TempDir(), "LOGET_CRASH_COMPACT=1", "LOGET_CRASH_CKPT=1", "LOGET_CRASH_INGEST=1")
}

// TestCrashKill9TTL: expiring puts (design 27) under kill -9 with
// compaction and index checkpoints racing.
func TestCrashKill9TTL(t *testing.T) {
	crashKill9(t, "fs://"+t.TempDir(), "LOGET_CRASH_COMPACT=1", "LOGET_CRASH_CKPT=1", "LOGET_CRASH_TTL=1")
}

func TestCrashKill9IndexFiles(t *testing.T) {
	if !runfile.Supported {
		t.Skip("run files need mmap")
	}
	crashKill9(t, "journal://"+t.TempDir(), "LOGET_CRASH_COMPACT=1", "LOGET_CRASH_INDEXDIR="+t.TempDir())
}

// crashKill9 repeatedly SIGKILLs a committing child process on the store at
// loc (a store URL or bare path) and checks the recovered state.
func crashKill9(t *testing.T, dir string, env ...string) {
	if testing.Short() {
		t.Skip("short")
	}
	iters := 25
	if s := os.Getenv("LOGET_CRASH_ITERS"); s != "" {
		iters, _ = strconv.Atoi(s)
	}
	acked := make([]int, crashG)
	ingested := map[string]bool{} // acknowledged ingests
	sawIngest := 0
	based := 0  // recoveries that started from a compaction base
	ckpted := 0 // recoveries with an index checkpoint in the manifest
	ttlOn := slices.Contains(env, "LOGET_CRASH_TTL=1")
	ttlLive := make([]int, crashG)
	ttlExpired, ttlSeen := 0, 0
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	for it := 0; it < iters; it++ {
		cmd := exec.Command(os.Args[0], "-test.run=^$")
		cmd.Env = append(append(os.Environ(), env...), "LOGET_CRASH_CHILD="+dir)
		out, _ := cmd.StdoutPipe()
		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}
		done := make(chan struct{})
		go func() {
			sc := bufio.NewScanner(out)
			for sc.Scan() {
				f := strings.Fields(sc.Text())
				if len(f) == 2 && f[0] == "ING" {
					ingested[f[1]] = true
				} else if len(f) == 3 && f[0] == "ACK" {
					g, _ := strconv.Atoi(f[1])
					i, _ := strconv.Atoi(f[2])
					if i > acked[g] {
						acked[g] = i
					}
				} else {
					t.Errorf("child: %s", sc.Text())
				}
			}
			close(done)
		}()
		time.Sleep(time.Duration(20+rng.Intn(150)) * time.Millisecond)
		cmd.Process.Kill() // SIGKILL mid-write
		cmd.Wait()
		<-done
		var recOpts []Option
		for _, e := range env {
			if d, ok := strings.CutPrefix(e, "LOGET_CRASH_INDEXDIR="); ok {
				noFiles(t, d)
				fs, _ := testIndexFiles(t, d, nil)
				recOpts = append(recOpts, WithIndexFiles(fs))
			}
		}

		// recover in-process and check the invariants
		st, err := store.Open(dir)
		if err != nil {
			t.Fatalf("iter %d: store: %v", it, err)
		}
		db, err := OpenStore(ctx, st, append([]Option{quiet(), WithIndex(tallyKind())}, recOpts...)...)
		if err != nil {
			t.Fatalf("iter %d: reopen: %v", it, err)
		}
		db.View(ctx, func(tx *Tx) error {
			seen := map[string]bool{}
			rs, _ := tx.ScanRecords("t:c", "")
			for _, r := range rs {
				seen[r.Key] = true
			}
			for g := 0; g < crashG; g++ {
				v, _ := tx.Get(fmt.Sprint("cnt", g))
				n, _ := strconv.Atoi(string(v))
				if n < acked[g] {
					t.Fatalf("iter %d: g%d recovered %d < acked %d: LOST ACKED WRITE", it, g, n, acked[g])
				}
				kvs, _ := tx.Scan(fmt.Sprintf("g%d/", g))
				if len(kvs) != n {
					t.Fatalf("iter %d g%d: %d keys but counter %d (not atomic)", it, g, len(kvs), n)
				}
				// a reverse range read over the recovered index (run files
				// included) returns the same keys, descending
				ri := tx.Iter(IterOptions{Prefix: fmt.Sprintf("g%d/", g), Reverse: true})
				for i := len(kvs) - 1; i >= 0; i-- {
					if !ri.Next() || ri.Key() != kvs[i].Key || string(ri.Value()) != string(kvs[i].Value) {
						t.Fatalf("iter %d g%d: reverse iterator at %d: %q %v, want %q", it, g, i, ri.Key(), ri.Err(), kvs[i].Key)
					}
				}
				if ri.Next() || ri.Err() != nil {
					t.Fatalf("iter %d g%d: reverse iterator has extra key %q (%v)", it, g, ri.Key(), ri.Err())
				}
				for i := 1; i <= n; i++ {
					if !seen[fmt.Sprintf("%d:%d", g, i)] {
						t.Fatalf("iter %d: g%d record %d missing (not prefix-consistent)", it, g, i)
					}
				}
				if seen[fmt.Sprintf("%d:%d", g, n+1)] {
					t.Fatalf("iter %d: record beyond counter", it)
				}
				if c, err := tallyCount(tx, "t:c", fmt.Sprint("g", g)); err != nil || c != n {
					t.Fatalf("iter %d g%d: tally index counts %d (%v), counter %d", it, g, c, err, n)
				}
				acked[g] = n // unacked-but-durable commits count from now on
				if ttlOn {
					// one TTL and monotone commit times: the live TTL keys
					// are exactly a suffix of 1..n
					ks, _ := tx.Scan(fmt.Sprintf("ttl%d/", g))
					for j, kv := range ks {
						if want := fmt.Sprintf("ttl%d/%08d", g, n-len(ks)+1+j); kv.Key != want {
							t.Fatalf("iter %d g%d: live TTL keys not a suffix: %q at %d, want %q", it, g, kv.Key, j, want)
						}
					}
					ttlLive[g] = len(ks)
					if len(ks) < n {
						ttlExpired++
					}
					if len(ks) > 0 {
						ttlSeen++
					}
				}
			}
			kvs, _ := tx.Scan("ing/")
			per := map[string]int{}
			for _, kv := range kvs {
				id, _, _ := strings.Cut(strings.TrimPrefix(kv.Key, "ing/"), "/")
				per[id]++
			}
			for id, n := range per {
				if n != crashIngestN {
					t.Fatalf("iter %d: ingest %s recovered %d of %d keys (not atomic)", it, id, n, crashIngestN)
				}
				ingested[id] = true
			}
			for id := range ingested {
				if per[id] == 0 {
					t.Fatalf("iter %d: acknowledged ingest %s lost", it, id)
				}
			}
			sawIngest = max(sawIngest, len(per))
			return nil
		})
		r, err := db.Verify(ctx)
		if err != nil || !r.OK {
			t.Fatalf("iter %d verify: %+v %v", it, r, err)
		}
		if ttlOn { // a second open (replay or checkpoint) reads the same
			db.Close()
			if db, err = OpenStore(ctx, st, append([]Option{quiet(), WithIndex(tallyKind())}, recOpts...)...); err != nil {
				t.Fatalf("iter %d: reopen: %v", it, err)
			}
			for g := 0; g < crashG; g++ {
				db.View(ctx, func(tx *Tx) error {
					if ks, _ := tx.Scan(fmt.Sprintf("ttl%d/", g)); len(ks) != ttlLive[g] {
						t.Fatalf("iter %d g%d: %d live TTL keys after a second open, %d before", it, g, len(ks), ttlLive[g])
					}
					return nil
				})
			}
		}
		for _, lm := range db.Manifest().Logs {
			if lm.Base != nil {
				based++
			}
			if lm.Ckpt != nil {
				ckpted++
			}
		}
		d1, _, _ := db.IndexDigests("main")
		d2, _ := db.RebuildDigests("main")
		if fmt.Sprint(d1) != fmt.Sprint(d2) {
			t.Fatalf("iter %d: index != fold(log)", it)
		}
		db.Close()
	}
	if len(env) > 0 && based == 0 {
		t.Fatal("no recovery started from a compaction base")
	}
	if slices.Contains(env, "LOGET_CRASH_INGEST=1") && sawIngest == 0 {
		t.Fatal("no ingest recovered")
	}
	if ttlOn && (ttlExpired == 0 || ttlSeen == 0) {
		t.Fatalf("TTL keys not exercised: %d recoveries saw expired ones, %d live ones", ttlExpired, ttlSeen)
	}
	if slices.Contains(env, "LOGET_CRASH_CKPT=1") && ckpted == 0 {
		t.Fatal("no recovery started from an index checkpoint")
	}
	total := 0
	for _, a := range acked {
		total += a
	}
	t.Logf("%d kill -9 cycles, %d transactions recovered, all acked writes present, prefix-consistent", iters, total)
}
