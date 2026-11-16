package loget

import (
	"bufio"
	"errors"
	"fmt"
	"math/rand"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
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

// crashDelay is an optional per-op pause for local slow-runner tests
// (LOGET_CRASH_DELAY, e.g. "20ms"). CI does not set it.
func crashDelay() time.Duration {
	d, _ := time.ParseDuration(os.Getenv("LOGET_CRASH_DELAY"))
	return d
}

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
		// 256 bytes: a few commits, not 4 KiB. A higher threshold lost
		// to the compact loop on a slow runner (sinceCk resets when a
		// compact replaces the writer).
		opts = append(opts, WithSnapshotEvery(0), WithCheckpointBytes(256))
	}
	db, err := OpenStore(ctx, st, opts...)
	if err != nil {
		fmt.Println("OPENERR", err)
		os.Exit(3)
	}
	var mu sync.Mutex
	var wg sync.WaitGroup
	if os.Getenv("LOGET_CRASH_SMALLCHUNKS") != "" {
		chunkMax = 2 << 10 // many value chunks: compactions share and copy some
	}
	slow := crashDelay()
	if os.Getenv("LOGET_CRASH_COMPACT") != "" {
		go func() { // compaction racing the commits: SIGKILL lands mid-compaction too
			for {
				time.Sleep(5*time.Millisecond + slow)
				if _, err := db.Compact(ctx, CompactOptions{}); err != nil && !errors.Is(err, ErrBusy) { // busy: an ingest
					fmt.Println("ERR compact", err)
					os.Exit(5)
				} else if err == nil {
					mu.Lock()
					os.Stdout.WriteString("BASE\n")
					mu.Unlock()
					if os.Getenv("LOGET_CRASH_CKPT") != "" {
						// Compact drops the index checkpoint with the old
						// base; wait until a new one is committed so a
						// recovery can start from it. The parent may kill
						// during this wait or during the next compact.
						for {
							ok := false
							for _, lm := range db.Manifest().Logs {
								if lm.Ckpt != nil {
									ok = true
									break
								}
							}
							if ok {
								break
							}
							time.Sleep(2 * time.Millisecond)
						}
					}
				}
			}
		}()
	}
	if os.Getenv("LOGET_CRASH_CKPT") != "" {
		go func() { // tell the parent when a checkpoint is in the manifest
			var last uint64
			for {
				time.Sleep(2 * time.Millisecond)
				for _, lm := range db.Manifest().Logs {
					if lm.Ckpt != nil && lm.Ckpt.LSN != last {
						last = lm.Ckpt.LSN
						mu.Lock()
						os.Stdout.WriteString("CKPT\n")
						mu.Unlock()
					}
				}
			}
		}()
	}
	if os.Getenv("LOGET_CRASH_INGEST") != "" {
		go func() { // bulk ingests racing the commits: all or nothing
			for {
				time.Sleep(10*time.Millisecond + slow) // leave compaction a turn
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
				if slow > 0 {
					time.Sleep(slow)
				}
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

// The same kill -9 invariants against an S3 endpoint (LOGET_TEST_S3, e.g.
// local MinIO or LocalStack behind a latency/fault proxy): the child's
// in-flight PUTs die with it, recovery reads the bucket.
func TestCrashKill9S3(t *testing.T) { crashKill9(t, s3CrashURL(t), "LOGET_CRASH_WAITACK=1") }

// ... and with compactions running in the child (killed mid-base too).
func TestCrashKill9S3Compact(t *testing.T) {
	crashKill9(t, s3CrashURL(t), "LOGET_CRASH_COMPACT=1", "LOGET_CRASH_WAITACK=1")
}

func s3CrashURL(t *testing.T) string {
	raw := os.Getenv("LOGET_TEST_S3")
	if raw == "" {
		t.Skip("LOGET_TEST_S3 not set")
	}
	u, err := url.Parse(raw)
	if err != nil {
		t.Fatal(err)
	}
	u.Path += fmt.Sprintf("/crash-%d", time.Now().UnixNano())
	return u.String()
}

// The journal store keeps every segment in one file; same invariants.
func TestCrashKill9Journal(t *testing.T) { crashKill9(t, "journal://"+t.TempDir()) }

// Same, with the child compacting (grace 0) in a loop while it commits.
func TestCrashKill9Compact(t *testing.T) {
	crashKill9(t, "fs://"+t.TempDir(), "LOGET_CRASH_COMPACT=1")
}

// Design 28: kills while compactions share value chunks of the previous
// base, copy the rest and list the dead ones as garbage.
func TestCrashKill9CompactShared(t *testing.T) {
	crashKill9(t, "fs://"+t.TempDir(), "LOGET_CRASH_COMPACT=1", "LOGET_CRASH_SMALLCHUNKS=1")
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
// crashSig is what one child has printed so far (the parent waits on c).
type crashSig struct {
	mu                   sync.Mutex
	c                    sync.Cond
	ack, ing, base, ckpt int
	firstAck             time.Time
}

// waitCrashSig blocks until pred (called with sig.mu held) is true, the
// child exits, or crashWait times out. A timeout is a test failure: the
// coverage assertion must not pass by luck on a fast box and fail on a
// slow one.
const crashWait = 60 * time.Second

func waitCrashSig(t *testing.T, it int, cmd *exec.Cmd, sig *crashSig, done <-chan struct{}, pred func() bool) {
	t.Helper()
	deadline := time.Now().Add(crashWait)
	fail := func(why string) {
		if cmd.Process != nil {
			cmd.Process.Kill()
		}
		t.Fatalf("iter %d: %s (ack=%d ing=%d base=%d ckpt=%d)", it, why, sig.ack, sig.ing, sig.base, sig.ckpt)
	}
	sig.mu.Lock()
	defer sig.mu.Unlock()
	for !pred() {
		if time.Now().After(deadline) {
			fail("timed out waiting for crash coverage")
		}
		select {
		case <-done:
			if pred() {
				return
			}
			fail("child exited before crash coverage")
		default:
		}
		// Wake at least every 5ms so a TTL span can elapse without
		// another line, and so a timeout is not stuck in Wait.
		timer := time.AfterFunc(5*time.Millisecond, func() {
			sig.mu.Lock()
			sig.c.Broadcast()
			sig.mu.Unlock()
		})
		sig.c.Wait()
		timer.Stop()
	}
}

// reapCrashRunFiles removes loget-kv-*.run left in dir when SIGKILL landed
// between CreateTemp and unlink. That window exists only on the
// createUnlinked path (macOS and any Linux file system without O_TMPFILE).
// Linux O_TMPFILE never names a file, so leftovers there stay for noFiles
// to fail on.
func reapCrashRunFiles(dir string) {
	if runtime.GOOS == "linux" {
		return
	}
	ents, err := os.ReadDir(dir)
	if err != nil {
		return
	}
	for _, e := range ents {
		n := e.Name()
		if strings.HasPrefix(n, "loget-kv-") && strings.HasSuffix(n, ".run") {
			os.Remove(filepath.Join(dir, n))
		}
	}
}

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
	needING := slices.Contains(env, "LOGET_CRASH_INGEST=1")
	// LOGET_CRASH_WAITACK: kill only after this cycle's first ack, so a
	// slow store (S3 latency: open alone takes about a second) still
	// crashes mid-workload instead of mid-open every time.
	needACK := slices.Contains(env, "LOGET_CRASH_WAITACK=1")
	needBASE := len(env) > 0 && !(needACK && len(env) == 1)
	needCKPT := slices.Contains(env, "LOGET_CRASH_CKPT=1")
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	for it := 0; it < iters; it++ {
		cmd := exec.Command(os.Args[0], "-test.run=^$")
		cmd.Env = append(append(os.Environ(), env...), "LOGET_CRASH_CHILD="+dir)
		out, _ := cmd.StdoutPipe()
		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}
		done := make(chan struct{})
		var sig crashSig
		sig.c.L = &sig.mu
		go func() {
			sc := bufio.NewScanner(out)
			for sc.Scan() {
				f := strings.Fields(sc.Text())
				sig.mu.Lock()
				switch {
				case len(f) == 1 && f[0] == "BASE":
					sig.base++
				case len(f) == 1 && f[0] == "CKPT":
					sig.ckpt++
				case len(f) == 2 && f[0] == "ING":
					ingested[f[1]] = true
					sig.ing++
				case len(f) == 3 && f[0] == "ACK":
					g, _ := strconv.Atoi(f[1])
					i, _ := strconv.Atoi(f[2])
					if i > acked[g] {
						acked[g] = i
					}
					sig.ack++
					if sig.firstAck.IsZero() {
						sig.firstAck = time.Now()
					}
				default:
					t.Errorf("child: %s", sc.Text())
				}
				sig.c.Broadcast()
				sig.mu.Unlock()
			}
			sig.mu.Lock()
			sig.c.Broadcast()
			sig.mu.Unlock()
			close(done)
		}()
		// Coverage first: do not kill until the child has acknowledged the
		// event this test asserts on (an ingest, a compaction base, a
		// checkpoint, TTL keys spanning crashTTL). A later random pause
		// still lands SIGKILL mid-operation. Once that event has been
		// recovered, later cycles keep the original random kill window so
		// early deaths stay in the mix.
		//
		// Compaction drops the index checkpoint when it commits a new
		// base, so a cycle that still needs a checkpoint waits for ING
		// and BASE first, then for a CKPT after that, and kills promptly
		// so a later compact cannot drop it before we reopen.
		if needING && sawIngest == 0 {
			waitCrashSig(t, it, cmd, &sig, done, func() bool { return sig.ing > 0 })
		}
		if needBASE && based == 0 {
			waitCrashSig(t, it, cmd, &sig, done, func() bool { return sig.base > 0 })
		}
		killQuick := false
		if needCKPT && ckpted == 0 {
			sig.mu.Lock()
			after := sig.ckpt
			sig.mu.Unlock()
			waitCrashSig(t, it, cmd, &sig, done, func() bool { return sig.ckpt > after })
			killQuick = true
		}
		if ttlOn && (ttlExpired == 0 || ttlSeen == 0) {
			waitCrashSig(t, it, cmd, &sig, done, func() bool {
				return sig.ack >= 2 && !sig.firstAck.IsZero() && time.Since(sig.firstAck) >= crashTTL
			})
		}
		if needACK {
			waitCrashSig(t, it, cmd, &sig, done, func() bool { return sig.ack > 0 })
		}
		if killQuick {
			time.Sleep(time.Duration(rng.Intn(5)) * time.Millisecond)
		} else {
			time.Sleep(time.Duration(20+rng.Intn(150)) * time.Millisecond)
		}
		cmd.Process.Kill() // SIGKILL mid-write
		cmd.Wait()
		<-done
		var recOpts []Option
		for _, e := range env {
			if d, ok := strings.CutPrefix(e, "LOGET_CRASH_INDEXDIR="); ok {
				reapCrashRunFiles(d)
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
	if needBASE && based == 0 {
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
