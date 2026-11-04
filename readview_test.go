package loget

import (
	"errors"
	"fmt"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/raman20/loget/store"
)

func TestSnapshotView(t *testing.T) {
	db := mustOpen(t, store.NewMem())
	defer db.Close()
	for i := 0; i < 5; i++ {
		db.Put(ctx, fmt.Sprint("k", i), []byte("old"))
	}
	s, err := db.Snapshot(ctx, SnapshotOptions{})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 5; i++ {
		db.Put(ctx, fmt.Sprint("k", i), []byte("new"))
	}
	db.Put(ctx, "k9", []byte("new"))
	// many reads and iterators, in several transactions, see one state
	for round := 0; round < 3; round++ {
		err := s.View(ctx, func(tx *Tx) error {
			it := tx.Iter(IterOptions{})
			n := 0
			for it.Next() {
				if string(it.Value()) != "old" {
					return fmt.Errorf("iter %s = %s", it.Key(), it.Value())
				}
				n++
			}
			if it.Err() != nil || n != 5 {
				return fmt.Errorf("iter: %d keys, %v", n, it.Err())
			}
			v, err := tx.Get("k3")
			if err != nil || string(v) != "old" {
				return fmt.Errorf("get: %q %v", v, err)
			}
			if tx.At() != s.LSN() {
				return fmt.Errorf("at %d, snapshot %d", tx.At(), s.LSN())
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	if db.LiveSnapshots() != 1 {
		t.Fatalf("live %d", db.LiveSnapshots())
	}
	s.Release()
	s.Release()
	if err := s.View(ctx, func(*Tx) error { return nil }); !errors.Is(err, ErrSnapshotReleased) {
		t.Fatalf("released: %v", err)
	}
	if db.LiveSnapshots() != 0 {
		t.Fatalf("live %d", db.LiveSnapshots())
	}
	if _, err := db.Snapshot(ctx, SnapshotOptions{At: 1 << 40}); !errors.Is(err, ErrBadLSN) {
		t.Fatalf("future LSN: %v", err)
	}
}

// TestSnapshotPinsCompaction: a live snapshot holds its log's base back;
// release or age lets compaction pass, and reads then fail ErrSnapshotTooOld.
func TestSnapshotPinsCompaction(t *testing.T) {
	var clk atomic.Int64
	clk.Store(1000)
	db := mustOpen(t, store.NewMem(), WithClock(clk.Load), WithSnapshotMaxAge(50))
	defer db.Close()
	db.Put(ctx, "a", []byte("1"))
	s, err := db.Snapshot(ctx, SnapshotOptions{})
	if err != nil {
		t.Fatal(err)
	}
	db.Put(ctx, "a", []byte("2"))
	st, err := db.Compact(ctx, CompactOptions{})
	if err != nil || st.Pinned != 1 || st.Logs != 0 {
		t.Fatalf("pinned compaction: %+v %v", st, err)
	}
	read := func() error {
		return s.View(ctx, func(tx *Tx) error {
			v, err := tx.Get("a")
			if err == nil && string(v) != "1" {
				err = fmt.Errorf("a = %q", v)
			}
			return err
		})
	}
	if err := read(); err != nil {
		t.Fatalf("after pinned compaction: %v", err)
	}
	clk.Add(100) // the snapshot ages out
	if err := read(); !errors.Is(err, ErrSnapshotTooOld) {
		t.Fatalf("aged: %v", err)
	}
	st, err = db.Compact(ctx, CompactOptions{})
	if err != nil || st.Pinned != 0 || st.Logs != 1 {
		t.Fatalf("compaction past an aged snapshot: %+v %v", st, err)
	}
	s.Release()
	// below the base now
	if _, err := db.Snapshot(ctx, SnapshotOptions{At: s.LSN()}); !errors.Is(err, ErrSnapshotTooOld) {
		t.Fatalf("below base: %v", err)
	}
	// a released snapshot does not pin
	db.Put(ctx, "a", []byte("3"))
	s2, _ := db.Snapshot(ctx, SnapshotOptions{})
	db.Put(ctx, "a", []byte("4"))
	s2.Release()
	if st, err := db.Compact(ctx, CompactOptions{}); err != nil || st.Pinned != 0 || st.Logs != 1 {
		t.Fatalf("after release: %+v %v", st, err)
	}
}

// TestSnapshotRace: snapshots at random LSNs created, read and released
// while compactions run. A young, unreleased snapshot that was created
// successfully must always read its exact state.
func TestSnapshotRace(t *testing.T) {
	db := mustOpen(t, store.NewMem(), WithSnapshotMaxAge(time.Hour))
	defer db.Close()
	var lsns sync.Map // lsn -> value of "x" at it
	put := func(i int) {
		lsn, err := db.Branch("main").UpdateLSN(ctx, func(tx *Tx) error { return tx.Put("x", []byte(fmt.Sprint(i))) })
		if err != nil {
			t.Error(err)
		}
		lsns.Store(lsn, fmt.Sprint(i))
	}
	for i := 0; i < 20; i++ {
		put(i)
	}
	var wg sync.WaitGroup
	stop := make(chan struct{})
	var made, tooOld, pinned, based atomic.Int64
	for g := 0; g < 4; g++ {
		wg.Add(1)
		go func(seed uint64) {
			defer wg.Done()
			rng := rand.New(rand.NewPCG(seed, 1))
			for {
				select {
				case <-stop:
					return
				default:
				}
				head, _ := db.Branch("main").Head()
				at := head - uint64(rng.IntN(5))
				want, ok := lsns.Load(at)
				if !ok {
					continue
				}
				s, err := db.Snapshot(ctx, SnapshotOptions{At: at})
				if errors.Is(err, ErrSnapshotTooOld) {
					tooOld.Add(1)
					continue
				}
				if err != nil {
					t.Error(err)
					return
				}
				made.Add(1)
				for r := 0; r < 3; r++ {
					err := s.View(ctx, func(tx *Tx) error {
						v, err := tx.Get("x")
						if err == nil && string(v) != want {
							err = fmt.Errorf("x at %d = %s, want %s", at, v, want)
						}
						return err
					})
					if err != nil {
						t.Error(err)
						return
					}
				}
				s.Release()
				time.Sleep(time.Duration(rng.IntN(300)) * time.Microsecond) // let compactions pass
			}
		}(uint64(g))
	}
	for i := 20; i < 220; i++ {
		put(i)
		time.Sleep(100 * time.Microsecond)
		if i%10 == 0 {
			st, err := db.Compact(ctx, CompactOptions{})
			if err != nil && !errors.Is(err, ErrBusy) {
				t.Error(err)
			}
			pinned.Add(int64(st.Pinned))
			based.Add(int64(st.Logs))
		}
	}
	close(stop)
	wg.Wait()
	if made.Load() == 0 {
		t.Fatal("no snapshot was made")
	}
	t.Logf("snapshots made %d, refused too old %d; compactions pinned %d, based %d", made.Load(), tooOld.Load(), pinned.Load(), based.Load())
}
