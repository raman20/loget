package loget

import (
	"errors"
	"fmt"
	"testing"

	"github.com/raman20/loget/store"
	"pgregory.net/rapid"
)

var errAborted = errors.New("aborted after a conflict")

// occTx is an Update held open inside fn until the test lets it commit.
type occTx struct {
	reads, writes []string
	at            uint64
	ready, commit chan struct{}
	conflicted    bool
	lsn           uint64
	done          chan error
}

// The recent-commit window never misses a conflict (design 23, part 3).
// Overlapping Update attempts are held open inside fn and committed in
// a random order between blind commits; each first attempt must get
// ErrConflict exactly when a reference model says a commit after its
// snapshot wrote a key it read. With the default budgets there are no
// false conflicts either; with a tiny budget (floor raised) a false
// conflict is allowed, a missed one never. When nothing is open the
// window holds at most the last commit.
func TestOCCWindowNoMissedConflicts(t *testing.T) {
	t.Run("default", func(t *testing.T) { rapid.Check(t, func(rt *rapid.T) { occModel(rt, false) }) })
	t.Run("tiny-budget", func(t *testing.T) {
		m, b := recentMax, recentMaxBytes
		recentMax, recentMaxBytes = 2, 300
		defer func() { recentMax, recentMaxBytes = m, b }()
		rapid.Check(t, func(rt *rapid.T) { occModel(rt, true) })
	})
}

func occModel(t *rapid.T, budget bool) {
	db, err := OpenStore(ctx, store.NewMem(), quiet(), WithIndex(tallyKind()))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	type commit struct {
		lsn    uint64
		writes []string
	}
	var commits []commit
	keyGen := rapid.SampledFrom([]string{"a", "b", "c", "d", "e"})
	keys := rapid.SliceOfNDistinct(keyGen, 0, 3, rapid.ID[string])
	var open []*occTx
	n := 0
	finish := func(tx *occTx) {
		close(tx.commit)
		err := <-tx.done
		want := false
		for _, c := range commits {
			if c.lsn <= tx.at {
				continue
			}
			for _, w := range c.writes {
				for _, r := range tx.reads {
					want = want || w == r
				}
			}
		}
		switch {
		case want && !tx.conflicted:
			t.Fatalf("missed conflict: read %v at %d, commits %v", tx.reads, tx.at, commits)
		case !want && tx.conflicted && !budget:
			t.Fatalf("false conflict: read %v at %d, commits %v", tx.reads, tx.at, commits)
		case tx.conflicted && !errors.Is(err, errAborted):
			t.Fatalf("conflicted, then %v", err)
		case !tx.conflicted && err != nil:
			t.Fatal(err)
		}
		if err == nil {
			commits = append(commits, commit{tx.lsn, tx.writes})
		}
	}
	steps := rapid.IntRange(1, 40).Draw(t, "steps")
	for s := 0; s < steps; s++ {
		switch rapid.IntRange(0, 2).Draw(t, "op") {
		case 0: // open an attempt
			if len(open) >= 4 {
				continue
			}
			tx := &occTx{reads: keys.Draw(t, "reads"), writes: keys.Draw(t, "writes"),
				ready: make(chan struct{}), commit: make(chan struct{}), done: make(chan error, 1)}
			if len(tx.writes) == 0 {
				tx.writes = []string{"z"}
			}
			calls := 0
			go func() {
				lsn, err := db.Branch("main").UpdateLSN(ctx, func(x *Tx) error {
					if calls++; calls > 1 {
						tx.conflicted = true
						return errAborted
					}
					tx.at = x.At()
					for _, k := range tx.reads {
						if _, err := x.Get(k); err != nil && !errors.Is(err, ErrNotFound) {
							return err
						}
					}
					for _, k := range tx.writes {
						if err := x.Put(k, []byte("v")); err != nil {
							return err
						}
					}
					close(tx.ready)
					<-tx.commit
					return nil
				})
				tx.lsn = lsn
				tx.done <- err
			}()
			<-tx.ready
			open = append(open, tx)
		case 1: // commit an open attempt
			if len(open) == 0 {
				continue
			}
			i := rapid.IntRange(0, len(open)-1).Draw(t, "which")
			tx := open[i]
			open = append(open[:i], open[i+1:]...)
			finish(tx)
		case 2: // blind writes, committed at once
			ws := keys.Draw(t, "blind")
			if len(ws) == 0 {
				continue
			}
			n++
			lsn, err := db.Branch("main").UpdateLSN(ctx, func(x *Tx) error {
				for _, k := range ws {
					if err := x.Put(k, []byte(fmt.Sprint(n))); err != nil {
						return err
					}
				}
				return nil
			})
			if err != nil {
				t.Fatal(err)
			}
			commits = append(commits, commit{lsn, ws})
		}
	}
	for _, tx := range open {
		finish(tx)
	}
	w, err := db.branchWriter("main")
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Put(ctx, "last", []byte("x")); err != nil { // the writer trims after a batch
		t.Fatal(err)
	}
	if b := w.oldestActive(); b != ^uint64(0) {
		t.Fatalf("snapshot %d still registered", b)
	}
	db.Close() // the writer goroutine exits; its fields are safe to read
	// Only "last" itself may remain: its own attempt was open during its batch.
	if len(w.recent) > 1 || w.recentBytes > 200 {
		t.Fatalf("window holds %d commits (%d bytes) with nothing open", len(w.recent), w.recentBytes)
	}
}
