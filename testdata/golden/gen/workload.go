package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math/rand"
	"strings"

	"github.com/raman20/loget"
	"github.com/raman20/loget/index"
)

// --- workload begin ---
// (testdata/golden/gen/workload.go holds an identical copy; a release's
// generator builds that release's golden database with it.)

// notesKind is a record store: records in spaces "n:*", read back through
// the core record API. It has no state of its own.
func notesKind() index.Kind { return index.Kind{Name: "notes", Version: 1, Spaces: []string{"n:"}} }

func goldenOpts() []loget.Option {
	var now int64 = 1_800_000_000_000_000_000
	return []loget.Option{
		loget.WithClock(func() int64 { now += 1_000_000; return now }),
		loget.WithRand(rand.New(rand.NewSource(1))),
		loget.WithSnapshotEvery(10),
		loget.WithLogf(func(string, ...any) {}),
		loget.WithIndex(notesKind()),
	}
}

func goldenWorkload(ctx context.Context, db *loget.DB) error {
	for i := 0; i < 40; i++ {
		err := db.Update(ctx, func(tx *loget.Tx) error {
			v := []byte(fmt.Sprint("v", i))
			if i%3 == 0 {
				v = bytes.Repeat(v, 40) // over 64 B: kept out of line
			}
			if err := tx.Put(fmt.Sprintf("k%02d", i%25), v); err != nil {
				return err
			}
			if i%7 == 6 {
				if err := tx.Delete(fmt.Sprintf("k%02d", i%25-1)); err != nil {
					return err
				}
			}
			if i%9 == 8 {
				if err := tx.DeleteRecord("n:users", fmt.Sprint("u", i%5)); err != nil {
					return err
				}
			}
			return tx.PutRecord("n:users", fmt.Sprint("u", i%5+i%2), []byte(fmt.Sprint("team", i%2, "-", i)))
		})
		if err != nil {
			return err
		}
	}
	err := db.Update(ctx, func(tx *loget.Tx) error {
		tx.Put("alice/email", []byte("alice@example.com"), loget.Subject("alice"))
		tx.Put("bob/email", []byte("bob@example.com"), loget.Subject("bob"))
		return tx.PutRecord("n:audit", "alice-1", []byte("alice did x"), loget.Subject("alice"))
	})
	if err != nil {
		return err
	}
	if _, err := db.Shred(ctx, "alice"); err != nil {
		return err
	}
	if err := db.CreateBranch(ctx, "exp", "main", 20); err != nil {
		return err
	}
	return db.Branch("exp").Update(ctx, func(tx *loget.Tx) error {
		tx.Put("exp-only", []byte("x"))
		tx.PutRecord("n:users", "u9", []byte("exp"))
		return tx.Delete("k01")
	})
}

// goldenDump prints everything a reader can see, in a fixed order.
func goldenDump(ctx context.Context, db *loget.DB) (string, error) {
	var b strings.Builder
	for _, br := range []string{"main", "exp"} {
		head, err := db.Branch(br).Head()
		if err != nil {
			return "", err
		}
		fmt.Fprintf(&b, "branch %s head %d\n", br, head)
		err = db.Branch(br).View(ctx, func(tx *loget.Tx) error {
			kvs, err := tx.Scan("")
			if err != nil {
				return err
			}
			for _, kv := range kvs {
				fmt.Fprintf(&b, "kv %s = %q\n", kv.Key, kv.Value)
			}
			for _, k := range []string{"alice/email", "bob/email"} {
				v, err := tx.Get(k)
				fmt.Fprintf(&b, "get %s = %q shredded=%v\n", k, v, errors.Is(err, loget.ErrShredded))
			}
			for _, sp := range []string{"n:users", "n:audit"} {
				rs, err := tx.ScanRecords(sp, "")
				if err != nil {
					return err
				}
				for _, r := range rs {
					fmt.Fprintf(&b, "record %s %s = %q\n", sp, r.Key, r.Value)
				}
			}
			_, err = tx.GetRecord("n:audit", "alice-1")
			fmt.Fprintf(&b, "get n:audit alice-1 notfound=%v\n", errors.Is(err, loget.ErrNotFound))
			return nil
		})
		if err != nil {
			return "", err
		}
	}
	fmt.Fprintf(&b, "shredded alice=%v bob=%v\n", db.IsShredded("alice"), db.IsShredded("bob"))
	rep, err := db.Verify(ctx)
	if err != nil {
		return "", err
	}
	fmt.Fprintf(&b, "verify ok=%v\n", rep.OK)
	return b.String(), nil
}

// --- workload end ---
