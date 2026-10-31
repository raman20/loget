// Command tour is a runnable walk through the LogEt Go API. Every output
// shown in the README's "API tour" comes from running this program:
//
//	go run ./examples/tour
package main

import (
	"context"
	"fmt"
	"log"

	"github.com/raman20/loget"
	"github.com/raman20/loget/examples/counter"
)

func must(err error) {
	if err != nil {
		log.Fatal(err)
	}
}

func main() {
	ctx := context.Background()
	// One URL: a directory, s3://bucket/prefix?endpoint=..., or mem:// for tests.
	db, err := loget.Open(ctx, "mem://tour", loget.WithLogf(func(string, ...any) {}), loget.WithIndex(counter.Kind()))
	must(err)
	defer db.Close()

	// KV
	must(db.Put(ctx, "config/model", []byte("small")))
	v, _ := db.Get(ctx, "config/model")
	fmt.Printf("kv: config/model = %s\n", v)

	// Transactions: serializable, optimistic. Reads are checked at
	// commit; a conflicting commit is retried by Update.
	must(db.Update(ctx, func(tx *loget.Tx) error {
		tx.Put("user/alice/email", []byte("alice@example.com"), loget.Subject("alice"))
		tx.Put("user/bob/email", []byte("bob@example.com"), loget.Subject("bob"))
		return tx.Put("users", []byte("2"))
	}))
	must(db.View(ctx, func(tx *loget.Tx) error {
		kvs, err := tx.Scan("user/")
		fmt.Printf("scan user/: %d keys, first %s\n", len(kvs), kvs[0].Key)
		return err
	}))

	// An index: examples/counter keeps live counts per value as it folds
	// the log (docs/index-authoring.md).
	must(db.Update(ctx, func(tx *loget.Tx) error {
		counter.Set(tx, "lunch", "alice", "tacos", loget.Subject("alice"))
		return counter.Set(tx, "lunch", "bob", "soup", loget.Subject("bob"))
	}))
	must(db.View(ctx, func(tx *loget.Tx) error {
		c, err := counter.Counts(tx, "lunch")
		fmt.Printf("index: lunch votes %v\n", c)
		return err
	}))

	// Time travel, branches, rewind.
	main := db.Branch("main")
	good, _ := main.Head()
	must(db.Put(ctx, "config/model", []byte("oops-huge")))
	must(main.ViewAt(ctx, good, func(tx *loget.Tx) error {
		old, err := tx.Get("config/model")
		fmt.Printf("time travel: at LSN %d config/model = %s\n", good, old)
		return err
	}))
	must(db.CreateBranch(ctx, "experiment", "main", good))
	must(db.Branch("experiment").Update(ctx, func(tx *loget.Tx) error { return tx.Put("config/model", []byte("medium")) }))
	must(db.Rewind(ctx, "main", good))
	v, _ = db.Get(ctx, "config/model")
	fmt.Printf("rewind: main config/model = %s (old head kept in reflog)\n", v)
	must(db.Branch("experiment").View(ctx, func(tx *loget.Tx) error {
		e, err := tx.Get("config/model")
		fmt.Printf("branch: experiment config/model = %s\n", e)
		return err
	}))

	// Shred: alice's data becomes unreadable everywhere, provably.
	cert, err := db.Shred(ctx, "alice")
	must(err)
	fmt.Printf("shred: certificate for LSN %d, DEK deleted=%v, signed=%v\n", cert.ShredLSN, cert.DEKDeleted, cert.Signature != "")
	must(db.View(ctx, func(tx *loget.Tx) error {
		_, err := tx.Get("user/alice/email")
		c, _ := counter.Counts(tx, "lunch")
		fmt.Printf("after shred: user/alice/email -> %v, lunch votes %v\n", err, c)
		return nil
	}))
	err = db.Update(ctx, func(tx *loget.Tx) error { return tx.Put("x", []byte("y"), loget.Subject("alice")) })
	fmt.Printf("after shred: new write for alice -> %v\n", err)
	rep, err := db.Verify(ctx)
	must(err)
	fmt.Printf("verify: ok=%v\n", rep.OK)
}
