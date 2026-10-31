// Command gen writes a golden database with whatever loget version its
// go.mod points at (replace directive), and the reader-visible dump of it
// taken before close (so the directory is exactly what the workload left).
// Used once per released version: see testdata/golden/README.md.
//
//	cd testdata/golden/gen && go run . ../vX.Y.Z [fs://]
package main

import (
	"context"
	"log"
	"os"
	"path/filepath"

	"github.com/raman20/loget"
)

func main() {
	ctx := context.Background()
	dir, _ := filepath.Abs(os.Args[1])
	scheme := "file://" // the default local URL; pass fs:// for the files layout
	if len(os.Args) > 2 {
		scheme = os.Args[2]
	}
	db, err := loget.Open(ctx, scheme+filepath.Join(dir, "db"), goldenOpts()...)
	if err != nil {
		log.Fatal(err)
	}
	if err := goldenWorkload(ctx, db); err != nil {
		log.Fatal(err)
	}
	d, err := goldenDump(ctx, db)
	if err != nil {
		log.Fatal(err)
	}
	if err := db.Close(); err != nil {
		log.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "dump.txt"), []byte(d), 0o644); err != nil {
		log.Fatal(err)
	}
}
