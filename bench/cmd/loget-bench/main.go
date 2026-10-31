// Command loget-bench is LogEt's reproducible benchmark suite. Every
// subcommand writes one JSON file to -out; `loget-bench report` turns a
// results directory into markdown. See docs/benchmarks.md. (The topic, NATS and
// vector suites live on the index/topics and index/vectors branches.)
package main

import (
	"fmt"
	"os"
)

const usage = `loget-bench <suite> [flags]

suites:
  ycsb     KV workloads A,B,C,D,F (zipfian) on loget|badger|pebble|bbolt|sqlite
  txn      contended OCC transactions (loget vs badger)
  s3       commit latency, batching, requests/commit and cold open on S3/MinIO
  compact  storage, open time and heap before/after compaction (overwrite-heavy)
  commit   commit latency tails on the journal (8 MiB file growth steps)
  gc       disk size per cycle under overwrite + branch + rewind churn: compact vs gc vs gc+shrink
  calib    harness + VM noise floor (no-op latency)
  report   render results/*.json as markdown

run "loget-bench <suite> -h" for flags.`

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, usage)
		os.Exit(2)
	}
	args := os.Args[2:]
	switch os.Args[1] {
	case "ycsb":
		cmdYCSB(args)
	case "txn":
		cmdTxn(args)
	case "s3":
		cmdS3(args)
	case "compact":
		cmdCompact(args)
	case "gc":
		cmdGC(args)
	case "commit":
		cmdCommit(args)
	case "calib":
		cmdCalib(args)
	case "report":
		cmdReport(args)
	default:
		fmt.Fprintln(os.Stderr, usage)
		os.Exit(2)
	}
}

func wipe(dir string) { os.RemoveAll(dir) }
