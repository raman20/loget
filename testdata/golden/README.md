Golden databases for TestGoldenRead / TestGoldenBytes (docs/compatibility.md).

- No released LogEt version exists yet. The freshDB v0.1.0 golden
  database was dropped at the rename: LogEt changed every magic string
  (docs/rebrand/README.md), so freshDB data is not readable by design.
  The first LogEt release adds `vX.Y.Z/` here.

- `main/`, `main-compacted/`: written by TestGoldenBytes with
  `LOGET_UPDATE_GOLDEN=1` (only for an intended format change).

`gen/workload.go` must stay identical to the workload in `golden_test.go`
(TestGoldenWorkloadInSync).
