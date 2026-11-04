# 2026-11-04 memory at 10M keys (design 25)

`run.sh BIN OUT [flags]`: 10M x 100 B, YCSB C then A, 8 clients, 10 s each.

- base: HEAD before design 25 (256 MiB value cache)
- dn: + MADV_DONTNEED after run seal
- vc16 / vc32 / vc64: dn binary with `-value-cache` 16/32/64 MiB
- s1: this commit, default options (32 MiB cache on local stores)
- peb: Pebble, default options

See docs/design/25-memory.md for the tables.
