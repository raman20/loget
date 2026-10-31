# Name check: "EnDB" / "endb" (2026-10-30, ~22:00 IST)

Method: web search, GitHub API, npm / PyPI / crates.io registries,
proxy.golang.org, Docker Hub, HTTPS probes. Local DNS on this box is
proxied, so domain results are only "something answers".

## Hard conflict

**Endatabas / Endb** — https://github.com/endatabas/endb, https://www.endatabas.com,
https://docs.endatabas.com
- "SQL document database with full history" (immutable records, ERASE
  for GDPR, Arrow columnar storage, HTAP ambitions), by Håkan Råberg and
  Steven Deobald, AGPL-3.0, 280 GitHub stars, beta.
- Its binary is `endb`, its Docker image is `endatabas/endb` (2,340 pulls),
  and it has a PyPI package `endb` ("Endatabas Python Client", 0.2.0b1)
  plus an npm client.
- Its own docs explain the name the same way: "In Swedish, Endatabas means
  both 'a database' and 'one database'", and describe it as "one giant log
  of transactions" with indexes on top. That is the same pitch.
- Activity: last push 2024-06-17 (repo updated 2026-07-18); not archived.
  It is quiet but alive, and the name, binary, packages and story are taken.

## Other uses of the name

- npm `endb`: "Key-value storage for multiple databases" (1.3.0, last
  modified 2022-05-01). This is a KV package with the same name.
- GitHub: 322 repos match "endb", the most-starred being endbasic/endbasic
  (382 stars, unrelated) and endatabas/endb.
- crates.io `endb`: free. proxy.golang.org `github.com/raman20/endb`:
  unused. github.com/raman20/endb: free.
- endb.com and endb.org answer over HTTPS (registered); endb.io and
  endb.dev did not answer (may be free; not verified).

## Assessment

This is a blocker: another open-source database already uses this exact
name and binary, the same Scandinavian "one database" meaning and the same
one-log idea, so users and search engines would confuse the two. Choosing
the name is Raman's call.

Unclaimed alternatives with the same meaning (quick check only, not a
trademark search):

| name | meaning | GitHub | npm | crates |
|---|---|---|---|---|
| EttDB | Swedish "ett" = one (neuter) | 1 repo, 0 stars | free | free |
| EttLog | same | none | free | free |
| EtDB | Danish/Norwegian "et" = one | 45 repos (biology ETDB, a small in-memory db) | free | free |
| EnLog | "one log" | 49 small repos | free | free |

## Outcome: EnDB rejected, LogEt chosen (2026-10-30, ~22:00 IST)

EnDB was rejected because of the Endatabas/Endb conflict above: same
name, same binary name (`endb`), same "one database" meaning, and the
same "one log with indexes on top" pitch. A second project with that
name would confuse users and search results, and could raise trademark
questions.

## Name check: "LogEt" / "loget" (2026-10-31, ~21:00 IST)

LogEt is the final name (Raman, 2026-10-31). Danish/Norwegian "loget"
is "the log" (log + the suffixed article "-et"), and "et" alone also
means "one": one log, every lens. Prose: LogEt. Go module
`github.com/raman20/loget`, binary `loget`, env vars `LOGET_*`, metrics
`loget_*`.

Checked on 2026-10-31 (~21:00 IST) from the box:

| where | result |
|---|---|
| npm `loget` | free (registry 404) |
| crates.io `loget` | free (404) |
| PyPI `loget` | free (404) |
| github.com/raman20/loget | free (404); not created, Raman decides when it goes public |
| proxy.golang.org `github.com/raman20/loget` | no versions (404) |
| GitHub repo search "loget" in name | 53 hits, all tiny and unrelated (top: a 2-star RPL routing project, a 1-star blog tool `Matt-SD/Loget`); no database or log engine |

Not a trademark search. Risk: English readers may say "lo-get"; the
README tagline ("One log, different indexes over it.") and the
meaning line ("the log"; "et" = one) say what it means.

## Names considered and dropped

All of these were working names only; none was released or published.

- **EnDB**: rejected for the Endatabas/Endb conflict above (same name,
  binary, packages and "one database / one log" pitch).
- **Superlog**: `superlog` is taken on npm and PyPI by an observability
  tool.
- **UrLog**, **CrazyLog**, **EnLog**: interim names, replaced by LogEt.

## Lineage

LogEt evolved from freshDB (github.com/raman20/freshdb, last commit
5a08052), which began as a rewrite of an earlier prototype called goli.
