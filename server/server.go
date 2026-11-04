// Package server exposes a loget DB over a small JSON/HTTP API.
//
// Every endpoint takes ?branch= (default main). Writes accept ?subject= to
// encrypt the value under that subject's key (crypto-shreddable).
//
//	GET    /v1/kv?prefix=&at=           scan            POST /v1/tx (= /v1/batch) atomic batch
//	       &start=&end=&reverse=1&limit=&after=   range / page (Loget-Next-After header)
//	GET    /v1/kv/{key}?at=             read (time travel with at=LSN)
//	PUT    /v1/kv/{key}                 write (body = value)
//	DELETE /v1/kv/{key}
//	GET    /v1/branches   POST /v1/branches {"name","from","at"}
//	POST   /v1/branches/{b}/rewind {"lsn"}   POST /v1/branches/{b}/promote {"to"}
//	DELETE /v1/branches/{b}
//	GET    /v1/log?branch=&from=          POST /v1/shred/{subject}    GET /v1/verify
//	POST   /v1/compact  POST /v1/gc  POST /v1/backup?to=  GET /metrics   (admin)
//
// Only the KV index is served over HTTP. Other indexes are embedded-only
// until they are ported back from their branches (docs/PIPELINE.md).
package server

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"time"

	"github.com/raman20/loget"
	"github.com/raman20/loget/format"
	"github.com/raman20/loget/store"
)

type Server struct {
	DB *loget.DB

	maxBody         int64
	obs             *observer
	pprof           bool
	drainCtx        context.Context // canceled when Options.Drain closes; nil: never
	readTO, writeTO time.Duration
}

func newServer(db *loget.DB, o Options) *Server {
	s := &Server{DB: db, obs: newObserver(o), pprof: o.Pprof && (o.Token != "" || o.Auth != nil)}
	s.maxBody = int64(o.MaxBody)
	if s.maxBody <= 0 {
		s.maxBody = DefaultMaxBody
	}
	to := func(d, def time.Duration) time.Duration {
		if d == 0 {
			return def
		}
		return max(d, 0)
	}
	s.readTO, s.writeTO = to(o.ReadTimeout, DefaultReadTimeout), to(o.WriteTimeout, DefaultWriteTimeout)
	if o.Drain != nil {
		ctx, cancel := context.WithCancel(context.Background())
		go func() { <-o.Drain; cancel() }()
		s.drainCtx = ctx
	}
	return s
}

func (s *Server) draining() bool { return s.drainCtx != nil && s.drainCtx.Err() != nil }

// deadlineBody clears the connection's read deadline once the body is
// read, so it bounds the upload only, not the handler.
type deadlineBody struct {
	io.ReadCloser
	rc *http.ResponseController
}

func (b *deadlineBody) Read(p []byte) (int, error) {
	n, err := b.ReadCloser.Read(p)
	if err == io.EOF { // on a timeout the deadline stays: the server's discard of the rest fails fast too
		b.rc.SetReadDeadline(time.Time{})
	}
	return n, err
}

// writeDeadline (re)arms the response write deadline d from now.
func writeDeadline(w http.ResponseWriter, d time.Duration) {
	if d > 0 {
		http.NewResponseController(w).SetWriteDeadline(time.Now().Add(d))
	}
}

// Handler is the API without auth and with the default limits.
func Handler(db *loget.DB) http.Handler { return newServer(db, Options{}).routes() }

func (s *Server) routes() http.Handler {
	m := http.NewServeMux()
	path := func(name string) func(*http.Request) string {
		return func(r *http.Request) string { return r.PathValue(name) }
	}
	// h registers a route that needs scope on (kind, name(r)); kind ""
	// is database-wide, "tx" checks each op in the handler.
	h := func(pattern string, scope Scope, kind string, name func(*http.Request) string, f http.HandlerFunc) {
		m.HandleFunc(pattern, s.obs.wrap(pattern, func(w http.ResponseWriter, r *http.Request) {
			k, n := kind, ""
			if name != nil {
				n = name(r)
			}
			if err := permit(r, scope, k, n); err != nil {
				fail(w, err)
				return
			}
			if r.Body != nil {
				r.Body = http.MaxBytesReader(w, r.Body, s.maxBody)
				if s.readTO > 0 && r.ContentLength != 0 {
					rc := http.NewResponseController(w)
					if rc.SetReadDeadline(time.Now().Add(s.readTO)) == nil {
						r.Body = &deadlineBody{r.Body, rc}
					}
				}
			}
			if scope != ScopeAdmin {
				writeDeadline(w, s.writeTO)
			}
			f(w, r)
		}))
	}
	const R, W, A = ScopeRead, ScopeWrite, ScopeAdmin
	prefix := func(r *http.Request) string { return r.URL.Query().Get("prefix") }
	h("GET /v1/kv", R, "kv", prefix, s.scan)
	h("GET /v1/kv/{key...}", R, "kv", path("key"), s.get)
	h("PUT /v1/kv/{key...}", W, "kv", path("key"), s.put)
	h("DELETE /v1/kv/{key...}", W, "kv", path("key"), s.del)
	h("POST /v1/tx", W, "tx", nil, s.tx)
	h("POST /v1/batch", W, "tx", nil, s.tx) // same handler: /v1/tx is an atomic batch
	h("GET /v1/branches", R, "", nil, s.branches)
	h("POST /v1/branches", W, "", nil, s.branchCreate)
	h("POST /v1/branches/{b}/rewind", A, "", nil, s.rewind)
	h("POST /v1/branches/{b}/promote", A, "", nil, s.promote)
	h("DELETE /v1/branches/{b}", A, "", nil, s.branchDelete)
	h("GET /v1/log", R, "", nil, s.log)
	h("POST /v1/shred/{subject}", A, "", nil, s.shred)
	h("GET /v1/verify", R, "", nil, s.verify)
	h("POST /v1/compact", A, "", nil, s.compact)
	h("POST /v1/gc", A, "", nil, s.gc)
	h("POST /v1/backup", A, "", nil, s.backup)
	h("GET /metrics", A, "", nil, s.metrics)
	if s.pprof {
		pprofRoutes(func(p string, f http.HandlerFunc) { h(p, A, "", nil, f) })
	}
	return m
}

var errBusy = errors.New("busy")

// ---- helpers ----

func reply(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(v)
}

func fail(w http.ResponseWriter, err error) {
	code := http.StatusInternalServerError
	switch {
	case errors.Is(err, loget.ErrNotFound), errors.Is(err, loget.ErrNoBranch):
		code = http.StatusNotFound
	case errors.Is(err, loget.ErrConflict), errors.Is(err, loget.ErrBranchExists), errors.Is(err, loget.ErrNotFastForward), errors.Is(err, loget.ErrMainBranch), errors.Is(err, loget.ErrBusy):
		code = http.StatusConflict
	case errors.Is(err, loget.ErrShredded), errors.Is(err, loget.ErrCompacted):
		code = http.StatusGone
	case errors.As(err, new(*http.MaxBytesError)), errors.Is(err, loget.ErrTooLarge):
		code = http.StatusRequestEntityTooLarge
	case errors.Is(err, os.ErrDeadlineExceeded):
		code = http.StatusRequestTimeout // --read-timeout while reading the body
	case errors.Is(err, loget.ErrBadLSN), errors.Is(err, loget.ErrBadName), errors.Is(err, loget.ErrBadValue), errors.Is(err, errBad):
		code = http.StatusBadRequest
	case errors.Is(err, loget.ErrFenced), errors.Is(err, errBusy):
		code = http.StatusServiceUnavailable
	case errors.Is(err, errForbidden):
		code = http.StatusForbidden
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
}

var errBad = errors.New("bad request")

func br(r *http.Request) *loget.Branch {
	b := r.URL.Query().Get("branch")
	if b == "" {
		b = "main"
	}
	return r.Context().Value(dbKey{}).(*loget.DB).Branch(b)
}

type dbKey struct{}

func (s *Server) with(r *http.Request) *http.Request {
	return r.WithContext(context.WithValue(r.Context(), dbKey{}, s.DB))
}

func u64(r *http.Request, name string) (uint64, bool, error) {
	v := r.URL.Query().Get(name)
	if v == "" {
		return 0, false, nil
	}
	n, err := strconv.ParseUint(v, 10, 64)
	if err != nil {
		return 0, false, fmt.Errorf("%w: %s", errBad, name)
	}
	return n, true, nil
}

func opts(r *http.Request) []loget.WriteOpt {
	if s := r.URL.Query().Get("subject"); s != "" {
		return []loget.WriteOpt{loget.Subject(s)}
	}
	return nil
}

// view runs fn at ?at= (time travel) or at the head.
func view(r *http.Request, fn func(*loget.Tx) error) error {
	at, ok, err := u64(r, "at")
	if err != nil {
		return err
	}
	b := br(r)
	if ok {
		return b.ViewAt(r.Context(), at, fn)
	}
	return b.View(r.Context(), fn)
}

func update(r *http.Request, fn func(*loget.Tx) error) (uint64, error) {
	return br(r).UpdateLSN(r.Context(), fn)
}

// body reads the request body; the route wrapper caps it (MaxBody), and
// a larger body is an error (413), never a silent truncation.
func body(r *http.Request) ([]byte, error) { return io.ReadAll(r.Body) }

// ---- KV ----

func (s *Server) get(w http.ResponseWriter, r *http.Request) {
	r = s.with(r)
	var v []byte
	err := view(r, func(tx *loget.Tx) (err error) { v, err = tx.Get(r.PathValue("key")); return })
	if err != nil {
		fail(w, err)
		return
	}
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Write(v)
}

func (s *Server) put(w http.ResponseWriter, r *http.Request) {
	r = s.with(r)
	b, err := body(r)
	if err != nil {
		fail(w, err)
		return
	}
	lsn, err := update(r, func(tx *loget.Tx) error { return tx.Put(r.PathValue("key"), b, opts(r)...) })
	if err != nil {
		fail(w, err)
		return
	}
	reply(w, map[string]any{"lsn": lsn})
}

func (s *Server) del(w http.ResponseWriter, r *http.Request) {
	r = s.with(r)
	lsn, err := update(r, func(tx *loget.Tx) error { return tx.Delete(r.PathValue("key")) })
	if err != nil {
		fail(w, err)
		return
	}
	reply(w, map[string]any{"lsn": lsn})
}

type kvOut struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

// scan serves GET /v1/kv: the keys under ?prefix= (the authorised
// scope), optionally narrowed to [start, end), descending with reverse=1,
// at most limit= of them. A full page sets Loget-Next-After to the
// (query-escaped) last key: pass it back as after= for the next page.
func (s *Server) scan(w http.ResponseWriter, r *http.Request) {
	r = s.with(r)
	q := r.URL.Query()
	o := loget.IterOptions{Prefix: q.Get("prefix"), Start: q.Get("start"), End: q.Get("end")}
	switch q.Get("reverse") {
	case "", "0", "false":
	case "1", "true":
		o.Reverse = true
	default:
		fail(w, fmt.Errorf("%w: reverse", errBad))
		return
	}
	if v := q.Get("limit"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 0 {
			fail(w, fmt.Errorf("%w: limit", errBad))
			return
		}
		o.Limit = n
	}
	if q.Has("after") {
		o = o.After(q.Get("after"))
	}
	var out []kvOut
	err := view(r, func(tx *loget.Tx) error {
		it := tx.Iter(o)
		for it.Next() {
			out = append(out, kvOut{it.Key(), string(it.Value())})
		}
		return it.Err()
	})
	if err != nil {
		fail(w, err)
		return
	}
	if o.Limit > 0 && len(out) == o.Limit {
		w.Header().Set("Loget-Next-After", url.QueryEscape(out[len(out)-1].Key))
	}
	reply(w, out)
}

// TxOp is one operation of an atomic POST /v1/tx batch.
type TxOp struct {
	Op      string          `json:"op"` // put or del
	Key     string          `json:"key,omitempty"`
	Value   json.RawMessage `json:"value,omitempty"` // a JSON string (its bytes) or any other JSON value (its raw text)
	Subject string          `json:"subject,omitempty"`
}

func asBytes(raw json.RawMessage) []byte {
	var s string
	if json.Unmarshal(raw, &s) == nil {
		return []byte(s)
	}
	return raw
}

func (s *Server) tx(w http.ResponseWriter, r *http.Request) {
	r = s.with(r)
	var ops []TxOp
	if err := json.NewDecoder(r.Body).Decode(&ops); err != nil {
		fail(w, fmt.Errorf("%w: %w", errBad, err))
		return
	}
	for _, o := range ops {
		if err := permit(r, ScopeWrite, "kv", o.Key); err != nil {
			fail(w, err)
			return
		}
	}
	// One atomic batch of blind writes (loget.Batch, design 26): one log
	// frame, never an OCC conflict, never retried.
	var b loget.Batch
	for _, o := range ops {
		var wo []loget.WriteOpt
		if o.Subject != "" {
			wo = append(wo, loget.Subject(o.Subject))
		}
		var err error
		switch o.Op {
		case "put":
			err = b.Put(o.Key, asBytes(o.Value), wo...)
		case "del":
			err = b.Delete(o.Key)
		default:
			err = fmt.Errorf("%w: unknown op %q", errBad, o.Op)
		}
		if err != nil {
			fail(w, err)
			return
		}
	}
	lsn, err := br(r).Apply(r.Context(), &b)
	if err != nil {
		fail(w, err)
		return
	}
	reply(w, map[string]any{"lsn": lsn, "ops": len(ops)})
}

// ---- branches ----

func (s *Server) branches(w http.ResponseWriter, r *http.Request) {
	m := s.DB.Manifest()
	reply(w, map[string]any{"heads": s.DB.Branches(), "refs": m.Refs, "reflog": m.Reflog, "epoch": m.Epoch})
}

func (s *Server) branchCreate(w http.ResponseWriter, r *http.Request) {
	var q struct {
		Name, From string
		At         uint64
	}
	if err := json.NewDecoder(r.Body).Decode(&q); err != nil {
		fail(w, fmt.Errorf("%w: %w", errBad, err))
		return
	}
	if q.From == "" {
		q.From = "main"
	}
	if err := s.DB.CreateBranch(r.Context(), q.Name, q.From, q.At); err != nil {
		fail(w, err)
		return
	}
	reply(w, map[string]any{"ok": true, "heads": s.DB.Branches()})
}

func (s *Server) rewind(w http.ResponseWriter, r *http.Request) {
	var q struct{ LSN uint64 }
	if err := json.NewDecoder(r.Body).Decode(&q); err != nil {
		fail(w, fmt.Errorf("%w: %w", errBad, err))
		return
	}
	if err := s.DB.Rewind(r.Context(), r.PathValue("b"), q.LSN); err != nil {
		fail(w, err)
		return
	}
	reply(w, map[string]any{"ok": true, "heads": s.DB.Branches()})
}

func (s *Server) branchDelete(w http.ResponseWriter, r *http.Request) {
	if err := s.DB.DeleteBranch(r.Context(), r.PathValue("b")); err != nil {
		fail(w, err)
		return
	}
	reply(w, map[string]any{"ok": true, "heads": s.DB.Branches()})
}

func (s *Server) promote(w http.ResponseWriter, r *http.Request) {
	var q struct{ To string }
	if err := json.NewDecoder(r.Body).Decode(&q); err != nil {
		fail(w, fmt.Errorf("%w: %w", errBad, err))
		return
	}
	if err := s.DB.Promote(r.Context(), r.PathValue("b"), q.To); err != nil {
		fail(w, err)
		return
	}
	reply(w, map[string]any{"ok": true, "heads": s.DB.Branches()})
}

// LogOp / LogEntry: the raw log (ciphertext of subject values is not shown).
type LogOp struct {
	Kind    string `json:"kind"`
	Space   string `json:"space"`
	Key     string `json:"key"`
	Subject string `json:"subject,omitempty"`
	Value   string `json:"value,omitempty"`
}
type LogEntry struct {
	LSN uint64  `json:"lsn"`
	TS  int64   `json:"ts"`
	Log string  `json:"log"`
	Ops []LogOp `json:"ops"`
}

func (s *Server) log(w http.ResponseWriter, r *http.Request) {
	r = s.with(r)
	b := r.URL.Query().Get("branch")
	if b == "" {
		b = "main"
	}
	from, _, _ := u64(r, "from")
	es, err := s.DB.Entries(b)
	if err != nil {
		fail(w, err)
		return
	}
	out := []LogEntry{}
	kinds := map[format.Kind]string{format.OpPut: "put", format.OpDel: "del", format.OpAppend: "append", format.OpShred: "shred", format.OpIngest: "ingest"}
	for _, e := range es {
		if e.LSN < from {
			continue
		}
		le := LogEntry{LSN: e.LSN, TS: e.TS, Log: e.Log}
		for _, op := range e.Ops {
			lo := LogOp{Kind: kinds[op.Kind], Space: op.Space, Key: op.Key, Subject: op.Subject}
			switch {
			case op.Subject != "" && op.Kind != format.OpShred:
				lo.Value = fmt.Sprintf("<encrypted %d bytes, sha256 %s…>", len(op.Value), hex.EncodeToString(sum(op.Value))[:12])
			case len(op.Value) <= 120:
				lo.Value = string(op.Value)
			default:
				lo.Value = fmt.Sprintf("<%d bytes>", len(op.Value))
			}
			le.Ops = append(le.Ops, lo)
		}
		out = append(out, le)
	}
	reply(w, out)
}

// ---- shred & verify ----

func (s *Server) shred(w http.ResponseWriter, r *http.Request) {
	c, err := s.DB.Shred(r.Context(), r.PathValue("subject"))
	if err != nil {
		fail(w, err)
		return
	}
	reply(w, map[string]any{"certificate": c, "valid_signature": loget.VerifyCertificate(c)})
}

// compact runs a compaction; ?grace=DURATION (default 5m) delays deleting
// replaced objects for readers of older manifests.
func (s *Server) compact(w http.ResponseWriter, r *http.Request) {
	o := loget.CompactOptions{Grace: 5 * time.Minute}
	if g := r.URL.Query().Get("grace"); g != "" {
		d, err := time.ParseDuration(g)
		if err != nil || d < 0 {
			fail(w, fmt.Errorf("%w: grace %q", errBad, g))
			return
		}
		o.Grace = d
	}
	st, err := s.DB.Compact(r.Context(), o)
	if err != nil {
		fail(w, err)
		return
	}
	reply(w, st)
}

// backup copies a consistent snapshot to ?to=URL (a store URL the server
// can write: a local path, journal:///, fs:///, s3://). Admin only: the
// server writes wherever it can reach.
func (s *Server) backup(w http.ResponseWriter, r *http.Request) {
	to := r.URL.Query().Get("to")
	if to == "" {
		fail(w, fmt.Errorf("%w: ?to= (destination store URL) is required", errBad))
		return
	}
	dst, err := store.Open(to)
	if err != nil {
		fail(w, fmt.Errorf("%w: backup destination: %v", errBad, err))
		return
	}
	if c, ok := dst.(io.Closer); ok {
		defer c.Close()
	}
	rep, err := s.DB.Backup(r.Context(), dst)
	if err != nil {
		fail(w, err)
		return
	}
	reply(w, rep)
}

// gc runs GC with retention: ?grace= (default 5m), keep-for=DURATION,
// keep-commits=N, keep-reflog=N and keep-manifests=N (see loget.GCOptions).
func (s *Server) gc(w http.ResponseWriter, r *http.Request) {
	o, err := ParseGCOptions(r.URL.Query().Get, 5*time.Minute)
	if err != nil {
		fail(w, fmt.Errorf("%w: %w", errBad, err))
		return
	}
	st, err := s.DB.GC(r.Context(), o)
	if err != nil {
		fail(w, err)
		return
	}
	reply(w, st)
}

// ParseGCOptions reads GC options named grace, keep-for, keep-commits,
// keep-reflog and keep-manifests through get ("" = default).
func ParseGCOptions(get func(string) string, grace time.Duration) (loget.GCOptions, error) {
	o := loget.GCOptions{Grace: grace}
	dur := func(name string, d *time.Duration) error {
		if v := get(name); v != "" {
			x, err := time.ParseDuration(v)
			if err != nil || x < 0 {
				return fmt.Errorf("%s %q: want a duration like 24h", name, v)
			}
			*d = x
		}
		return nil
	}
	num := func(name string, n *uint64) error {
		if v := get(name); v != "" {
			x, err := strconv.ParseUint(v, 10, 63)
			if err != nil {
				return fmt.Errorf("%s %q: want a number", name, v)
			}
			*n = x
		}
		return nil
	}
	var reflog, mans uint64
	for _, err := range []error{dur("grace", &o.Grace), dur("keep-for", &o.KeepFor), num("keep-commits", &o.KeepCommits),
		num("keep-reflog", &reflog), num("keep-manifests", &mans)} {
		if err != nil {
			return o, err
		}
	}
	o.KeepReflog, o.KeepManifests = int(min(reflog, 1<<30)), int(min(mans, 1<<30))
	return o, nil
}

func (s *Server) verify(w http.ResponseWriter, r *http.Request) {
	rep, err := s.DB.Verify(r.Context())
	if err != nil {
		fail(w, err)
		return
	}
	reply(w, rep)
}
