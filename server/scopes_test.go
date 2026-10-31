package server

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"math/big"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/raman20/loget"
)

var memSeq atomic.Int64

func memDB(t *testing.T) *loget.DB {
	t.Helper()
	db, err := loget.Open(context.Background(), fmt.Sprintf("mem://%s-%d", t.Name(), memSeq.Add(1)), loget.WithLogf(func(string, ...any) {}))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

func call(h http.Handler, method, path, tok, body string) int {
	r := httptest.NewRequest(method, path, strings.NewReader(body))
	if tok != "" {
		r.Header.Set("Authorization", "Bearer "+tok)
	}
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	return w.Code
}

const tokenFile = `# scoped tokens
read   reader-0001
write  writer-0001 name=app
admin  admin-00001
write  limited-001 name=svc kv:users/* kv:jobs
read   ro-limited1 kv:pub/*
legacy-admin-token
`

func TestScopedTokens(t *testing.T) {
	db := memDB(t)
	ts, err := ParseTokens([]byte(tokenFile))
	if err != nil {
		t.Fatal(err)
	}
	h := New(db, Options{Token: "static-admin", Auth: NewAuth(ts)})
	for _, tc := range []struct {
		method, path, tok, body string
		want                    int
	}{
		{"PUT", "/v1/kv/a", "admin-00001", "v", 200},
		{"PUT", "/v1/kv/a", "static-admin", "v", 200},
		{"PUT", "/v1/kv/a", "legacy-admin-token", "v", 200},
		{"GET", "/v1/kv/a", "reader-0001", "", 200},
		{"PUT", "/v1/kv/a", "reader-0001", "v", 403},
		{"PUT", "/v1/kv/a", "writer-0001", "v", 200},
		{"POST", "/v1/compact", "writer-0001", "", 403},
		{"POST", "/v1/gc", "writer-0001", "", 403},
		{"POST", "/v1/shred/x", "writer-0001", "", 403},
		{"POST", "/v1/gc", "admin-00001", "", 200},
		{"GET", "/v1/kv/a", "nobody-00001", "", 401},
		// limited token: its collections only, nothing database-wide
		{"PUT", "/v1/kv/users/1", "limited-001", "v", 200},
		{"PUT", "/v1/kv/a", "limited-001", "v", 403},
		{"GET", "/v1/kv?prefix=users/", "limited-001", "", 200},
		{"GET", "/v1/kv", "limited-001", "", 403},
		{"PUT", "/v1/kv/jobs", "limited-001", "v", 200},  // exact name
		{"PUT", "/v1/kv/jobs2", "limited-001", "v", 403}, // not a prefix grant
		{"GET", "/v1/log", "limited-001", "", 403},
		{"GET", "/v1/branches", "limited-001", "", 403},
		{"POST", "/v1/tx", "limited-001", `[{"op":"put","key":"users/2","value":"v"},{"op":"del","key":"jobs"}]`, 200},
		{"POST", "/v1/tx", "limited-001", `[{"op":"put","key":"users/3","value":"v"},{"op":"put","key":"admin","value":"v"}]`, 403},
		{"GET", "/v1/kv/pub/x", "ro-limited1", "", 404},
		{"GET", "/v1/kv/users/1", "ro-limited1", "", 403},
		{"PUT", "/v1/kv/pub/x", "ro-limited1", "v", 403},
	} {
		if got := call(h, tc.method, tc.path, tc.tok, tc.body); got != tc.want {
			t.Errorf("%s %s as %s: %d, want %d", tc.method, tc.path, tc.tok, got, tc.want)
		}
	}
	// the refused tx wrote nothing
	if got := call(h, "GET", "/v1/kv/users/3", "admin-00001", ""); got != 404 {
		t.Fatalf("refused tx left users/3: %d", got)
	}
	for _, bad := range []string{"", "root tok-12345", "read short", "read tok-12345 files:x", "read tok-12345 topic:x", "read dup-12345\nwrite dup-12345"} {
		if _, err := ParseTokens([]byte(bad)); err == nil {
			t.Errorf("accepted %q", bad)
		}
	}
}

// TestTokenRotation: the file is re-read without a restart; a bad file
// keeps the old tokens.
func TestTokenRotation(t *testing.T) {
	db := memDB(t)
	f := filepath.Join(t.TempDir(), "tokens")
	write := func(s string) {
		t.Helper()
		tmp := f + ".tmp"
		if err := os.WriteFile(tmp, []byte(s), 0o600); err != nil {
			t.Fatal(err)
		}
		if err := os.Rename(tmp, f); err != nil {
			t.Fatal(err)
		}
	}
	write("read old-token-1\n")
	a, err := LoadTokenFile(f)
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(New(db, Options{Auth: a}))
	defer srv.Close()
	h := srv.Config.Handler
	if c := call(h, "GET", "/v1/kv/a", "old-token-1", ""); c != 404 {
		t.Fatal("old token refused", c)
	}
	// rotate: both during the overlap, then only the new one
	write("read old-token-1\nadmin new-token-1\n")
	if ok, err := a.Reload(); !ok || err != nil {
		t.Fatal(ok, err)
	}
	if call(h, "GET", "/v1/kv/a", "old-token-1", "") != 404 || call(h, "PUT", "/v1/kv/a", "new-token-1", "v") != 200 {
		t.Fatal("overlap")
	}
	write("admin new-token-1\n")
	a.Reload()
	if call(h, "GET", "/v1/kv/a", "old-token-1", "") != 401 {
		t.Fatal("revoked token still works")
	}
	// a broken file keeps the current tokens
	write("superuser x\n")
	if _, err := a.Reload(); err == nil {
		t.Fatal("bad file accepted")
	}
	if call(h, "GET", "/v1/kv/a", "new-token-1", "") != 200 {
		t.Fatal("tokens lost on a bad reload")
	}
}

func writePair(t *testing.T, dir string, serial int64) (string, string) {
	t.Helper()
	k, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	tpl := &x509.Certificate{SerialNumber: big.NewInt(serial), Subject: pkix.Name{CommonName: "127.0.0.1"},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour), DNSNames: []string{"localhost"}}
	der, err := x509.CreateCertificate(rand.Reader, tpl, tpl, &k.PublicKey, k)
	if err != nil {
		t.Fatal(err)
	}
	kd, _ := x509.MarshalECPrivateKey(k)
	c, kf := filepath.Join(dir, "cert.pem"), filepath.Join(dir, "key.pem")
	os.WriteFile(c+".tmp", pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0o600)
	os.WriteFile(kf+".tmp", pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: kd}), 0o600)
	os.Rename(c+".tmp", c)
	os.Rename(kf+".tmp", kf)
	return c, kf
}

func TestCertReload(t *testing.T) {
	dir := t.TempDir()
	cf, kf := writePair(t, dir, 1)
	c, err := LoadCerts(cf, kf)
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewUnstartedServer(New(memDB(t), Options{}))
	srv.Listener = tls.NewListener(srv.Listener, &tls.Config{GetCertificate: c.GetCertificate})
	srv.Start()
	defer srv.Close()
	url := "https://" + srv.Listener.Addr().String()
	serial := func() int64 {
		t.Helper()
		cl := &http.Client{Transport: &http.Transport{TLSClientConfig: &tls.Config{InsecureSkipVerify: true}, DisableKeepAlives: true}}
		r, err := cl.Get(url + "/healthz")
		if err != nil {
			t.Fatal(err)
		}
		r.Body.Close()
		return r.TLS.PeerCertificates[0].SerialNumber.Int64()
	}
	if s := serial(); s != 1 {
		t.Fatalf("serial %d", s)
	}
	time.Sleep(10 * time.Millisecond) // a distinct mtime
	writePair(t, dir, 2)
	if ok, err := c.Reload(); !ok || err != nil {
		t.Fatal(ok, err)
	}
	if s := serial(); s != 2 {
		t.Fatalf("after reload: serial %d", s)
	}
	// a half-written renewal (key without its cert) is refused
	os.WriteFile(kf, []byte("garbage"), 0o600)
	if _, err := c.Reload(); err == nil {
		t.Fatal("bad pair accepted")
	}
	if s := serial(); s != 2 {
		t.Fatalf("after a bad reload: serial %d", s)
	}
}

// TestScopedScanBounds: start/end/after cannot widen a scoped token's scan
// beyond its authorised prefix (bounds intersect the prefix).
func TestScopedScanBounds(t *testing.T) {
	db := memDB(t)
	ts, err := ParseTokens([]byte(tokenFile))
	if err != nil {
		t.Fatal(err)
	}
	h := New(db, Options{Token: "static-admin", Auth: NewAuth(ts)})
	for _, k := range []string{"a", "jobs", "users/1", "users/2", "z"} {
		if got := call(h, "PUT", "/v1/kv/"+k, "admin-00001", "v"); got != 200 {
			t.Fatalf("put %s: %d", k, got)
		}
	}
	for _, q := range []string{"&start=&end=", "&start=a&end=zz", "&start=%00", "&reverse=1&start=a", "&after=a", "&reverse=1&after=zz"} {
		r := httptest.NewRequest("GET", "/v1/kv?prefix=users/"+q, nil)
		r.Header.Set("Authorization", "Bearer limited-001")
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		if w.Code != 200 {
			t.Fatalf("%s: %d", q, w.Code)
		}
		var out []struct{ Key string }
		if err := json.Unmarshal(w.Body.Bytes(), &out); err != nil {
			t.Fatal(err)
		}
		if len(out) != 2 {
			t.Errorf("%s: %d keys %v, want users/1 and users/2", q, len(out), out)
		}
		for _, kv := range out {
			if !strings.HasPrefix(kv.Key, "users/") {
				t.Errorf("%s: leaked %q", q, kv.Key)
			}
		}
	}
}
