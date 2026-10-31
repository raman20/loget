package server

import (
	"bufio"
	"context"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/raman20/loget"
)

// TestTimeouts: a stalled upload is cut off after ReadTimeout, and a
// request whose body was read is still answered after ReadTimeout.
func TestTimeouts(t *testing.T) {
	db, err := loget.Open(context.Background(), "mem://", loget.WithLogf(func(string, ...any) {}))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	srv := httptest.NewServer(New(db, Options{ReadTimeout: 300 * time.Millisecond, WriteTimeout: 300 * time.Millisecond}))
	defer srv.Close()

	// stalled body
	c, err := net.Dial("tcp", srv.Listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	io.WriteString(c, "PUT /v1/kv/stalled HTTP/1.1\r\nHost: x\r\nContent-Length: 100\r\n\r\nabc")
	c.SetReadDeadline(time.Now().Add(3 * time.Second))
	t0 := time.Now()
	resp, _ := bufio.NewReader(c).ReadString('\n')
	if time.Since(t0) > 2*time.Second {
		t.Fatalf("stalled upload held the connection %v", time.Since(t0))
	}
	if resp != "" && !strings.HasPrefix(resp, "HTTP/1.1 408") { // closed, or 408
		t.Fatalf("stalled upload acknowledged: %q", resp)
	}
	if _, err := db.Get(context.Background(), "stalled"); err == nil {
		t.Fatal("stalled upload was written")
	}

	// body read, then an answer after ReadTimeout has passed
	req, _ := http.NewRequest("PUT", srv.URL+"/v1/kv/late", strings.NewReader("x"))
	time.Sleep(400 * time.Millisecond)
	r, err := http.DefaultClient.Do(req)
	if err != nil || r.StatusCode != 200 {
		t.Fatalf("put: %v %v", err, r)
	}
	r.Body.Close()
}

// TestDrain: closing Options.Drain flips readyz to 503 while requests
// are still served.
func TestDrain(t *testing.T) {
	db, err := loget.Open(context.Background(), "mem://", loget.WithLogf(func(string, ...any) {}))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	drain := make(chan struct{})
	srv := httptest.NewServer(New(db, Options{Drain: drain}))
	defer srv.Close()
	if r, _ := http.Get(srv.URL + "/readyz"); r == nil || r.StatusCode != 200 {
		t.Fatalf("readyz before drain: %v", r)
	}
	close(drain)
	r, _ := http.Get(srv.URL + "/readyz")
	b, _ := io.ReadAll(r.Body)
	r.Body.Close()
	if r.StatusCode != 503 || !strings.Contains(string(b), "draining") {
		t.Fatalf("readyz while draining: %d %q", r.StatusCode, b)
	}
	if err := db.Put(context.Background(), "k", []byte("v")); err != nil {
		t.Fatal(err)
	}
	if r, _ := http.Get(srv.URL + "/v1/kv/k"); r == nil || r.StatusCode != 200 {
		t.Fatalf("request while draining: %v", r)
	}
}

// BenchmarkLoopbackPut: PUT over a real loopback connection, per-request
// deadlines on (defaults) vs off.
func BenchmarkLoopbackPut(b *testing.B) {
	for _, tc := range []struct {
		name string
		to   time.Duration
	}{{"deadlines", 0}, {"none", -1}} {
		b.Run(tc.name, func(b *testing.B) {
			db, err := loget.Open(context.Background(), "mem://", loget.WithLogf(func(string, ...any) {}))
			if err != nil {
				b.Fatal(err)
			}
			defer db.Close()
			srv := httptest.NewServer(New(db, Options{ReadTimeout: tc.to, WriteTimeout: tc.to}))
			defer srv.Close()
			c := srv.Client()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				req, _ := http.NewRequest("PUT", srv.URL+"/v1/kv/k", strings.NewReader("value"))
				r, err := c.Do(req)
				if err != nil {
					b.Fatal(err)
				}
				io.Copy(io.Discard, r.Body)
				r.Body.Close()
			}
		})
	}
}
