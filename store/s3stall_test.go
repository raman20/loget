package store

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// stallServer is a fake S3 endpoint whose connections go silent: GETs send
// the headers and part of the body, PUTs stop reading the upload, and then
// nothing more arrives until the test ends (a dead network path with the
// TCP connection still open, which no ResponseHeaderTimeout catches).
// After stalls requests it behaves.
func stallServer(t *testing.T, stalls int32) (*httptest.Server, *atomic.Int32) {
	var n atomic.Int32
	done := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		stall := n.Add(1) <= stalls
		switch r.Method {
		case http.MethodGet:
			body := bytes.Repeat([]byte("x"), 1000)
			code := http.StatusOK
			var a, b int
			if _, err := fmt.Sscanf(r.Header.Get("Range"), "bytes=%d-%d", &a, &b); err == nil {
				w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", a, b, len(body)))
				body, code = body[a:b+1], http.StatusPartialContent
			}
			w.Header().Set("Content-Length", strconv.Itoa(len(body)))
			w.Header().Set("ETag", `"e"`)
			w.Header().Set("Last-Modified", time.Now().UTC().Format(http.TimeFormat))
			w.WriteHeader(code)
			if !stall {
				w.Write(body)
				return
			}
			w.Write(body[:len(body)/10])
			w.(http.Flusher).Flush()
		case http.MethodPut:
			if !stall {
				io.Copy(io.Discard, r.Body)
				w.Header().Set("ETag", `"e"`)
				return
			}
			// read a little of the upload, then stop reading
			io.CopyN(io.Discard, r.Body, 1)
		default:
			return
		}
		select {
		case <-done:
		case <-r.Context().Done():
		}
	}))
	t.Cleanup(func() { close(done); srv.Close() })
	return srv, &n
}

func stallStore(t *testing.T, srv *httptest.Server, extra string) *S3 {
	t.Setenv("AWS_ACCESS_KEY_ID", "test")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "test")
	u, _ := url.Parse("s3://bkt/p?insecure=1&region=us-east-1&endpoint=" + strings.TrimPrefix(srv.URL, "http://") + extra)
	s, err := NewS3FromURL(u)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

// A transfer that stops making progress fails the attempt after the idle
// timeout and is retried, instead of hanging the caller forever.
func TestS3StalledTransferTimesOut(t *testing.T) {
	for _, c := range []struct {
		name string
		op   func(ctx context.Context, s *S3) error
	}{
		{"get", func(ctx context.Context, s *S3) error { _, err := s.Get(ctx, "k"); return err }},
		{"range", func(ctx context.Context, s *S3) error { _, err := s.GetRange(ctx, "k", 10, 500); return err }},
		{"put", func(ctx context.Context, s *S3) error {
			return s.PutIfAbsent(ctx, "k", bytes.Repeat([]byte("y"), 8<<20))
		}},
	} {
		t.Run(c.name, func(t *testing.T) {
			srv, n := stallServer(t, 1)
			s := stallStore(t, srv, "&idle_timeout=300ms")
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()
			t0 := time.Now()
			err := c.op(ctx, s)
			// a GET whose body stalls mid-way is a short read the store
			// retries; a stalled PUT is retried by the HTTP client
			if ctx.Err() != nil {
				t.Fatalf("hung until the caller's deadline (%v): %v", time.Since(t0), err)
			}
			if err != nil {
				t.Fatalf("stalled %s was not retried: %v", c.name, err)
			}
			if n.Load() < 2 {
				t.Fatalf("%d requests: the stall was never hit", n.Load())
			}
			t.Logf("%s: %v after %v, %d requests", c.name, err, time.Since(t0).Round(time.Millisecond), n.Load())
		})
	}
}

// cutServer answers its first cuts requests with the response's headers
// and only part of its body (an error document or a listing cut off by a
// reset), then answers in full: GET is 404 NoSuchKey, LIST lists one key.
func cutServer(t *testing.T, cuts int32) (*httptest.Server, *atomic.Int32) {
	var n atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		cut := n.Add(1) <= cuts
		var body string
		code := http.StatusOK
		if r.URL.Query().Has("list-type") {
			body = `<?xml version="1.0" encoding="UTF-8"?><ListBucketResult><Name>bkt</Name><Prefix>p/x/</Prefix><KeyCount>1</KeyCount><MaxKeys>1000</MaxKeys><IsTruncated>false</IsTruncated><Contents><Key>p/x/a</Key><Size>1</Size><ETag>"e"</ETag></Contents></ListBucketResult>`
		} else {
			code = http.StatusNotFound
			body = `<?xml version="1.0" encoding="UTF-8"?><Error><Code>NoSuchKey</Code><Message>The specified key does not exist.</Message><Key>p/k</Key><RequestId>r</RequestId></Error>`
		}
		w.Header().Set("Content-Type", "application/xml")
		w.Header().Set("Content-Length", strconv.Itoa(len(body)))
		w.WriteHeader(code)
		if !cut {
			io.WriteString(w, body)
			return
		}
		io.WriteString(w, body[:len(body)/3])
		w.(http.Flusher).Flush()
		panic(http.ErrAbortHandler)
	}))
	t.Cleanup(srv.Close)
	return srv, &n
}

// A response cut off mid-body is transient even when it is an error
// document or a listing: retried, never surfaced as a bare "unexpected
// EOF" (found with the s3sim hostile profile: open failed on one).
func TestS3CutResponseIsRetried(t *testing.T) {
	ctx := context.Background()
	t.Run("get-404", func(t *testing.T) {
		srv, n := cutServer(t, 1)
		s := stallStore(t, srv, "")
		if _, err := s.Get(ctx, "k"); !errors.Is(err, ErrNotFound) {
			t.Fatalf("got %v after %d requests, want ErrNotFound", err, n.Load())
		}
	})
	t.Run("range-404", func(t *testing.T) {
		srv, n := cutServer(t, 1)
		s := stallStore(t, srv, "")
		if _, err := s.GetRange(ctx, "k", 0, 10); !errors.Is(err, ErrNotFound) {
			t.Fatalf("got %v after %d requests, want ErrNotFound", err, n.Load())
		}
	})
	t.Run("list", func(t *testing.T) {
		srv, n := cutServer(t, 1)
		s := stallStore(t, srv, "")
		ks, err := s.List(ctx, "x/")
		if err != nil || len(ks) != 1 || ks[0] != "x/a" {
			t.Fatalf("got %v, %v after %d requests", ks, err, n.Load())
		}
	})
}
