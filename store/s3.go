package store

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

// S3 is an S3-compatible Store (AWS S3, MinIO, R2, ...). PutIfAbsent uses
// "If-None-Match: *" (S3 conditional writes, GA Aug 2024); a 412 response
// maps to ErrExists. Stores that silently ignore the header are NOT safe for
// writing; run CheckConditional before trusting a new endpoint.
type S3 struct {
	c      *minio.Client
	bucket string
	prefix string
}

func NewS3FromURL(u *url.URL) (*S3, error) {
	q := u.Query()
	endpoint := q.Get("endpoint")
	if endpoint == "" {
		endpoint = "s3.amazonaws.com"
	}
	ak := firstEnv("AWS_ACCESS_KEY_ID", "MINIO_ROOT_USER")
	sk := firstEnv("AWS_SECRET_ACCESS_KEY", "MINIO_ROOT_PASSWORD")
	idle := defaultS3IdleTimeout
	if v := q.Get("idle_timeout"); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil || d <= 0 {
			return nil, fmt.Errorf("store: bad idle_timeout %q", v)
		}
		idle = d
	}
	secure := q.Get("insecure") == ""
	tr, err := idleTransport(secure, idle)
	if err != nil {
		return nil, err
	}
	c, err := minio.New(endpoint, &minio.Options{
		Creds:     credentials.NewStaticV4(ak, sk, ""),
		Secure:    secure,
		Region:    q.Get("region"),
		Transport: tr,
	})
	if err != nil {
		return nil, err
	}
	if q.Get("create") != "" {
		ctx := context.Background()
		if ok, err := c.BucketExists(ctx, u.Host); err == nil && !ok {
			if err := c.MakeBucket(ctx, u.Host, minio.MakeBucketOptions{Region: q.Get("region")}); err != nil {
				// a concurrent opener may have created it first; that's fine
				if ok, err2 := c.BucketExists(ctx, u.Host); err2 != nil || !ok {
					return nil, err
				}
			}
		}
	}
	p := strings.Trim(u.Path, "/")
	if p != "" {
		p += "/"
	}
	return &S3{c: c, bucket: u.Host, prefix: p}, nil
}

// defaultS3IdleTimeout bounds how long one S3 request may make no
// progress (no byte read or written on its connection) before the attempt
// fails and the client retries it. Without it a dead network path with
// the TCP connection still open hangs a stalled upload or a response body
// forever: the HTTP client's ResponseHeaderTimeout covers only the wait
// for headers, and LogEt's single writer puts segments without a caller
// deadline. Override per store with ?idle_timeout=30s in the s3:// URL.
const defaultS3IdleTimeout = 30 * time.Second

// idleTransport is minio-go's default transport with a per-connection
// no-progress deadline.
func idleTransport(secure bool, idle time.Duration) (*http.Transport, error) {
	tr, err := minio.DefaultTransport(secure)
	if err != nil {
		return nil, err
	}
	dial := tr.DialContext
	tr.DialContext = func(ctx context.Context, network, addr string) (net.Conn, error) {
		c, err := dial(ctx, network, addr)
		if err != nil {
			return nil, err
		}
		return &idleConn{Conn: c, idle: idle}, nil
	}
	// A pooled connection waits in Read for the next response; close it
	// before its deadline would fire.
	tr.IdleConnTimeout = min(tr.IdleConnTimeout, idle*3/4)
	return tr, nil
}

// idleConn fails a Read or Write that makes no progress for idle.
type idleConn struct {
	net.Conn
	idle time.Duration
}

func (c *idleConn) Read(p []byte) (int, error) {
	c.Conn.SetReadDeadline(time.Now().Add(c.idle))
	return c.Conn.Read(p)
}

func (c *idleConn) Write(p []byte) (int, error) {
	// one deadline per chunk so a slow but moving upload never trips it
	n := 0
	for n < len(p) {
		e := min(len(p), n+64<<10)
		c.Conn.SetWriteDeadline(time.Now().Add(c.idle))
		m, err := c.Conn.Write(p[n:e])
		n += m
		if err != nil {
			return n, err
		}
	}
	return n, nil
}

func NewS3(c *minio.Client, bucket, prefix string) *S3 {
	if prefix != "" && !strings.HasSuffix(prefix, "/") {
		prefix += "/"
	}
	return &S3{c: c, bucket: bucket, prefix: prefix}
}

func firstEnv(keys ...string) string {
	for _, k := range keys {
		if v := os.Getenv(k); v != "" {
			return v
		}
	}
	return ""
}

func (s *S3) PutIfAbsent(ctx context.Context, key string, data []byte) error {
	opts := minio.PutObjectOptions{ContentType: "application/octet-stream", DisableMultipart: true}
	opts.SetMatchETagExcept("*")
	_, err := s.c.PutObject(ctx, s.bucket, s.prefix+key, bytes.NewReader(data), int64(len(data)), opts)
	if err != nil {
		var er minio.ErrorResponse
		if errors.As(err, &er) && (er.StatusCode == http.StatusPreconditionFailed || er.Code == "PreconditionFailed" || er.StatusCode == http.StatusConflict) {
			return ErrExists
		}
		return err
	}
	return nil
}

// s3ReadTries bounds the attempts of one Get or GetRange whose transfer
// fails after the response started (a reset, truncation or stall mid-body:
// ErrShortRead). The HTTP client retries failures before the body itself.
const s3ReadTries = 3

func (s *S3) Get(ctx context.Context, key string) ([]byte, error) {
	return retryShort(ctx, func() ([]byte, error) { return s.get(ctx, key) })
}

// GetRange implements RangeReader with an HTTP Range request.
func (s *S3) GetRange(ctx context.Context, key string, off, n int64) ([]byte, error) {
	return retryShort(ctx, func() ([]byte, error) { return s.getRange(ctx, key, off, n) })
}

func retryShort(ctx context.Context, f func() ([]byte, error)) ([]byte, error) {
	var b []byte
	var err error
	for try := 0; try < s3ReadTries; try++ {
		if b, err = f(); !errors.Is(err, ErrShortRead) || ctx.Err() != nil {
			return b, err
		}
	}
	return b, err
}

func (s *S3) get(ctx context.Context, key string) ([]byte, error) {
	// Core.GetObject makes one request and returns the response's object
	// info with the body (Object.Stat would cost a second request).
	body, info, _, err := minio.Core{Client: s.c}.GetObject(ctx, s.bucket, s.prefix+key, minio.GetObjectOptions{})
	if err != nil {
		if cutOff(ctx, err) { // e.g. a 404 whose error document was cut off
			return nil, fmt.Errorf("%w: %s: %v", ErrShortRead, key, err)
		}
		return nil, mapNF(err)
	}
	defer body.Close()
	b, err := io.ReadAll(body)
	if err != nil && ctx.Err() == nil { // body cut short, reset or stalled
		return nil, fmt.Errorf("%w: %s: %v", ErrShortRead, key, err)
	}
	if err != nil {
		return nil, err
	}
	// Belt and braces: the size S3 reported must be what we read. (Both come
	// from the response, so a proxy that rewrites Content-Length and body
	// consistently is outside what this can catch.)
	if info.Size != int64(len(b)) {
		return nil, fmt.Errorf("%w: %s: got %d of %d bytes", ErrShortRead, key, len(b), info.Size)
	}
	return b, nil
}

func (s *S3) getRange(ctx context.Context, key string, off, n int64) ([]byte, error) {
	if off < 0 || n <= 0 {
		if n == 0 && off >= 0 {
			return []byte{}, nil
		}
		return nil, fmt.Errorf("store: bad range %d+%d", off, n)
	}
	opts := minio.GetObjectOptions{}
	if err := opts.SetRange(off, off+n-1); err != nil {
		return nil, err
	}
	o, err := s.c.GetObject(ctx, s.bucket, s.prefix+key, opts)
	if err != nil {
		return nil, mapNF(err)
	}
	defer o.Close()
	b, err := io.ReadAll(o)
	if err != nil {
		// minio-go's Object sends the request on first read, so a missing
		// key or an error status surfaces here as an ErrorResponse
		if er := minio.ToErrorResponse(err); er.Code != "" || ctx.Err() != nil {
			return nil, mapNF(err)
		}
		return nil, fmt.Errorf("%w: range %d+%d of %s: %v", ErrShortRead, off, n, key, err)
	}
	if int64(len(b)) < n {
		return nil, fmt.Errorf("%w: range %d+%d of %s: got %d bytes", ErrShortRead, off, n, key, len(b))
	}
	if int64(len(b)) != n {
		return nil, fmt.Errorf("store: range %d+%d of %s: got %d bytes", off, n, key, len(b))
	}
	return b, nil
}

func mapNF(err error) error {
	if minio.ToErrorResponse(err).Code == "NoSuchKey" {
		return ErrNotFound
	}
	return err
}

// cutOff reports whether err is a response cut off mid-body (an error
// document or listing that ends early, a reset or stall while reading it)
// rather than an answer from S3. minio-go retries failures before the
// response; one that breaks while its body is parsed reaches us as a bare
// io error. It is transient: the request is retried.
func cutOff(ctx context.Context, err error) bool {
	if ctx.Err() != nil || minio.ToErrorResponse(err).Code != "" {
		return false
	}
	var ne net.Error
	return errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, io.EOF) || errors.As(err, &ne)
}

// retryList retries a listing cut off mid-response.
func retryList(ctx context.Context, f func() ([]string, error)) ([]string, error) {
	var out []string
	var err error
	for try := 0; try < s3ReadTries; try++ {
		if out, err = f(); err == nil || !cutOff(ctx, err) {
			return out, err
		}
	}
	return out, err
}

func (s *S3) List(ctx context.Context, prefix string) ([]string, error) {
	return retryList(ctx, func() ([]string, error) { return s.list(ctx, prefix) })
}

// Keys lists every object under the prefix (recursive), sorted; the
// conditional-write probe objects are skipped.
func (s *S3) Keys(ctx context.Context) ([]string, error) {
	return retryList(ctx, func() ([]string, error) { return s.keys(ctx) })
}

func (s *S3) list(ctx context.Context, prefix string) ([]string, error) {
	var out []string
	for o := range s.c.ListObjects(ctx, s.bucket, minio.ListObjectsOptions{Prefix: s.prefix + prefix}) {
		if o.Err != nil {
			return nil, o.Err
		}
		k := strings.TrimPrefix(o.Key, s.prefix)
		if strings.HasSuffix(k, "/") {
			continue
		}
		out = append(out, k)
	}
	sort.Strings(out)
	return out, nil
}

func (s *S3) keys(ctx context.Context) ([]string, error) {
	var out []string
	for o := range s.c.ListObjects(ctx, s.bucket, minio.ListObjectsOptions{Prefix: s.prefix, Recursive: true}) {
		if o.Err != nil {
			return nil, o.Err
		}
		k := strings.TrimPrefix(o.Key, s.prefix)
		if k == "" || strings.HasSuffix(k, "/") || strings.HasPrefix(k, "_probe/") {
			continue
		}
		out = append(out, k)
	}
	sort.Strings(out)
	return out, nil
}

func (s *S3) Delete(ctx context.Context, key string) error {
	return s.c.RemoveObject(ctx, s.bucket, s.prefix+key, minio.RemoveObjectOptions{})
}

// CheckConditional verifies the endpoint honours If-None-Match (a second
// create of the same key must fail). Refuse to write if it does not.
func CheckConditional(ctx context.Context, s Store) error {
	k := fmt.Sprintf("_probe/%d", os.Getpid())
	_ = s.Delete(ctx, k)
	if err := s.PutIfAbsent(ctx, k, []byte("a")); err != nil {
		return err
	}
	defer s.Delete(ctx, k)
	if err := s.PutIfAbsent(ctx, k, []byte("b")); !errors.Is(err, ErrExists) {
		return fmt.Errorf("store does not enforce create-only writes (got %v); refusing to open for write", err)
	}
	return nil
}
