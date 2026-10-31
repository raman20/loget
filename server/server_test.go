package server

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/raman20/loget"
)

func TestHTTPAPI(t *testing.T) {
	db, err := loget.Open(context.Background(), "mem://", loget.WithLogf(func(string, ...any) {}))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ts := httptest.NewServer(Handler(db))
	defer ts.Close()
	do := func(method, path, body string, want int) string {
		t.Helper()
		req, _ := http.NewRequest(method, ts.URL+path, strings.NewReader(body))
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		b, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode != want {
			t.Fatalf("%s %s: %d %s", method, path, resp.StatusCode, b)
		}
		return string(b)
	}
	do("PUT", "/v1/kv/pref?subject=alice", "tea", 200)
	if got := do("GET", "/v1/kv/pref", "", 200); got != "tea" {
		t.Fatal(got)
	}
	do("POST", "/v1/tx", `[{"op":"put","key":"done/t1","value":"ok"},{"op":"put","key":"j","value":{"a":1}}]`, 200)
	if got := do("GET", "/v1/kv/j", "", 200); got != `{"a":1}` {
		t.Fatalf("tx JSON value: %s", got)
	}
	do("POST", "/v1/tx", `[{"op":"pub","key":"x"}]`, 400) // unknown op
	do("POST", "/v1/tx", `[{"op":"put","key":"x","value":"v"},{"op":"ack","key":"x"}]`, 400)
	do("GET", "/v1/kv/x", "", 404) // the refused tx wrote nothing
	do("POST", "/v1/branches", `{"name":"exp","from":"main","at":0}`, 200)
	do("PUT", "/v1/kv/pref?branch=exp&subject=alice", "coffee", 200)
	if got := do("GET", "/v1/kv/pref", "", 200); got != "tea" {
		t.Fatal("branch leaked into main")
	}
	do("POST", "/v1/branches/main/rewind", `{"lsn":1}`, 200)
	do("GET", "/v1/kv/done/t1", "", 404)
	do("POST", "/v1/shred/alice", "", 200)
	do("GET", "/v1/kv/pref", "", 404)
	do("GET", "/v1/kv/pref?branch=exp", "", 404)
	do("GET", "/v1/kv/pref?at=1", "", 404)
	do("PUT", "/v1/kv/x?subject=alice", "y", 410)
	var r struct{ OK bool }
	json.Unmarshal([]byte(do("GET", "/v1/verify", "", 200)), &r)
	if !r.OK {
		t.Fatal("verify")
	}
}

// TestHTTPScanPages: range bounds, reverse and limit/after pagination on
// GET /v1/kv, and the Loget-Next-After header on full pages only.
func TestHTTPScanPages(t *testing.T) {
	db, err := loget.Open(context.Background(), "mem://", loget.WithLogf(func(string, ...any) {}))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ts := httptest.NewServer(Handler(db))
	defer ts.Close()
	for _, k := range []string{"p/a", "p/b", "p/c", "p/d", "p/e", "q/a"} {
		if err := db.Put(context.Background(), k, []byte(k)); err != nil {
			t.Fatal(err)
		}
	}
	get := func(path string, want int) ([]string, string) {
		t.Helper()
		resp, err := http.Get(ts.URL + path)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		if resp.StatusCode != want {
			b, _ := io.ReadAll(resp.Body)
			t.Fatalf("GET %s: %d %s", path, resp.StatusCode, b)
		}
		var out []struct{ Key string }
		json.NewDecoder(resp.Body).Decode(&out)
		var keys []string
		for _, kv := range out {
			keys = append(keys, kv.Key)
		}
		return keys, resp.Header.Get("Loget-Next-After")
	}
	walk := func(q string) []string {
		var all []string
		path := "/v1/kv?" + q
		for i := 0; i < 10; i++ {
			keys, next := get(path, 200)
			all = append(all, keys...)
			if next == "" {
				return all
			}
			path = "/v1/kv?" + q + "&after=" + next
		}
		t.Fatal("no last page")
		return nil
	}
	cases := map[string]string{
		"prefix=p/&limit=2":                         "p/a p/b p/c p/d p/e",
		"prefix=p/&limit=2&reverse=1":               "p/e p/d p/c p/b p/a",
		"prefix=p/&start=p/b&end=p/e&limit=1":       "p/b p/c p/d",
		"prefix=p/&start=p/b&reverse=true":          "p/e p/d p/c p/b",
		"start=p/d&limit=3":                         "p/d p/e q/a",
		"prefix=p/&limit=5":                         "p/a p/b p/c p/d p/e",
		"prefix=p/&start=a&end=z&reverse=1&limit=4": "p/e p/d p/c p/b p/a",
	}
	for q, want := range cases {
		if got := strings.Join(walk(q), " "); got != want {
			t.Errorf("%s: %q, want %q", q, got, want)
		}
	}
	if keys, next := get("/v1/kv?prefix=p/&limit=3", 200); len(keys) != 3 || next != "p%2Fc" {
		t.Errorf("first page %v next %q", keys, next)
	}
	get("/v1/kv?limit=-1", 400)
	get("/v1/kv?reverse=maybe", 400)
}
