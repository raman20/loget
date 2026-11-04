package server

import (
	"net/http"
	"testing"
)

// POST /v1/batch (= /v1/tx) is all-or-nothing: a bad op commits nothing.
func TestBatchRoute(t *testing.T) {
	db := memDB(t)
	h := New(db, Options{})
	if c := call(h, "POST", "/v1/batch", "", `[{"op":"put","key":"a","value":"1"},{"op":"nope","key":"b"}]`); c != http.StatusBadRequest {
		t.Fatalf("bad op: %d, want 400", c)
	}
	if c := call(h, "GET", "/v1/kv/a", "", ""); c != http.StatusNotFound {
		t.Fatalf("part of a refused batch stored: %d", c)
	}
	if c := call(h, "POST", "/v1/batch", "", `[{"op":"put","key":"a","value":"1"},{"op":"put","key":"b","value":"2"},{"op":"del","key":"a"}]`); c != http.StatusOK {
		t.Fatalf("batch: %d", c)
	}
	if c := call(h, "GET", "/v1/kv/a", "", ""); c != http.StatusNotFound {
		t.Fatalf("a after put+del: %d", c)
	}
	if c := call(h, "GET", "/v1/kv/b", "", ""); c != http.StatusOK {
		t.Fatalf("b: %d", c)
	}
}
