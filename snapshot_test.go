package loget

import (
	"fmt"
	"testing"
)

func TestSnapshotRoundTrip(t *testing.T) {
	h := snapHeader{log: "L7", lsn: 42, chain: [32]byte{1, 2, 3}}
	secs := map[string][]byte{"tally@1": []byte("t-bytes"), "z@2": {}}
	b := encodeSnapshot(h, []byte("kv-bytes"), secs)
	got, kv, sx, err := decodeSnapshot(b)
	if err != nil || got != h || string(kv) != "kv-bytes" || fmt.Sprint(sx) != fmt.Sprint(secs) {
		t.Fatalf("round trip: %+v %q %q %v", got, kv, sx, err)
	}
	for i := range b { // every single-byte change is caught
		c := append([]byte{}, b...)
		c[i] ^= 0x40
		if _, _, _, err := decodeSnapshot(c); err == nil {
			t.Fatalf("flip at %d accepted", i)
		}
	}
	for n := 0; n < len(b); n++ {
		if _, _, _, err := decodeSnapshot(b[:n]); err == nil {
			t.Fatalf("truncation to %d accepted", n)
		}
	}
}

func FuzzDecodeSnapshot(f *testing.F) {
	f.Add(encodeSnapshot(snapHeader{log: "L1", lsn: 9}, []byte("kv"), nil))
	f.Add(encodeSnapshot(snapHeader{log: "L1", lsn: 9}, []byte("kv"), map[string][]byte{"a@1": []byte("x")}))
	f.Add([]byte("LGSNAP2\n"))
	f.Fuzz(func(t *testing.T, b []byte) {
		h, kv, tp, err := decodeSnapshot(b)
		if err != nil {
			return
		}
		if again := encodeSnapshot(h, kv, tp); string(again) != string(b) {
			t.Fatalf("accepted bytes that do not re-encode identically")
		}
	})
}
