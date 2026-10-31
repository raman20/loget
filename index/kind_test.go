package index

import "testing"

func TestRegistry(t *testing.T) {
	var r Registry
	newKV := func() Index { return NewKV() }
	if err := r.Register(Kind{Name: "kv", Spaces: []string{"kv"}}); err != nil {
		t.Fatal(err)
	}
	if err := r.Register(Kind{Name: "c", Spaces: []string{"ctr:"}, New: newKV}); err != nil {
		t.Fatal(err)
	}
	for _, bad := range []Kind{
		{Name: "", Spaces: []string{"x"}},
		{Name: "Bad", Spaces: []string{"x"}},
		{Name: "kv", Spaces: []string{"y"}},  // duplicate
		{Name: "n", Spaces: nil},             // no spaces
		{Name: "n", Spaces: []string{""}},    // empty prefix
		{Name: "n", Spaces: []string{"kvx"}}, // overlaps "kv"
		{Name: "n", Spaces: []string{"ct"}},  // overlaps "ctr:"
		{Name: "n", Spaces: []string{"z"}, Restore: func([]byte) (Index, error) { return nil, nil }}, // Restore without New
	} {
		if err := r.Register(bad); err == nil {
			t.Errorf("Register(%+v) accepted", bad)
		}
	}
	if k, ok := r.Owner("ctr:a"); !ok || k.Name != "c" {
		t.Fatalf("Owner(ctr:a) = %v %v", k, ok)
	}
	if _, ok := r.Owner("other"); ok {
		t.Fatal("Owner(other) found a kind")
	}
	if d := r.Derived(); len(d) != 1 || d[0].Name != "c" {
		t.Fatalf("Derived = %v", d)
	}
	if got := (&Kind{Name: "c", Version: 3}).SectionName(); got != "c@3" {
		t.Fatal(got)
	}
}
