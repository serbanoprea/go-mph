package mph

import (
	"bufio"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"testing"

	"github.com/cespare/xxhash/v2"
)

var keysFile = flag.String("keys", "", "load keys datafile (one key per line)")

func loadKeysU64(tb testing.TB) []uint64 {
	tb.Helper()

	if *keysFile != "" {
		return loadBigKeysU64(tb, *keysFile)
	}

	base := []string{
		"foo",
		"bar",
		"baz",
		"qux",
		"zot",
		"frob",
		"zork",
		"zeek",
	}
	out := make([]uint64, len(base))
	for i, s := range base {
		out[i] = xxhash.Sum64String(s)
	}
	return out
}

func loadBigKeysU64(tb testing.TB, filename string) []uint64 {
	tb.Helper()

	f, err := os.Open(filename)
	if err != nil {
		tb.Fatalf("unable to open keys file: %v", err)
	}
	defer f.Close()

	var ks []uint64
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		ks = append(ks, xxhash.Sum64String(sc.Text()))
	}
	if err := sc.Err(); err != nil {
		tb.Fatalf("error reading keys file: %v", err)
	}
	return ks
}

func testMPHU64(t *testing.T, keys []uint64) {
	tab, err := NewUint64(keys)
	if err != nil {
		t.Fatal(err)
	}
	for i, k := range keys {
		if got := tab.Query(k); got != int32(i) {
			t.Errorf("Lookup(%v)=%v, want %v", k, got, i)
		}
	}
}

func TestMPH_U64(t *testing.T) {
	keys := loadKeysU64(t)
	testMPHU64(t, keys)
}

func TestMPH_Empty(t *testing.T) {
	var keys []uint64
	// would panic in intial implementation
	tab, err := NewUint64(keys)
	if err != nil {
		t.Fatal(err)
	}
	_ = tab
}

func TestMPH_Determinism(t *testing.T) {
	keys := loadKeysU64(t)

	tab1, err := NewUint64(keys)
	if err != nil {
		t.Fatal(err)
	}
	tab2, err := NewUint64(keys)
	if err != nil {
		t.Fatal(err)
	}
	for i, k := range keys {
		g1 := tab1.Query(k)
		g2 := tab2.Query(k)
		if g1 != g2 || g1 != int32(i) {
			t.Fatalf("determinism failure: key[%d]=%v -> (%d,%d), want %d", i, k, g1, g2, i)
		}
	}
}

func TestMPH_RandomSubsets_U64(t *testing.T) {
	keys := loadKeysU64(t)

	iterations := 100 * len(keys)
	if iterations == 0 {
		iterations = 100
	}

	for i := 0; i < iterations; i++ {
		n := rand.Intn(len(keys) + 1)
		perm := rand.Perm(len(keys))[:n]
		sub := make([]uint64, n)
		for j, v := range perm {
			sub[j] = keys[v]
		}

		t.Run(fmt.Sprintf("%d-%d", i, len(sub)), func(t *testing.T) {
			testMPHU64(t, sub)
		})
	}
}

var sinkI32 int32

func BenchmarkMPH_U64(b *testing.B) {
	keys := loadKeysU64(b)
	tab, err := NewUint64(keys)
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for _, k := range keys {
			sinkI32 += tab.Query(k)
		}
	}
}

func BenchmarkMap_U64(b *testing.B) {
	keys := loadKeysU64(b)
	m := make(map[uint64]int32, len(keys))
	for i, k := range keys {
		m[k] = int32(i)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for _, k := range keys {
			sinkI32 += m[k]
		}
	}
}
