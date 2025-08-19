// Package mph implements a hash/displace minimal perfect hash function.
package mph

import (
	"errors"
	"fmt"
	"sort"

	"github.com/cespare/xxhash/v2"
)

var (
	ErrCouldNotBuildTable = errors.New("could not build table")
)

// Table stores the arrays that represent the hash function.
// - Values: final ordinal (index)
// - Seeds:  route to value, this was part of a multi key bucket input
//   - high bit set = singleton value, there is no contention for allocation
//   - high bit clear = multi-key bucket, the seed is the displacement of the initial value retrieved
type Table struct {
	Values []int32
	Seeds  []uint32
	Mask   uint64
}

const (
	singletonBit = uint32(1 << 31)
	payloadMask  = ^singletonBit
)

type entry struct {
	idx  int32
	hash uint64
}

func New(keys []string) (*Table, error) {
	hKeys := make([]uint64, len(keys))
	for i, k := range keys {
		hKeys[i] = xxhash.Sum64String(k)
	}

	return NewUint64(hKeys)
}

func NewUint64(keys []uint64) (*Table, error) {
	if len(keys) == 0 {
		return &Table{}, nil
	}

	if len(keys) > (1 << 31) {
		return nil, fmt.Errorf("%w: too many keys, 2^31 is the max", ErrCouldNotBuildTable)
	}

	// early dedupe/error on duplicate hash
	tmp := append([]uint64(nil), keys...)
	sort.Slice(tmp, func(i, j int) bool { return tmp[i] < tmp[j] })
	for i := 1; i < len(tmp); i++ {
		if tmp[i] == tmp[i-1] {
			return nil, fmt.Errorf("%w: duplicate 64-bit hash %d", ErrCouldNotBuildTable, tmp[i])
		}
	}

	size := uint64(nextPower2(len(keys)))
	mask := size - 1
	h := make([][]entry, int(size))
	values := make([]int32, int(size))
	seeds := make([]uint32, int(size))

	for idx, k := range keys {
		hash := k
		// extract the lower log2 size bits
		i := int(hash & mask)
		// 0 means empty, that is why idx+1 is used
		h[i] = append(h[i], entry{int32(idx) + 1, hash})
	}

	// pick the biggest buckets first - handle the most difficult before moving on to the simpler
	sort.Slice(h, func(i, j int) bool { return len(h[i]) > len(h[j]) })

	var hidx int
	for hidx = 0; hidx < len(h) && len(h[hidx]) > 1; hidx++ {
		subkeys := h[hidx]

		var seed uint64
		entries := make(map[uint64]int32, len(subkeys))

	newseed:
		for {
			seed++
			// we use the first bit for singletons
			if seed >= (1 << 31) {
				return nil, fmt.Errorf("%w: no seed < 2^31", ErrCouldNotBuildTable)
			}

			for _, k := range subkeys {
				i := xorshiftMult64(k.hash+seed) & mask

				// check if slot i is free in both temporary (entries) and permanent (values)
				if entries[i] == 0 && values[int(i)] == 0 {
					entries[i] = k.idx
					continue
				}

				// hash collision, clear scratch claims and try next seed
				for k := range entries {
					delete(entries, k)
				}
				continue newseed
			}

			break
		}

		// commit placements: mark these slots as permanently taken.
		for k, v := range entries {
			values[int(k)] = v
		}

		// store this seed for the entire bucket
		i := subkeys[0].hash & mask
		seeds[int(i)] = uint32(seed) // fits in 31 bits
	}

	// these are all singletons - entries with no conflicts in their respective buckets
	// collect free values
	free := make([]int, 0, int(size))
	for i := range values {
		if values[i] == 0 {
			free = append(free, i)
		} else {
			// stored idx+1 before
			values[i]--
		}
	}

	for ; hidx < len(h) && len(h[int(hidx)]) > 0; hidx++ {
		k := h[int(hidx)][0]
		i := k.hash & mask

		dst := free[0]
		free = free[1:]

		values[dst] = k.idx - 1

		// high bit = 1 (marked as singleton, no seed logic required to get to value), payload = dst.
		seeds[int(i)] = singletonBit | uint32(dst)
	}

	return &Table{
		Values: values,
		Seeds:  seeds,
		Mask:   mask,
	}, nil
}

func (t *Table) Query(hash uint64) int32 {
	if len(t.Values) == 0 {
		return -1
	}

	i := int(hash & t.Mask)
	s := t.Seeds[i]

	// `if seed < 0` is bugged
	// there are edge cases where the value returned by doing `[-seed-1]` will return an index that is out of bounds
	if s&singletonBit != 0 {
		// singleton case, this is easy, just get everything except for the highest bit
		dst := int(s & payloadMask)
		return t.Values[dst]
	}

	// multi key case recompute displaced slot with xorshiftMult64(hash+seed) & mask
	seed := uint64(s & payloadMask)
	j := xorshiftMult64(hash+seed) & t.Mask
	return t.Values[int(j)]
}

func xorshiftMult64(x uint64) uint64 {
	x ^= x >> 12
	x ^= x << 25
	x ^= x >> 27
	return x * 2685821657736338717
}

// nextPower2 returns the smallest power of two >= n.
func nextPower2(n int) int {
	i := 1
	for i < n {
		i <<= 1
	}
	return i
}
