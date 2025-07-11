package collector

import (
	"encoding/binary"
	"github.com/dolthub/swiss"
	"github.com/viant/rta/collector/fmap"
	"sync"
	"unsafe"
)

// Shard holds a subset of the map with its own lock.
type Shard struct {
	mux     sync.Mutex
	FastMap *swiss.Map[any, any]
}

// ShardedAccumulator splits a big map into multiple shards to reduce contention.
type ShardedAccumulator struct {
	Shards []*Shard
	count  uint32
}

// Len returns the total number of entries across all shards.
func (a *ShardedAccumulator) Len() int {
	total := 0
	// Len() is a moment-in-time estimate, don't lock the shards.
	for _, sh := range a.Shards {
		total += sh.FastMap.Count()
	}
	return total
}

// Put stores or replaces the value for the given key in the appropriate Shard.
func (a *ShardedAccumulator) Put(key, value interface{}) {
	sh := a.getShard(key)
	sh.mux.Lock()
	sh.FastMap.Put(key, value)
	sh.mux.Unlock()
}

// getShard picks a shard for the given key. We only support string or integer keys here.
// Anything else falls back to shard 0.
func (a *ShardedAccumulator) getShard(key interface{}) *Shard {
	var idx uint32
	switch v := key.(type) {

	case string:
		idx = last4bytesToUint32(v) % a.count
	case int:
		idx = uint32(v) % a.count
	case int8:
		idx = uint32(v) % a.count
	case int16:
		idx = uint32(v) % a.count
	case int32:
		idx = uint32(v) % a.count
	case int64:
		idx = uint32(v) % a.count
	case uint:
		idx = uint32(v) % a.count
	case uint8:
		idx = uint32(v) % a.count
	case uint16:
		idx = uint32(v) % a.count
	case uint32:
		idx = v % a.count
	case uint64:
		idx = uint32(v) % a.count
	default:
		idx = 0
	}
	return a.Shards[idx]
}

// last4bytesToUint32 treats the last up to 4 bytes of the string as a big-endian uint32.
// If the string is shorter than 4 bytes, it uses all available bytes (left padded).
//
// Internally, this function reads directly from the string’s backing buffer (avoiding []byte(s)),
// and uses one bounds-check only. In the >=4-byte path, it reads 4 bytes at once via unsafe
// and converts with encoding/binary.BigEndian in native operations.
func last4bytesToUint32(s string) uint32 {
	n := len(s)
	if n >= 4 {
		// Point to the first of the last 4 bytes. We know s’s backing data is contiguous.
		// We do *not* allocate a []byte: we take a *pointer* to that position in memory.
		strHeader := (*[2]uintptr)(unsafe.Pointer(&s))
		// strHeader[0] is &stringData, strHeader[1] is length
		dataPtr := uintptr(strHeader[0]) + uintptr(n-4)

		// Read 4 bytes starting at dataPtr.
		// We must use a [4]byte pointer to avoid extra bounds-check.
		last4 := *(*[4]byte)(unsafe.Pointer(dataPtr))
		return binary.BigEndian.Uint32(last4[:])
	}

	// If string has fewer than 4 bytes, build a big-endian int manually.
	var x uint32
	// We can still index s[i] safely; Go only checks that i < n once per iteration.
	for i := 0; i < n; i++ {
		shift := uint((n - 1 - i) * 8)
		x |= uint32(s[i]) << shift
	}
	return x
}

func (a *ShardedAccumulator) tryGet(key interface{}, sh *Shard) (data interface{}, ok bool) {
	defer func() {
		if r := recover(); r != nil {
			// If Swiss panics (e.g. mid-resize), treat it as a miss
			ok = false
			data = nil
		}
	}()

	// 1) Read the “version” (resident count) before Get
	scn := fmap.Residents(sh.FastMap)

	// 2) Do a lock-free lookup
	data, ok = sh.FastMap.Get(key)

	// 3) Read the “version” again
	next := fmap.Residents(sh.FastMap)

	// 4) If it changed, the table was being mutated/rehashed while we were reading
	if scn != next {
		ok = false
	}
	return
}

func (a *ShardedAccumulator) GetOrCreate(key interface{}, get func() interface{}) (interface{}, bool) {
	sh := a.getShard(key)

	// 1) First try the Swiss cache (lock-free when no rehash)
	if data, ok := a.tryGet(key, sh); ok && data != nil {
		return data, true
	}

	sh.mux.Lock()
	if existing, ok2 := sh.FastMap.Get(key); ok2 {
		sh.mux.Unlock()
		// (Optionally) “promote” into the FastMap so future reads skip the lock
		//sh.FastMap.Put(key, existing)
		return existing, true
	}

	value := get()
	sh.FastMap.Put(key, value)
	sh.mux.Unlock()

	return value, true
}
