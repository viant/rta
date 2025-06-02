package collector

import (
	"fmt"
	"hash/fnv"
	"sync"
)

// Shard holds a subset of the map with its own lock.
type Shard struct {
	mux sync.RWMutex
	M   map[interface{}]interface{}
}

// ShardedAccumulator splits a big map into multiple shards to reduce contention.
type ShardedAccumulator struct {
	Shards []*Shard
	count  uint32
}

// NewShardedAccumulator creates a ShardedAccumulator with the given number of shards.
func NewShardedAccumulator(shardCount int) *ShardedAccumulator {
	s := &ShardedAccumulator{
		Shards: make([]*Shard, shardCount),
		count:  uint32(shardCount),
	}
	for i := 0; i < shardCount; i++ {
		s.Shards[i] = &Shard{M: make(map[interface{}]interface{})}
	}
	return s
}

// getShard chooses a Shard based on the hash of the key.
func (a *ShardedAccumulator) getShard(key interface{}) *Shard {
	h := fnv.New32a()
	h.Write([]byte(fmt.Sprint(key)))
	idx := h.Sum32() % a.count
	//	fmt.Printf("###@@@ Key: %v, Hash: %d, Shard Index: %d\n", key, h.Sum32(), idx)
	return a.Shards[idx]
}

// GetOrCreate returns the existing value for key or calls get() to create, store, and return it.
func (a *ShardedAccumulator) GetOrCreate(key interface{}, get func() interface{}) (interface{}, bool) {
	sh := a.getShard(key)

	// First, try the read path.
	sh.mux.RLock()
	if v, ok := sh.M[key]; ok {
		sh.mux.RUnlock()
		return v, true
	}
	sh.mux.RUnlock()

	// Write path with double-check.
	sh.mux.Lock()
	if existing, ok := sh.M[key]; ok {
		sh.mux.Unlock()
		return existing, true
	}
	value := get()
	sh.M[key] = value
	sh.mux.Unlock()

	return value, true
}

//func main() {
//	acc := NewShardedAccumulator(16)
//
//	// Example usage: each key is initialized only once.
//	for i := 0; i < 10; i++ {
//		key := fmt.Sprintf("key-%d", i%5) // some keys repeat
//		value, _ := acc.GetOrCreate(key, func() interface{} {
//			// expensive computation replaced with a simple string
//			return fmt.Sprintf("value-for-%s", key)
//		})
//		fmt.Printf("Retrieved: %v => %v\n", key, value)
//	}
//}

// Len returns the total number of entries across all shards.
func (a *ShardedAccumulator) Len() int {
	total := 0
	for _, sh := range a.Shards {
		sh.mux.RLock()
		total += len(sh.M)
		sh.mux.RUnlock()
	}
	return total
}

// Put stores or replaces the value for the given key in the appropriate Shard.
func (a *ShardedAccumulator) Put(key, value interface{}) {
	sh := a.getShard(key)
	sh.mux.Lock()
	sh.M[key] = value
	sh.mux.Unlock()
}
