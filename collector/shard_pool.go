package collector

import (
	"fmt"
	"sync"
)

// ShardPool is a pool of ready-to-use maps.
// It holds up to cap(p.pool) maps; excess returned maps are dropped.
type ShardAccPool struct {
	pool     sync.Pool
	initSize int
}

// NewShardPool creates a new ShardPool
func NewShardAccPool(initMapSize int, shardCount int) *ShardAccPool {
	ret := &ShardAccPool{
		initSize: initMapSize,
	}

	ret.pool = sync.Pool{
		New: func() interface{} {
			result := &ShardedAccumulator{
				Shards: make([]*Shard, shardCount),
				count:  uint32(shardCount),
			}
			for i := 0; i < shardCount; i++ {
				result.Shards[i] = &Shard{M: make(map[interface{}]interface{}, initMapSize)}
			}

			return result
		},
	}

	return ret
}

// Get returns a map from the pool or allocates a fresh one if the pool is empty.
func (p *ShardAccPool) Get() *ShardedAccumulator {
	fmt.Printf("###@@@ ShardAccPool Get\n")
	v := p.pool.Get()
	aShard := v.(*ShardedAccumulator)
	return aShard
}

// Put resets the map and returns it to the pool.
func (p *ShardAccPool) Put(acc *ShardedAccumulator) {
	fmt.Printf("###@@@ ShardAccPool Put\n")
	for _, aShard := range acc.Shards {
		for k, _ := range aShard.M {
			delete(aShard.M, k)
		}
	}

	p.pool.Put(acc)
}
