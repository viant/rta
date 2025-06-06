package collector

import (
	"github.com/dolthub/swiss"
	"sync"
)

// ShardAccPool is a pool of pre-sharded Accumulators.
type ShardAccPool struct {
	pool sync.Pool
}

// NewShardPool creates a new ShardPool
func NewShardAccPool(initMapSize int, shardCount int) *ShardAccPool {
	ret := &ShardAccPool{}

	ret.pool = sync.Pool{
		New: func() interface{} {
			result := &ShardedAccumulator{
				Shards: make([]*Shard, shardCount),
				count:  uint32(shardCount),
			}
			for i := 0; i < shardCount; i++ {
				result.Shards[i] = &Shard{
					FastMap: swiss.NewMap[any, any](uint32(initMapSize)),
				}
			}

			return result
		},
	}

	return ret
}

// Get returns a map from the pool or allocates a fresh one if the pool is empty.
func (p *ShardAccPool) Get() *ShardedAccumulator {
	v := p.pool.Get()
	aShard := v.(*ShardedAccumulator)
	return aShard
}

// Put resets the map and returns it to the pool.
func (p *ShardAccPool) Put(acc *ShardedAccumulator) {
	for _, aShard := range acc.Shards {
		aShard.FastMap.Clear()
	}

	p.pool.Put(acc)
}
