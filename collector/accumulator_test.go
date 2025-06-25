package collector

import (
	"github.com/stretchr/testify/assert"
	"sync"
	"testing"
)

func TestNewAccumulator(t *testing.T) {
	testCases := []struct {
		description    string
		fastMapPool    *FMapPool
		mapPool        *MapPool
		shardAccPool   *ShardAccPool
		expectedType   string
		expectedLength int
	}{
		{
			description:    "with standard map",
			fastMapPool:    nil,
			mapPool:        nil,
			shardAccPool:   nil,
			expectedType:   "map",
			expectedLength: 0,
		},
		{
			description:    "with map pool",
			fastMapPool:    nil,
			mapPool:        NewMapPool(100),
			shardAccPool:   nil,
			expectedType:   "map",
			expectedLength: 0,
		},
		{
			description:    "with fast map pool",
			fastMapPool:    NewFMapPool(100, 2),
			mapPool:        nil,
			shardAccPool:   nil,
			expectedType:   "fastmap",
			expectedLength: 0,
		},
		{
			description:    "with sharded accumulator pool",
			fastMapPool:    nil,
			mapPool:        nil,
			shardAccPool:   NewShardAccPool(100, 8),
			expectedType:   "sharded",
			expectedLength: 0,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.description, func(t *testing.T) {
			acc := NewAccumulator(tc.fastMapPool, tc.mapPool, tc.shardAccPool)

			assert.NotNil(t, acc, "Accumulator should not be nil")
			assert.Equal(t, tc.expectedLength, acc.Len(), "Initial length should be 0")

			switch tc.expectedType {
			case "map":
				assert.NotNil(t, acc.Map, "Map should not be nil")
				assert.False(t, acc.UseFastMap, "UseFastMap should be false")
				assert.False(t, acc.UseShardedAcc, "UseShardedAcc should be false")
			case "fastmap":
				assert.NotNil(t, acc.FastMap, "FastMap should not be nil")
				assert.True(t, acc.UseFastMap, "UseFastMap should be true")
				assert.False(t, acc.UseShardedAcc, "UseShardedAcc should be false")
			case "sharded":
				assert.NotNil(t, acc.ShardedAccumulator, "ShardedAccumulator should not be nil")
				assert.True(t, acc.UseShardedAcc, "UseShardedAcc should be true")
			}
		})
	}
}

func TestAccumulator_GetOrCreate(t *testing.T) {
	testCases := []struct {
		description  string
		accumulator  *Accumulator
		key          interface{}
		expectedData interface{}
	}{
		{
			description:  "standard map - new key",
			accumulator:  NewAccumulator(nil, nil, nil),
			key:          1,
			expectedData: 100,
		},
		{
			description: "standard map - existing key",
			accumulator: func() *Accumulator {
				acc := NewAccumulator(nil, nil, nil)
				acc.Map[1] = 100
				return acc
			}(),
			key:          1,
			expectedData: 100,
		},
		{
			description:  "fast map - new key",
			accumulator:  NewAccumulator(NewFMapPool(100, 2), nil, nil),
			key:          1,
			expectedData: 100,
		},
		{
			description: "fast map - existing key",
			accumulator: func() *Accumulator {
				acc := NewAccumulator(NewFMapPool(100, 2), nil, nil)
				acc.FastMap.Put(1, 100)
				return acc
			}(),
			key:          1,
			expectedData: 100,
		},
		{
			description:  "sharded accumulator - new key",
			accumulator:  NewAccumulator(nil, nil, NewShardAccPool(100, 8)),
			key:          1,
			expectedData: 100,
		},
		{
			description: "sharded accumulator - existing key",
			accumulator: func() *Accumulator {
				acc := NewAccumulator(nil, nil, NewShardAccPool(100, 8))
				acc.ShardedAccumulator.Put(1, 100)
				return acc
			}(),
			key:          1,
			expectedData: 100,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.description, func(t *testing.T) {
			data, created := tc.accumulator.GetOrCreate(tc.key, func() interface{} {
				return tc.expectedData
			})

			assert.Equal(t, tc.expectedData, data, "Data should match expected")
			assert.True(t, created, "Created flag should be true")

			// Get again to verify it's retrieved from the map
			data2, created2 := tc.accumulator.GetOrCreate(tc.key, func() interface{} {
				return 999 // Different value to ensure we get the cached one
			})

			assert.Equal(t, tc.expectedData, data2, "Data should match expected on second retrieval")
			assert.True(t, created2, "Created flag should be true on second retrieval")
		})
	}
}

func TestAccumulator_Put(t *testing.T) {
	testCases := []struct {
		description string
		accumulator *Accumulator
		key         interface{}
		value       interface{}
	}{
		{
			description: "standard map",
			accumulator: NewAccumulator(nil, nil, nil),
			key:         1,
			value:       100,
		},
		{
			description: "fast map",
			accumulator: NewAccumulator(NewFMapPool(100, 2), nil, nil),
			key:         1,
			value:       100,
		},
		{
			description: "sharded accumulator",
			accumulator: NewAccumulator(nil, nil, NewShardAccPool(100, 8)),
			key:         1,
			value:       100,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.description, func(t *testing.T) {
			// Put the value
			result := tc.accumulator.Put(tc.key, tc.value)
			assert.Equal(t, tc.value, result, "Put should return the value")

			// Verify length
			assert.Equal(t, 1, tc.accumulator.Len(), "Length should be 1 after Put")

			// Verify the value can be retrieved
			var retrievedValue interface{}
			if tc.accumulator.UseShardedAcc {
				shard := tc.accumulator.ShardedAccumulator.getShard(tc.key)
				retrievedValue, _ = tc.accumulator.ShardedAccumulator.tryGet(tc.key, shard)
			} else if tc.accumulator.UseFastMap {
				k := int64(tc.key.(int))
				retrievedValue, _ = tc.accumulator.FastMap.Get(k)
			} else {
				retrievedValue = tc.accumulator.Map[tc.key]
			}

			assert.Equal(t, tc.value, retrievedValue, "Retrieved value should match what was put")
		})
	}
}

func TestAccumulator_Len(t *testing.T) {
	testCases := []struct {
		description    string
		accumulator    *Accumulator
		itemsToAdd     int
		expectedLength int
	}{
		{
			description:    "standard map - empty",
			accumulator:    NewAccumulator(nil, nil, nil),
			itemsToAdd:     0,
			expectedLength: 0,
		},
		{
			description:    "standard map - with items",
			accumulator:    NewAccumulator(nil, nil, nil),
			itemsToAdd:     5,
			expectedLength: 5,
		},
		{
			description:    "fast map - empty",
			accumulator:    NewAccumulator(NewFMapPool(100, 2), nil, nil),
			itemsToAdd:     0,
			expectedLength: 0,
		},
		{
			description:    "fast map - with items",
			accumulator:    NewAccumulator(NewFMapPool(100, 2), nil, nil),
			itemsToAdd:     5,
			expectedLength: 5,
		},
		{
			description:    "sharded accumulator - empty",
			accumulator:    NewAccumulator(nil, nil, NewShardAccPool(100, 8)),
			itemsToAdd:     0,
			expectedLength: 0,
		},
		{
			description:    "sharded accumulator - with items",
			accumulator:    NewAccumulator(nil, nil, NewShardAccPool(100, 8)),
			itemsToAdd:     5,
			expectedLength: 5,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.description, func(t *testing.T) {
			// Add items
			for i := 0; i < tc.itemsToAdd; i++ {
				tc.accumulator.Put(i, i*100)
			}

			// Check length
			assert.Equal(t, tc.expectedLength, tc.accumulator.Len(), "Length should match expected")
		})
	}
}

func TestAccumulator_ConcurrentAccess(t *testing.T) {
	testCases := []struct {
		description   string
		accumulator   *Accumulator
		numGoroutines int
		numOperations int
	}{
		{
			description:   "standard map - concurrent access",
			accumulator:   NewAccumulator(nil, nil, nil),
			numGoroutines: 10,
			numOperations: 100,
		},
		{
			description:   "fast map - concurrent access",
			accumulator:   NewAccumulator(NewFMapPool(1000, 2), nil, nil),
			numGoroutines: 10,
			numOperations: 100,
		},
		{
			description:   "sharded accumulator - concurrent access",
			accumulator:   NewAccumulator(nil, nil, NewShardAccPool(1000, 8)),
			numGoroutines: 10,
			numOperations: 100,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.description, func(t *testing.T) {
			var wg sync.WaitGroup
			wg.Add(tc.numGoroutines)

			// Launch goroutines to perform concurrent operations
			for g := 0; g < tc.numGoroutines; g++ {
				go func(id int) {
					defer wg.Done()

					// Mix of Put and GetOrCreate operations
					for i := 0; i < tc.numOperations; i++ {
						key := id*1000 + i

						if i%2 == 0 {
							// Put operation
							tc.accumulator.Put(key, key*10)
						} else {
							// GetOrCreate operation
							value, _ := tc.accumulator.GetOrCreate(key, func() interface{} {
								return key * 10
							})
							assert.Equal(t, key*10, value, "Value should match expected")
						}
					}
				}(g)
			}

			wg.Wait()

			// Verify the accumulator has the expected number of entries
			expectedEntries := tc.numGoroutines * tc.numOperations // Each goroutine does numOperations operations
			assert.Equal(t, expectedEntries, tc.accumulator.Len(), "Final length should match expected")
		})
	}
}
