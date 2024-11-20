package loadfs

import (
	"context"
	"fmt"
	"github.com/stretchr/testify/assert"
	"github.com/viant/rta/collector/loader"
	"github.com/viant/rta/loadfs/config"
	"testing"
)

func Benchmark_Service_Load(b *testing.B) {

	cfg := &config.Config{
		FlushMod:       100000,
		URL:            "/tmp/signals/${collectorId}/${yyyy}/${MM}/${dd}/${HH}/sig.log.${collectorId}.${hostIpInfo}.${batchId}.[yyyyMMdd_HHmmss].json",
		StreamUpload:   false,
		MaxMessageSize: 128, // DefaultMaxMessageSize = 2048
		Concurrency:    8,   // DefaultConcurrency    = 8
	}

	var testCases = []struct {
		description  string
		batchID      string
		mode         string
		data         interface{}
		count        int
		functionName string
	}{
		{
			description:  "load records with encoder implementation",
			batchID:      "aaaa-bbbb-cccc-%v-%v",
			data:         nil,
			count:        1000000,
			functionName: "recordsWithEncoder",
		},
		{
			description:  "load records with no encoder implementation",
			batchID:      "aaaa-bbbb-cccc-%v-%v",
			data:         nil,
			count:        1000000,
			functionName: "recordsIntWithNoEncoder",
		},
	}

	for n, testCase := range testCases {
		loadService, err := New(cfg)
		if !assert.Nil(b, err, testCase.description) {
			continue
		}

		if testCase.functionName == "recordsWithEncoder" {
			testCase.data, _ = recordsWithEncoder(testCase.count, false)
		} else {
			testCase.data = recordsIntWithNoEncoder(testCase.count)
		}

		b.Run(fmt.Sprintf("%s|%8d|", testCase.description, testCase.count), func(b *testing.B) {
			b.ResetTimer()
			b.ReportAllocs() // Report memory allocations
			for i := 0; i < b.N; i++ {
				testCase.batchID = fmt.Sprintf(testCase.batchID, n, i)
				err = loadService.Load(context.Background(), testCase.data, testCase.batchID, loader.WithInstanceId("instance1"))
				if !assert.Nil(b, err, testCase.description) {
					continue
				}
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
