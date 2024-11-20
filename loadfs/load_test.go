package loadfs

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"github.com/stretchr/testify/assert"
	"github.com/viant/assertly"
	"github.com/viant/rta/collector/loader"
	"github.com/viant/rta/loadfs/config"
	tio "github.com/viant/tapper/io"
	"os"
	"testing"
	"time"
)

func TestService_Load(t *testing.T) {

	cfg := &config.Config{
		FlushMod:       2,
		URL:            "/tmp/sig.log.${collectorId}.${batchId}.json",
		StreamUpload:   false,
		MaxMessageSize: 96,
		Concurrency:    8,
	}

	var testCases = []struct {
		description  string
		batchID      string
		mode         string
		data         interface{}
		count        int
		functionName string
		expectedPath string
	}{
		{
			description:  "load records with encoder implementation",
			batchID:      "aaaa",
			data:         nil, // generated in test
			count:        5,
			functionName: "recordsWithEncoder",
			expectedPath: "/tmp/sig.log.instance1.aaaa.json",
		},
		{
			description:  "load records with no encoder implementation",
			batchID:      "bbbb",
			data:         nil, // generated in test
			count:        5,
			functionName: "recordsIntWithNoEncoder",
			expectedPath: "/tmp/sig.log.instance1.bbbb.json",
		},
	}

	for _, testCase := range testCases {
		_ = os.Remove(testCase.expectedPath)

		loadService, err := New(cfg)
		if !assert.Nil(t, err, testCase.description) {
			continue
		}

		var expected interface{}
		if testCase.functionName == "recordsWithEncoder" {
			testCase.data, expected = recordsWithEncoder(testCase.count, true)
		} else {
			testCase.data = recordsIntWithNoEncoder(testCase.count)
			expected = testCase.data

		}

		err = loadService.Load(context.Background(), testCase.data, testCase.batchID, loader.WithInstanceId("instance1"))
		if !assert.Nil(t, err, testCase.description) {
			continue
		}

		actual, err := unmarshalFile(testCase.expectedPath)
		if !assert.Nil(t, err, testCase.description) {
			continue
		}

		assertly.AssertValues(t, expected, actual)
		assertly.AssertValues(t, actual, expected)

	}
}

type Record struct {
	ID     int         `aerospike:"id,pk" sqlx:"id"`
	Value  interface{} `aerospike:"value,mapKey" sqlx:"value"`
	Bucket int         `aerospike:"bucket,arrayIndex,arraySize=144" sqlx:"bucket"`
	Count  int64       `aerospike:"count,component" sqlx:"count"`
	At     *time.Time  `aerospike:"-" sqlx:"-"`
}

type RecordInt struct {
	ID     int        `aerospike:"id,pk" sqlx:"id"`
	Value  int        `aerospike:"value,mapKey" sqlx:"value"`
	Bucket int        `aerospike:"bucket,arrayIndex,arraySize=144" sqlx:"bucket"`
	Count  int64      `aerospike:"count,component" sqlx:"count"`
	At     *time.Time `aerospike:"-" sqlx:"-"`
}

func (r Record) Encode(stream tio.Stream) {

	switch r.Value.(type) {
	case string:
		stream.PutString("value", r.Value.(string))
	default:

		stream.PutInt("value", r.Value.(int))
	}

	stream.PutInt("count", int(r.Count))
	stream.PutString("at", r.At.Format(time.RFC3339))
}

func recordsWithEncoder(n int, expected bool) (interface{}, interface{}) {
	t1, _ := time.Parse(
		time.RFC3339,
		"2024-01-01T00:00:00+00:00")
	var result = []*Record{}
	var expectedRes = []*Record{}

	for i := 1; i <= n; i++ {
		t2 := t1.Add(time.Hour * time.Duration(i))
		rec := &Record{
			ID:     i,
			Value:  i * 10,
			Bucket: i,
			Count:  int64(i) * 100,
			At:     &t2,
		}
		result = append(result, rec)

		if expected {
			aRec := *rec
			aRec.ID = 0
			aRec.Bucket = 0
			expectedRes = append(expectedRes, &aRec)
		}
	}

	return result, expectedRes
}

func recordsIntWithNoEncoder(n int) interface{} {
	t1, _ := time.Parse(
		time.RFC3339,
		"2024-01-01T00:00:00+00:00")
	var result = []*RecordInt{}

	for i := 1; i <= n; i++ {
		t2 := t1.Add(time.Hour * time.Duration(i))
		rec := &RecordInt{
			ID:     i,
			Value:  i * 10,
			Bucket: i,
			Count:  int64(i) * 100,
			At:     &t2,
		}
		result = append(result, rec)
	}

	return result
}

func unmarshalFile(path string) (interface{}, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("error opening file: %w", err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)

	var records []*Record
	for scanner.Scan() {
		line := scanner.Text()

		var record = &Record{}
		err := json.Unmarshal([]byte(line), record)
		if err != nil {
			return nil, fmt.Errorf("error unmarshaling line: %w", err)
		}
		records = append(records, record)
	}

	if err := scanner.Err(); err != nil {
		fmt.Println("error reading file:", err)
		return nil, err
	}

	return records, nil
}
