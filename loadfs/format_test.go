package loadfs

import (
	"github.com/stretchr/testify/assert"
	"github.com/viant/assertly"
	"os"
	"testing"
	"time"
)

func TestService_Format(t *testing.T) {
	tNow := &time.Time{}

	var testCases = []struct {
		description         string
		url                 string
		batchID             string
		collectorInstanceId string
		timestamp           *time.Time
		hostIpInfo          string
		category            string
		expected            string
	}{
		{
			description: "url with no tag",
			url:         "/tmp/sig.log.json",
			timestamp:   tNow,
			expected:    "/tmp/sig.log.json",
		},
		{
			description: "url with timestamp tag",
			url:         "/tmp/sig.log.[yyyyMMdd_HHmmss].json",
			timestamp:   tNow,
			expected:    "/tmp/sig.log." + tNow.Format("20060102_150405") + ".json",
		},
		{
			description:         "url all tags",
			url:                 "/tmp/signals/${category}/${collectorId}/${yyyy}/${MM}/${dd}/${HH}/sig.log.${category}.${collectorId}.${hostIpInfo}.${batchId}.[yyyyMMdd_HHmmss].json",
			batchID:             "aa-bb",
			collectorInstanceId: "instance1",
			timestamp:           tNow,
			hostIpInfo:          "127001",
			category:            "string",
			expected:            "/tmp/signals/string/instance1/" + tNow.Format("2006/01/02/15") + "/sig.log.string.instance1.127001.aa-bb." + tNow.Format("20060102_150405") + ".json",
		},
	}

	for _, testCase := range testCases {
		_ = os.Remove(testCase.expected)

		format := &Format{}
		err := format.Init(testCase.url)
		if !assert.Nil(t, err, testCase.description) {
			continue
		}

		actual := format.expandURL(testCase.url, testCase.timestamp, testCase.batchID, testCase.collectorInstanceId, testCase.hostIpInfo, testCase.category)

		assertly.AssertValues(t, testCase.expected, actual)
		assertly.AssertValues(t, actual, testCase.expected)
	}
}
