package merge

import (
	"context"
	"fmt"
	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/assert"
	"github.com/viant/dsunit"
	"github.com/viant/rta/merge/config"
	_ "github.com/viant/sqlx/metadata/product/sqlite"
	"github.com/viant/toolbox"
	"os"
	"path"
	"testing"
)

func TestService_Merge(t *testing.T) {
	baseDir, _ := os.Getwd()
	cfg, err := config.NewConfigFromURL(context.Background(), path.Join(baseDir, "test/config.yaml"))
	if err != nil {
		t.Fail()
		return
	}
	var testCases = []struct {
		description  string
		caseDataPath string
	}{
		{
			description:  "load 4 inventories",
			caseDataPath: "/case1/",
		},
	}
	testLocation := toolbox.CallerDirectory(3)
	for _, testCase := range testCases {
		mergeService, err := New(cfg)
		if !assert.Nil(t, err, testCase.description) {
			continue
		}

		if !dsunit.InitFromURL(t, path.Join(testLocation, "test", "dbconfig.yaml")) {
			return
		}

		err = mergeService.Merge(context.Background())
		if !assert.Nil(t, err, testCase.description) {
			continue
		}

		expectData := dsunit.NewDatasetResource("db", path.Join(testLocation, fmt.Sprintf("test/cases%vexpect", testCase.caseDataPath)), "", "")
		if !dsunit.Expect(t, dsunit.NewExpectRequest(dsunit.SnapshotDatasetCheckPolicy, expectData)) {
			return
		}
	}
}
