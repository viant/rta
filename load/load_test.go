package load

import (
	"context"
	"fmt"
	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/assert"
	"github.com/viant/dsunit"
	"github.com/viant/rta/load/config"
	_ "github.com/viant/sqlx/metadata/product/sqlite"
	"github.com/viant/toolbox"
	"os"
	"path"
	"testing"
	"time"
)

func TimeBasedSuffixForTest() string {
	return "10000000000"
}

func TimeBasedSuffixForTest1() string {
	return "10000000001"
}

func TimeBasedSuffixForTest2() string {
	return "10000000002"
}

func TestService_Load(t *testing.T) {
	baseDir, _ := os.Getwd()
	cfg, err := config.NewConfigFromURL(context.Background(), path.Join(baseDir, "test/config.yaml"))
	if err != nil {
		t.Fail()
		return
	}
	var testCases = []struct {
		description  string
		batchID      string
		initSQL      []string
		caseDataPath string
		suffix       config.Suffix
		data         interface{}
	}{
		{
			description:  "load 3 inventories",
			caseDataPath: "/case1/",
			batchID:      "aaaa-bbbb-cccc-11111-22222",
			suffix:       TimeBasedSuffixForTest,
			data:         getInventories(),
		},
		{
			description:  "load 1 inventory",
			caseDataPath: "/case2/",
			batchID:      "aaaa-bbbb-cccc-11111-22223",
			suffix:       TimeBasedSuffixForTest1,
			data:         getInventory(),
		},
		{
			description:  "load 2 inventories",
			caseDataPath: "/case3/",
			batchID:      "aaaa-bbbb-cccc-11111-22225",
			suffix:       TimeBasedSuffixForTest2,
			data:         getInventory2(),
		},
	}
	testLocation := toolbox.CallerDirectory(3)

	for _, testCase := range testCases {
		loadService, err := New(cfg, testCase.suffix)
		if !assert.Nil(t, err, testCase.description) {
			continue
		}

		if !dsunit.InitFromURL(t, path.Join(testLocation, "test", "dbconfig.yaml")) {
			return
		}

		err = loadService.Load(context.Background(), testCase.data, testCase.batchID)
		if !assert.Nil(t, err, testCase.description) {
			continue
		}

		expectData := dsunit.NewDatasetResource("db", path.Join(testLocation, fmt.Sprintf("test/cases%vexpect", testCase.caseDataPath)), "", "")
		if !dsunit.Expect(t, dsunit.NewExpectRequest(dsunit.SnapshotDatasetCheckPolicy, expectData)) {
			return
		}

	}
}

type Inventory struct {
	ProductID int        `sqlx:"PRODUCT_ID"`
	Name      string     `sqlx:"NAME"`
	Quantity  int        `sqlx:"QUANTITY"`
	Price     float64    `sqlx:"PRICE"`
	Updated   *time.Time `sqlx:"UPDATED"`
}

func getInventories() []*Inventory {
	t1, _ := time.Parse(
		time.RFC3339,
		"2022-03-05T00:00:00+00:00")
	var inventories = []*Inventory{
		&Inventory{
			ProductID: 1,
			Name:      "name1",
			Quantity:  2,
			Price:     0.55,
			Updated:   &t1,
		},
		&Inventory{
			ProductID: 1,
			Name:      "name1",
			Quantity:  3,
			Price:     0.65,
			Updated:   &t1,
		},
		&Inventory{
			ProductID: 2,
			Name:      "name2",
			Quantity:  15,
			Price:     7.651,
			Updated:   &t1,
		},
	}
	return inventories

}

func getInventory() []*Inventory {
	t1, _ := time.Parse(
		time.RFC3339,
		"2022-03-06T00:00:00+00:00")
	var inventories = []*Inventory{
		&Inventory{
			ProductID: 1,
			Name:      "name1",
			Quantity:  8,
			Price:     0.55,
			Updated:   &t1,
		},
	}
	return inventories
}

func getInventory2() []*Inventory {
	t1, _ := time.Parse(
		time.RFC3339,
		"2022-03-07T00:00:00+00:00")
	var inventories = []*Inventory{
		&Inventory{
			ProductID: 5,
			Name:      "name5",
			Quantity:  58,
			Price:     5.55,
			Updated:   &t1,
		}, &Inventory{
			ProductID: 8,
			Name:      "name8",
			Quantity:  158,
			Price:     15.551,
			Updated:   &t1,
		},
	}
	return inventories
}
