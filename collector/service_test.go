package collector

import (
	"context"
	"fmt"
	"github.com/google/gops/agent"
	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/assert"
	"github.com/viant/afs"
	"github.com/viant/afs/file"
	"github.com/viant/afs/url"
	"github.com/viant/dsunit"
	"github.com/viant/rta/collector/config"
	"github.com/viant/rta/load"
	_ "github.com/viant/sqlx/metadata/product/sqlite"
	"github.com/viant/toolbox"
	"log"
	"os"
	"path"
	"strings"
	"testing"
	"time"
)

func TimeBasedSuffixForTest() string {
	return "10000000000"
}

func init() {
	go func() {
		if err := agent.Listen(agent.Options{}); err != nil {
			log.Fatal(err)
		}
	}()

}
func TestService_Collect(t *testing.T) {
	baseDir, _ := os.Getwd()

	cfg, err := config.NewConfigFromURL(context.Background(), path.Join(baseDir, "test/config.yaml"))
	if err != nil {
		t.Fail()
		return
	}

	loadCfg := cfg.Loader
	if err != nil {
		t.Fail()
		return
	}

	load, err := load.New(loadCfg, TimeBasedSuffixForTest)
	if err != nil {
		t.Fail()
		return
	}
	fs := afs.New()
	parent, _ := url.Split(cfg.Stream.URL, file.Scheme)
	fs.Delete(context.Background(), parent)

	var testCases = []struct {
		description string
		serverIp    string
		data        []*Inventory
	}{
		{
			description: "load 6 inventories",
			data:        getInventories(),
		},
	}
	testLocation := toolbox.CallerDirectory(3)
	if !dsunit.InitFromURL(t, path.Join(testLocation, "test", "dbconfig.yaml")) {
		return
	}
	fs.Create(context.Background(), parent, file.DefaultDirOsMode, true)

	for _, testCase := range testCases {
		service, err := New(cfg, newRecordFn, keyFn, reducerFn, mapperFn, load)
		if err != nil {
			continue
		}
		for _, data := range testCase.data {
			service.Collect(data)
		}
		err = service.Close()
		assert.Nil(t, err, testCase.description)
		time.Sleep(time.Second)

		caseDataPath := "/case1/"
		expectData := dsunit.NewDatasetResource("db", path.Join(testLocation, fmt.Sprintf("test/cases%vexpect", caseDataPath)), "", "")
		if !dsunit.Expect(t, dsunit.NewExpectRequest(dsunit.SnapshotDatasetCheckPolicy, expectData)) {
			return
		}

	}

}
func getInventories() []*Inventory {
	t1, _ := time.Parse(
		time.RFC3339,
		"2022-03-11T00:00:00+00:00")
	t2, _ := time.Parse(
		time.RFC3339,
		"2022-03-12T00:00:00+00:00")

	t3, _ := time.Parse(
		time.RFC3339,
		"2022-03-13T00:00:00+00:00")

	var inventories = []*Inventory{
		&Inventory{
			ProductID: 1,
			Name:      "test name1",
			Quantity:  20,
			Price:     1.55,
			Updated:   &t1,
		},
		&Inventory{
			ProductID: 2,
			Name:      "test name2",
			Quantity:  15,
			Price:     7.651,
			Updated:   &t1,
		},
		&Inventory{
			ProductID: 1,
			Name:      "test name1",
			Quantity:  30,
			Price:     8.65,
			Updated:   &t3,
		},
		&Inventory{
			ProductID: 2,
			Name:      "test name3",
			Quantity:  16,
			Price:     17.651,
			Updated:   &t1,
		},
		&Inventory{
			ProductID: 1,
			Name:      "test name1",
			Quantity:  68,
			Price:     35.665,
			Updated:   &t2,
		},
		&Inventory{
			ProductID: 5,
			Name:      "test name5",
			Quantity:  55,
			Price:     55.665,
			Updated:   &t2,
		},
	}
	return inventories

}

func reducerFn(accumulator, source interface{}) {
	acc := accumulator.(*Inventory)
	src := source.(*Inventory)
	acc.Price += src.Price
	acc.Quantity += src.Quantity

	if acc.Updated == nil {
		acc.Updated = src.Updated
	} else {
		if src.Updated.UnixMilli() > acc.Updated.UnixMilli() {
			acc.Updated = src.Updated
		}
	}

	if acc.Name == "" {
		acc.Name = src.Name
	}
	if acc.ProductID == 0 {
		acc.ProductID = src.ProductID
	}

}

func mapperFn(accumulator *Accumulator) interface{} {
	var result = make([]*Inventory, accumulator.Len())
	i := 0
	accumulator.Lock()
	defer accumulator.Unlock()
	for _, v := range accumulator.Map {
		result[i] = v.(*Inventory)
		i++
	}
	return result
}
func keyFn(record interface{}) interface{} {
	src := record.(*Inventory)
	builder := strings.Builder{}
	builder.WriteString(toolbox.AsString(src.ProductID) + "." + src.Name)
	return builder.String()
}
func newRecordFn() interface{} {
	return &Inventory{}
}

type Inventory struct {
	ProductID int        `sqlx:"PRODUCT_ID"`
	Name      string     `sqlx:"NAME"`
	Quantity  int        `sqlx:"QUANTITY"`
	Price     float64    `sqlx:"PRICE"`
	Updated   *time.Time `sqlx:"UPDATED"`
}
