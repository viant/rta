package collector

import (
	"context"
	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/assert"
	"github.com/viant/afs"
	"github.com/viant/afs/file"
	"github.com/viant/afs/url"
	"github.com/viant/assertly"
	"github.com/viant/rta/collector/config"
	_ "github.com/viant/sqlx/metadata/product/sqlite"
	tconfig "github.com/viant/tapper/config"
	"github.com/viant/toolbox"
	"os"
	"path"
	"sort"
	"strings"
	"testing"
)

func TestService_prepareRetryOnStartUpWithLink(t *testing.T) {
	parent := toolbox.CallerDirectory(3)
	baseDir := path.Join(parent, "test", "tmp")
	trashDirSuf := "_" + trashDirSuffixFmt
	trashDirPref := trashDirPrefix[:len(trashDirPrefix)-1]
	aConfig := &config.Config{
		Stream: &tconfig.Stream{
			URL: path.Join(baseDir, "test.log"),
		},
	}

	var testCases = []struct {
		description   string
		dirs          []string
		files         []string
		links         []string
		linksTrg      []string
		config        *config.Config
		maxCollectors int
		expected      []string
	}{

		{
			description:   "01 transition from 1 collector to 2 collectors ",
			dirs:          []string{},
			files:         []string{"test.log_0A", "test.log_0B"},
			links:         []string{},
			config:        aConfig,
			maxCollectors: 2,
			expected:      []string{"test.log_0A", "test.log_0B", "0", "0/test.log_0A", "0/test.log_0B"},
		},
		{
			description:   "02 transition from 2 collectors to 1 collector - omitting folder with name not like number",
			dirs:          []string{"0", "1"},
			files:         []string{"test.log_0", "bid.log_f1498d7f-5d3f-4453-99d9-a04389457752", "A/test.log_99"},
			links:         []string{"0/test.log_0", "1/bid.log_f1498d7f-5d3f-4453-99d9-a04389457752"},
			linksTrg:      []string{"test.log_0", "bid.log_f1498d7f-5d3f-4453-99d9-a04389457752"},
			config:        aConfig,
			maxCollectors: 1,
			expected:      []string{"test.log_0", "bid.log_f1498d7f-5d3f-4453-99d9-a04389457752", "A", "A/test.log_99"},
		},
		{
			description:   "03 no transition 1 collector",
			dirs:          []string{},
			files:         []string{"test.log_A", "test.log_B"},
			links:         []string{},
			linksTrg:      []string{},
			config:        aConfig,
			maxCollectors: 1,
			expected:      []string{"test.log_A", "test.log_B"},
		},
		{
			description:   "04 no transition 2 collectors",
			dirs:          []string{"0", "1"},
			files:         []string{"test.log_0", "test.log_1"},
			links:         []string{"0/test.log_0", "1/test.log_1"},
			linksTrg:      []string{"test.log_0", "test.log_1"},
			config:        aConfig,
			maxCollectors: 2,
			expected:      []string{"test.log_0", "test.log_1", "0", "1", "0/test.log_0", "1/test.log_1"},
		},
		{
			description:   "05 transition from 2 collectors to 5 collectors",
			dirs:          []string{"0", "1"},
			files:         []string{"test.log_0", "test.log_1"},
			links:         []string{"0/test.log_0", "1/test.log_1"},
			linksTrg:      []string{"test.log_0", "test.log_1"},
			config:        aConfig,
			maxCollectors: 5,
			expected:      []string{"test.log_0", "test.log_1", "0", "1", "0/test.log_0", "1/test.log_1"},
		},
		{
			description:   "06 transition from 5 collectors to 2 collectors",
			dirs:          []string{"0", "1", "2", "3", "4"},
			files:         []string{"test.log_0", "test.log_1", "test.log_2", "test.log_3", "test.log_4", "test.log_4B"},
			links:         []string{"0/test.log_0", "1/test.log_1", "2/test.log_2", "3/test.log_3", "4/test.log_4", "4/test.log_4B"},
			linksTrg:      []string{"test.log_0", "test.log_1", "test.log_2", "test.log_3", "test.log_4", "test.log_4B"},
			config:        aConfig,
			maxCollectors: 2,
			expected: []string{"test.log_0", "test.log_1", "test.log_2", "test.log_3", "test.log_4", "test.log_4B",
				"0", "1", "0/test.log_0", "1/test.log_1", "0/test.log_2", "0/test.log_3", "0/test.log_4", "0/test.log_4B"},
		},
		{
			description:   "07 transition from 3 collectors to 2 collectors with additional unexpected file in base dir",
			dirs:          []string{"0", "1", "2"},
			files:         []string{"test.log_0", "test.log_1", "test.log_2", "test.wrong_name_file"},
			links:         []string{"0/test.log_0", "1/test.log_1", "2/test.log_2"},
			linksTrg:      []string{"test.log_0", "test.log_1", "test.log_2"},
			config:        aConfig,
			maxCollectors: 2,
			expected: []string{"test.log_0", "test.log_1", "test.log_2",
				"0", "1", "0/test.log_0", "1/test.log_1", "0/test.log_2", "test.wrong_name_file"},
		},
		{
			description:   "08 transition from 5 collectors to 2 collectors - no files to move from dirs: 3 and 4",
			dirs:          []string{"0", "1", "2", "3", "4"},
			files:         []string{"test.log_0", "test.log_1", "test.log_2"},
			links:         []string{"0/test.log_0", "1/test.log_1", "2/test.log_2"},
			linksTrg:      []string{"test.log_0", "test.log_1", "test.log_2"},
			config:        aConfig,
			maxCollectors: 2,
			expected: []string{"test.log_0", "test.log_1", "test.log_2",
				"0", "1", "0/test.log_0", "1/test.log_1", "0/test.log_2"},
		},
		{
			description:   "09 transition from 5 collectors to 2 collectors - no files",
			dirs:          []string{"0", "1", "2", "3", "4"},
			files:         []string{},
			config:        aConfig,
			maxCollectors: 2,
			expected:      []string{"0", "1"},
		},
		{
			description:   "10 transition from 1 collector to 2 collectors - no files",
			dirs:          []string{},
			files:         []string{},
			config:        aConfig,
			maxCollectors: 2,
			expected:      []string{"0"},
		},
		{
			description:   "11 transition from 2 collectors to 1 collector - no files",
			dirs:          []string{"0", "1"},
			files:         []string{},
			config:        aConfig,
			maxCollectors: 1,
			expected:      []string{},
		},
		{
			description:   "12 no transition 1 collector - no files",
			dirs:          []string{},
			files:         []string{},
			config:        aConfig,
			maxCollectors: 1,
			expected:      []string{},
		},
		{
			description:   "13 no transition 2 collectors - no files",
			dirs:          []string{"0", "1"},
			files:         []string{},
			config:        aConfig,
			maxCollectors: 2,
			expected:      []string{"0", "1"},
		},
		{
			description:   "14 transition from 2 collectors to 1 collector - unexpected nested files and folders ",
			dirs:          []string{"0", "1", "0/00", "1/111"},
			files:         []string{"test.log_0", "test.log_1", "1/11/test.log_1"},
			links:         []string{"0/test.log_0", "1/test.log_1", "0/00/test.log_0"},
			linksTrg:      []string{"test.log_0", "test.log_1", "test.log_0"},
			config:        aConfig,
			maxCollectors: 1,
			expected: []string{
				"test.log_0",
				"test.log_1",
				"CHECK_ME_BEFORE_DELETE",
				"CHECK_ME_BEFORE_DELETE/0",
				"CHECK_ME_BEFORE_DELETE/0/00",
				"CHECK_ME_BEFORE_DELETE/0/00/test.log_0",
				"CHECK_ME_BEFORE_DELETE/1",
				"CHECK_ME_BEFORE_DELETE/1/11",
				"CHECK_ME_BEFORE_DELETE/1/11/test.log_1",
				"CHECK_ME_BEFORE_DELETE/1/111"},
		},
		{
			description:   "15 transition from 1 collector to 2 collectors files with invalid prefix remain in place",
			dirs:          []string{},
			files:         []string{"testX.log_0A", "testX.log_0B"},
			links:         []string{},
			linksTrg:      []string{},
			config:        aConfig,
			maxCollectors: 2,
			expected:      []string{"testX.log_0A", "testX.log_0B", "0"},
		},
		{
			description:   "16 transition from 2 collectors to 1 collector files with invalid prefix are not moved",
			dirs:          []string{"0", "1"},
			files:         []string{"0/testX.log_0", "1/testX.log_1"},
			config:        aConfig,
			maxCollectors: 1,
			expected: []string{
				"CHECK_ME_BEFORE_DELETE",
				"CHECK_ME_BEFORE_DELETE/0",
				"CHECK_ME_BEFORE_DELETE/1",
				"CHECK_ME_BEFORE_DELETE/0/testX.log_0",
				"CHECK_ME_BEFORE_DELETE/1/testX.log_1"},
		},
		{
			description:   "17 transition from 1 collectors to 5 collectors - with broken existing folder struct",
			dirs:          []string{"1", "2", "4"},
			files:         []string{"test.log_0", "test.log_1", "test.log_2", "test.log_3", "test.log_4"},
			links:         []string{"1/test.log_1", "2/test.log_2", "4/test.log_4", "4/test.log_4B"},
			linksTrg:      []string{"test.log_1", "test.log_2", "test.log_4", "test.log_4B"},
			config:        aConfig,
			maxCollectors: 5,
			expected: []string{"0", "1", "2", "3", "4",
				"test.log_0", "test.log_1", "test.log_2", "test.log_3", "test.log_4",
				"0/test.log_0", "0/test.log_3", "1/test.log_1", "2/test.log_2", "4/test.log_4",
			},
		},
		{
			description:   "18 transition from 5 collectors to 2 collectors - with broken existing folder struct",
			dirs:          []string{"1", "2", "4"},
			files:         []string{"test.log_0", "test.log_1", "test.log_2", "test.log_3", "test.log_4"},
			links:         []string{"1/test.log_1", "2/test.log_2", "4/test.log_4", "4/test.log_4B"},
			linksTrg:      []string{"test.log_1", "test.log_2", "test.log_4", "test.log_4B"},
			config:        aConfig,
			maxCollectors: 2,
			expected: []string{"0", "1",
				"test.log_0", "test.log_1", "test.log_2", "test.log_3", "test.log_4",
				"0/test.log_0", "0/test.log_3", "0/test.log_2", "0/test.log_4", "1/test.log_1",
			},
		},
		{
			description:   "19 transition from 5 collectors to 1 collector - with broken existing folder struct",
			dirs:          []string{"1", "2", "4"},
			files:         []string{"test.log_0", "test.log_1", "test.log_2", "test.log_3", "test.log_4"},
			links:         []string{"1/test.log_1", "2/test.log_2", "4/test.log_4", "4/test.log_4B"},
			linksTrg:      []string{"test.log_1", "test.log_2", "test.log_4", "test.log_4B"},
			config:        aConfig,
			maxCollectors: 1,
			expected: []string{
				"test.log_0", "test.log_1", "test.log_2", "test.log_3", "test.log_4",
			},
		},
	}

	for _, testCase := range testCases {
		fs := afs.New()

		exists, err := fs.Exists(context.Background(), baseDir, file.DefaultDirOsMode, true)
		assert.Nil(t, err, testCase.description)
		if exists {
			err = fs.Delete(context.Background(), baseDir, file.DefaultDirOsMode, true)
			assert.Nil(t, err, testCase.description)
		}

		err = fs.Create(context.Background(), baseDir, file.DefaultDirOsMode, true)
		assert.Nil(t, err, testCase.description)

		for _, dir := range testCase.dirs {
			err = fs.Create(context.Background(), path.Join(baseDir, dir), file.DefaultDirOsMode, true)
			assert.Nil(t, err, testCase.description)
		}

		for _, f := range testCase.files {
			err = fs.Create(context.Background(), path.Join(baseDir, f), file.DefaultFileOsMode, false)
			assert.Nil(t, err, testCase.description)
		}

		for i, f := range testCase.links {
			//fmt.Println(path.Join(baseDir, testCase.linksTrg[i]), " <= ", path.Join(baseDir, f))
			err := os.Symlink(path.Join(baseDir, testCase.linksTrg[i]), path.Join(baseDir, f))
			assert.Nil(t, err, testCase.description)
		}

		err = prepareRetryOnStartUpWithLink(fs, testCase.maxCollectors, aConfig)
		assert.Nil(t, err, testCase.description)

		actualList, err := fs.List(context.Background(), baseDir)
		assert.Nil(t, err, testCase.description)
		actual := make([]string, 0)
		for _, a := range actualList {
			if a.Name() == "tmp" && a.IsDir() {
				continue
			}
			if a.IsDir() {
				subList, err := listDirRecursively(fs, a.URL())
				assert.Nil(t, err, testCase.description)
				actual = append(actual, subList...)
				continue
			}
			actual = append(actual, a.URL())
		}

		for i, URL := range actual {
			if n := strings.Index(URL, trashDirPref); n != -1 {
				actual[i] = URL[:n+len(trashDirPref)] + URL[n+len(trashDirPref)+len(trashDirSuf):]
			}
		}

		sort.Slice(actual, func(i, j int) bool {
			return actual[i] < actual[j]
		})

		for i, e := range testCase.expected {
			testCase.expected[i] = url.Normalize(path.Join(baseDir, e), "file")
		}

		sort.Slice(testCase.expected, func(i, j int) bool {
			return testCase.expected[i] < testCase.expected[j]
		})

		if !assertly.AssertValues(t, testCase.expected, actual, testCase.description) {
			//fmt.Println("EXPECTED")
			//toolbox.DumpIndent(testCase.expected, true)
			//fmt.Println("ACTUAL")
			//toolbox.DumpIndent(actual, true)
		}

		if !assertly.AssertValues(t, actual, testCase.expected, testCase.description) {
			//fmt.Println("EXPECTED")
			//toolbox.DumpIndent(testCase.expected, true)
			//fmt.Println("ACTUAL")
			//toolbox.DumpIndent(actual, true)
		}

	}
}

func TestService_prepareToRetryOnStartup(t *testing.T) {
	parent := toolbox.CallerDirectory(3)
	baseDir := path.Join(parent, "test", "tmp")
	trashDirSuf := "_" + trashDirSuffixFmt
	trashDirPref := trashDirPrefix[:len(trashDirPrefix)-1]
	aConfig := &config.Config{
		Stream: &tconfig.Stream{
			URL: path.Join(baseDir, "test.log"),
		},
	}

	var testCases = []struct {
		description   string
		dirs          []string
		files         []string
		config        *config.Config
		maxCollectors int
		expected      []string
	}{
		{
			description:   "01 transition from 1 collector to 2 collectors ",
			dirs:          []string{},
			files:         []string{"test.log_0A", "test.log_0B"},
			config:        aConfig,
			maxCollectors: 2,
			expected:      []string{"0", "0/test.log_0A", "0/test.log_0B"},
		},
		{
			description:   "02 transition from 2 collectors to 1 collector - omitting folder with name not like number",
			dirs:          []string{"0", "1"},
			files:         []string{"0/test.log_0", "1/bid.log_f1498d7f-5d3f-4453-99d9-a04389457752", "A/test.log_99"},
			config:        aConfig,
			maxCollectors: 1,
			expected:      []string{"test.log_0", "bid.log_f1498d7f-5d3f-4453-99d9-a04389457752", "A", "A/test.log_99"},
		},
		{
			description:   "03 no transition 1 collector",
			dirs:          []string{},
			files:         []string{"test.log_A", "test.log_B"},
			config:        aConfig,
			maxCollectors: 1,
			expected:      []string{"test.log_A", "test.log_B"},
		},
		{
			description:   "04 no transition 2 collectors",
			dirs:          []string{"0", "1"},
			files:         []string{"0/test.log_0", "1/test.log_1"},
			config:        aConfig,
			maxCollectors: 2,
			expected:      []string{"0", "1", "0/test.log_0", "1/test.log_1"},
		},
		{
			description:   "05 transition from 2 collectors to 5 collectors",
			dirs:          []string{"0", "1"},
			files:         []string{"0/test.log_0", "1/test.log_1"},
			config:        aConfig,
			maxCollectors: 5,
			expected:      []string{"0", "1", "0/test.log_0", "1/test.log_1"},
		},
		{
			description:   "06 transition from 5 collectors to 2 collectors",
			dirs:          []string{"0", "1", "2", "3", "4"},
			files:         []string{"0/test.log_0", "1/test.log_1", "2/test.log_2", "3/test.log_3", "4/test.log_4", "4/test.log_4B"},
			config:        aConfig,
			maxCollectors: 2,
			expected:      []string{"0", "1", "0/test.log_0", "1/test.log_1", "0/test.log_2", "0/test.log_3", "0/test.log_4", "0/test.log_4B"},
		},
		{
			description:   "07 transition from 3 collectors to 2 collectors with additional unexpected file in base dir",
			dirs:          []string{"0", "1", "2"},
			files:         []string{"0/test.log_0", "1/test.log_1", "2/test.log_2", "test.log_unexpected"},
			config:        aConfig,
			maxCollectors: 2,
			expected:      []string{"0", "1", "0/test.log_0", "1/test.log_1", "0/test.log_2", "0/test.log_unexpected"},
		},
		{
			description:   "08 transition from 5 collectors to 2 collectors - no files to move from dirs: 3 and 4",
			dirs:          []string{"0", "1", "2", "3", "4"},
			files:         []string{"0/test.log_0", "1/test.log_1", "2/test.log_2"},
			config:        aConfig,
			maxCollectors: 2,
			expected:      []string{"0", "1", "0/test.log_0", "1/test.log_1", "0/test.log_2"},
		},
		{
			description:   "09 transition from 5 collectors to 2 collectors - no files",
			dirs:          []string{"0", "1", "2", "3", "4"},
			files:         []string{},
			config:        aConfig,
			maxCollectors: 2,
			expected:      []string{"0", "1"},
		},
		{
			description:   "10 transition from 1 collector to 2 collectors - no files",
			dirs:          []string{},
			files:         []string{},
			config:        aConfig,
			maxCollectors: 2,
			expected:      []string{},
		},
		{
			description:   "11 transition from 2 collectors to 1 collector - no files",
			dirs:          []string{"0", "1"},
			files:         []string{},
			config:        aConfig,
			maxCollectors: 1,
			expected:      []string{},
		},
		{
			description:   "12 no transition 1 collector - no files",
			dirs:          []string{},
			files:         []string{},
			config:        aConfig,
			maxCollectors: 1,
			expected:      []string{},
		},
		{
			description:   "13 no transition 2 collectors - no files",
			dirs:          []string{"0", "1"},
			files:         []string{},
			config:        aConfig,
			maxCollectors: 2,
			expected:      []string{"0", "1"},
		},
		{
			description:   "14 transition from 2 collectors to 1 collector - unexpected nested files and folders ",
			dirs:          []string{"0", "1", "1/111"},
			files:         []string{"0/test.log_0", "1/test.log_1", "0/00/test.log_0", "1/11/test.log_1"},
			config:        aConfig,
			maxCollectors: 1,
			expected: []string{
				"test.log_0",
				"test.log_1",
				"CHECK_ME_BEFORE_DELETE",
				"CHECK_ME_BEFORE_DELETE/0",
				"CHECK_ME_BEFORE_DELETE/0/00",
				"CHECK_ME_BEFORE_DELETE/0/00/test.log_0",
				"CHECK_ME_BEFORE_DELETE/1",
				"CHECK_ME_BEFORE_DELETE/1/11",
				"CHECK_ME_BEFORE_DELETE/1/11/test.log_1",
				"CHECK_ME_BEFORE_DELETE/1/111"},
		},
		{
			description:   "15 transition from 1 collector to 2 collectors files with invalid prefix remain in place",
			dirs:          []string{},
			files:         []string{"testX.log_0A", "testX.log_0B"},
			config:        aConfig,
			maxCollectors: 2,
			expected:      []string{"testX.log_0A", "testX.log_0B"},
		},
		{
			description:   "16 transition from 2 collectors to 1 collector files with invalid prefix are moved",
			dirs:          []string{"0", "1"},
			files:         []string{"0/testX.log_0", "1/testX.log_1"},
			config:        aConfig,
			maxCollectors: 1,
			expected:      []string{"testX.log_0", "testX.log_1"},
		},
	}

	for _, testCase := range testCases {
		fs := afs.New()

		exists, err := fs.Exists(context.Background(), baseDir, file.DefaultDirOsMode, true)
		assert.Nil(t, err, testCase.description)
		if exists {
			err = fs.Delete(context.Background(), baseDir, file.DefaultDirOsMode, true)
			assert.Nil(t, err, testCase.description)
		}

		err = fs.Create(context.Background(), baseDir, file.DefaultDirOsMode, true)
		assert.Nil(t, err, testCase.description)

		for _, dir := range testCase.dirs {
			err = fs.Create(context.Background(), path.Join(baseDir, dir), file.DefaultDirOsMode, true)
			assert.Nil(t, err, testCase.description)
		}

		for _, f := range testCase.files {
			err = fs.Create(context.Background(), path.Join(baseDir, f), file.DefaultFileOsMode, false)
			assert.Nil(t, err, testCase.description)
		}

		//TODO prepareRetryOnStartUp
		err = prepareRetryOnStartUp(fs, testCase.maxCollectors, aConfig)
		assert.Nil(t, err, testCase.description)

		actualList, err := fs.List(context.Background(), baseDir)
		assert.Nil(t, err, testCase.description)
		actual := make([]string, 0)
		for _, a := range actualList {
			if a.Name() == "tmp" && a.IsDir() {
				continue
			}
			if a.IsDir() {
				subList, err := listDirRecursively(fs, a.URL())
				assert.Nil(t, err, testCase.description)
				actual = append(actual, subList...)
				continue
			}
			actual = append(actual, a.URL())
		}

		for i, URL := range actual {
			if n := strings.Index(URL, trashDirPref); n != -1 {
				actual[i] = URL[:n+len(trashDirPref)] + URL[n+len(trashDirPref)+len(trashDirSuf):]
			}
		}

		sort.Slice(actual, func(i, j int) bool {
			return actual[i] < actual[j]
		})

		for i, e := range testCase.expected {
			testCase.expected[i] = url.Normalize(path.Join(baseDir, e), "file")
		}

		sort.Slice(testCase.expected, func(i, j int) bool {
			return testCase.expected[i] < testCase.expected[j]
		})

		if !assertly.AssertValues(t, testCase.expected, actual, testCase.description) {
			//fmt.Println("EXPECTED")
			//toolbox.DumpIndent(testCase.expected, true)
			//fmt.Println("ACTUAL")
			//toolbox.DumpIndent(actual, true)
		}

		if !assertly.AssertValues(t, actual, testCase.expected, testCase.description) {
			//fmt.Println("EXPECTED")
			//toolbox.DumpIndent(testCase.expected, true)
			//fmt.Println("ACTUAL")
			//toolbox.DumpIndent(actual, true)
		}
	}
}

func listDirRecursively(fs afs.Service, path string) ([]string, error) {
	objects, err := fs.List(context.Background(), path)
	if err != nil {
		return nil, err
	}
	var result = make([]string, 0)
	for _, object := range objects {
		if object.IsDir() && object.URL() == url.Normalize(path, "file") {
			result = append(result, object.URL())
			continue
		}
		if object.IsDir() {
			subResult, err := listDirRecursively(fs, object.URL())
			if err != nil {
				return nil, err
			}
			result = append(result, subResult...)
			continue
		}
		result = append(result, object.URL())
	}
	return result, nil
}
