package collector

import (
	"context"
	"github.com/viant/afs"
	"github.com/viant/afs/file"
	"github.com/viant/afs/storage"
	"github.com/viant/afs/url"
	"github.com/viant/rta/collector/config"
	iofs "io/fs"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"
)

func prepareRetryOnStartUpWithLink(fs afs.Service, collectorCnt int, cfg *config.Config) error {
	if collectorCnt == 0 {
		collectorCnt = 1
	}

	URL := url.Normalize(cfg.Stream.URL, file.Scheme)
	baseURL, _ := url.Split(URL, file.Scheme)

	objects, err := fs.List(context.Background(), baseURL)
	if err != nil {
		return err
	}

	exColDirs := exCollectorDirs(objects, baseURL)

	err = cleanBrokenLinks(fs, exColDirs)
	if err != nil {
		return err
	}

	filled, err := fillDirGaps(fs, exColDirs, baseURL)
	if err != nil {
		return err
	}

	if filled {
		objects, err = fs.List(context.Background(), baseURL)
		if err != nil {
			return err
		}
		exColDirs = exCollectorDirs(objects, baseURL)
	}

	col0Dir := url.Join(baseURL, "0")
	if collectorCnt > 1 {
		err := ensureDir(fs, col0Dir)
		if err != nil {
			return err
		}
	}

	err = ensureMissingLinks(fs, exColDirs, objects, cfg, collectorCnt, col0Dir)
	if err != nil {
		return err
	}

	switch {
	case collectorCnt == 1 && len(exColDirs) == 0:
		return nil
	case collectorCnt == 1 && len(exColDirs) > 0:
		dirsToDel, err := dirsToDelete(exColDirs, collectorCnt)
		if err != nil {
			return err
		}

		linksToDel, err := getLinks(fs, dirsToDel)
		if err != nil {
			return err
		}

		err = deleteLinks(fs, linksToDel)
		if err != nil {
			return err
		}

		err = cleanUpDirs(fs, baseURL, dirsToDel)
		if err != nil {
			return err
		}
	case collectorCnt < len(exColDirs):
		dirsToDel, err := dirsToDelete(exColDirs, collectorCnt)
		if err != nil {
			return err
		}

		linksToMove, err := getLinks(fs, dirsToDel)
		if err != nil {
			return err
		}

		err = moveLinks(fs, linksToMove, col0Dir)
		if err != nil {
			return err
		}

		err = cleanUpDirs(fs, baseURL, dirsToDel)
		if err != nil {
			return err
		}
	case collectorCnt >= len(exColDirs):
		return nil
	}

	return nil
}

// TODO this function is not backward compatible for now, please use prepareRetryOnStartUpWithLink instead
func prepareRetryOnStartUp(fs afs.Service, collectorCnt int, cfg *config.Config) error {
	if collectorCnt == 0 {
		collectorCnt = 1
	}

	URL := url.Normalize(cfg.Stream.URL, file.Scheme)
	baseURL, _ := url.Split(URL, file.Scheme)

	objects, err := fs.List(context.Background(), baseURL)
	if err != nil {
		return err
	}

	dirsToMove := getDirsToMove(objects, collectorCnt)

	filesToMove, err := getFilesToMove(fs, dirsToMove, objects, collectorCnt, cfg)
	if err != nil {
		return err
	}

	if len(dirsToMove) == 0 && len(filesToMove) == 0 {
		return nil
	}

	filesDst := baseURL
	if collectorCnt > 1 {
		filesDst = url.Join(baseURL, "0")
	}

	err = moveFiles(fs, filesToMove, filesDst)
	if err != nil {
		return err
	}

	err = moveDirs(fs, baseURL, dirsToMove)
	if err != nil {
		return err
	}
	return nil
}

func moveDirs(fs afs.Service, parentURL string, dirsToMove []storage.Object) error {

	dirsDst := url.Join(parentURL, trashDirPrefix+time.Now().Format(trashDirSuffixFmt))
	if len(dirsToMove) > 0 {
		exists, err := fs.Exists(context.Background(), dirsDst, file.DefaultDirOsMode, true)
		if err != nil {
			return err
		}

		if !exists {
			err = fs.Create(context.Background(), dirsDst, file.DefaultDirOsMode, true)
			if err != nil {
				return err
			}
		}

		canDelete := true

		for _, d := range dirsToMove {
			list, err := fs.List(context.Background(), d.URL())
			if err != nil {
				return err
			}

			if len(list) > 1 {
				canDelete = false
			}
			err = fs.Move(context.Background(), d.URL(), url.Join(dirsDst, d.Name()), file.DefaultDirOsMode)
			if err != nil {
				return err
			}
		}

		if canDelete {
			err = fs.Delete(context.Background(), dirsDst)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func moveFiles(fs afs.Service, filesToMove []storage.Object, filesDst string) error {
	for _, f := range filesToMove {
		err := fs.Move(context.Background(), f.URL(), url.Join(filesDst, f.Name()))
		if err != nil {
			return err
		}
	}
	return nil
}

func getDirsToMove(objects []storage.Object, collectorCnt int) []storage.Object {
	var result []storage.Object

	for _, obj := range objects {
		if !obj.IsDir() {
			continue
		}

		n, err := strconv.Atoi(obj.Name())
		if err != nil {
			continue
		}

		switch collectorCnt {
		case 1:
			result = append(result, obj)
		default:
			if n+1 > collectorCnt {
				result = append(result, obj)
			}
		}
	}
	return result
}

func getFilesToMove(fs afs.Service, dirsToMove []storage.Object, baseObjects []storage.Object, collectorCnt int, cfg *config.Config) ([]storage.Object, error) {
	var result []storage.Object
	for _, dir := range dirsToMove {
		objects, err := fs.List(context.Background(), dir.URL())
		if err != nil {
			return nil, err
		}

		for _, obj := range objects {
			if !obj.IsDir() {
				result = append(result, obj)
			}
		}
	}

	if collectorCnt > 1 {
		for _, obj := range baseObjects {
			if !obj.IsDir() && strings.HasPrefix(obj.URL(), url.Normalize(cfg.Stream.URL, file.Scheme)) {
				result = append(result, obj)
			}
		}
	}
	return result, nil
}

func ensureDir(fs afs.Service, dst string) error {
	exists, err := fs.Exists(context.Background(), dst, file.DefaultDirOsMode, true)
	if err != nil {
		return err
	}

	if !exists {
		err := fs.Create(context.Background(), dst, file.DefaultDirOsMode, true)
		if err != nil {
			return err
		}
	}
	return nil
}

func fillDirGaps(fs afs.Service, collectorDirs []storage.Object, baseURL string) (bool, error) {
	cnt := len(collectorDirs)
	if cnt == 0 {
		return false, nil
	}

	created := false
	sort.Slice(collectorDirs, func(i, j int) bool {
		return collectorDirs[i].Name() < collectorDirs[j].Name()
	})

	if collectorDirs[0].Name() != "0" || collectorDirs[cnt-1].Name() != strconv.Itoa(cnt-1) {
		current := make([]string, cnt)
		for i, c := range collectorDirs {
			current[i] = c.Name()
		}

		last := collectorDirs[cnt-1].Name()
		n, err := strconv.Atoi(last)
		if err != nil {
			return false, err
		}

		required := make([]string, n)
		for i := 0; i < n; i++ {
			required[i] = strconv.Itoa(i)
		}

		missing := diff(required, current)

		for _, name := range missing {
			err = fs.Create(context.Background(), url.Join(baseURL, name), file.DefaultDirOsMode, true)
			if err != nil {
				return false, err
			}
			created = true
		}
	}
	return created, nil
}

func cleanBrokenLinks(fs afs.Service, dirs []storage.Object) error {
	linksToDel, err := brokenLinks(fs, dirs)
	if err != nil {
		return err
	}

	err = deleteLinks(fs, linksToDel)
	if err != nil {
		return err
	}
	return err
}

func ensureMissingLinks(fs afs.Service, collectorDirs []storage.Object, objects []storage.Object, cfg *config.Config, collectorCnt int, dst string) error {
	if collectorCnt == 1 {
		return nil
	}

	allSymlinks, err := getAllSymlinks(fs, collectorDirs)
	if err != nil {
		return err
	}

	logFiles, err := getLogFiles(objects, cfg)

	filesWithNoSymlink := filesWithNoSymLink(logFiles, allSymlinks)
	err = createSymLinks(filesWithNoSymlink, dst)
	if err != nil {
		return err
	}
	return nil
}

func diff(a, b []string) []string {
	m := make(map[string]bool)
	var inANotB []string
	for _, item := range b {
		m[item] = true
	}
	for _, item := range a {
		if _, ok := m[item]; !ok {
			inANotB = append(inANotB, item)
		}
	}
	return inANotB
}

func filesWithNoSymLink(a, b []storage.Object) []storage.Object {
	m := make(map[string]bool)
	var inANotB []storage.Object
	for _, item := range b {
		m[item.Name()] = true
	}
	for _, item := range a {
		if _, ok := m[item.Name()]; !ok {
			inANotB = append(inANotB, item)
		}
	}
	return inANotB
}

func cleanUpDirs(fs afs.Service, baseURL string, dirsToMove []storage.Object) error {
	if len(dirsToMove) == 0 {
		return nil
	}

	dst := url.Join(baseURL, trashDirPrefix+time.Now().Format(trashDirSuffixFmt))
	err := ensureDir(fs, dst)
	if err != nil {
		return err
	}

	canDelete := true
	for _, d := range dirsToMove {
		list, err := fs.List(context.Background(), d.URL())
		if err != nil {
			return err
		}

		if len(list) > 1 {
			canDelete = false
		}
		err = fs.Move(context.Background(), d.URL(), url.Join(dst, d.Name()), file.DefaultDirOsMode)
		if err != nil {
			return err
		}
	}

	if canDelete {
		err = fs.Delete(context.Background(), dst)
		if err != nil {
			return err
		}
	}

	return nil
}

func moveLinks(fs afs.Service, objects []storage.Object, filesDst string) error {
	for _, obj := range objects {
		if obj.Mode()&iofs.ModeSymlink == 0 {
			continue
		}

		err := fs.Move(context.Background(), obj.URL(), url.Join(filesDst, obj.Name()))
		if err != nil {
			return err
		}
	}
	return nil
}

func deleteLinks(fs afs.Service, objects []storage.Object) error {
	for _, obj := range objects {
		if obj.Mode()&iofs.ModeSymlink == 0 {
			continue
		}

		err := fs.Delete(context.Background(), obj.URL())
		if err != nil {
			return err
		}
	}
	return nil
}

func dirsToDelete(colDirs []storage.Object, collectorCnt int) ([]storage.Object, error) {
	var result []storage.Object

	for _, obj := range colDirs {
		switch collectorCnt {
		case 1:
			result = append(result, obj)
		default:
			n, err := strconv.Atoi(obj.Name())
			if err != nil {
				return nil, err
			}

			if n+1 > collectorCnt {
				result = append(result, obj)
			}
		}
	}
	return result, nil
}

func getLinks(fs afs.Service, dirs []storage.Object) ([]storage.Object, error) {
	var result []storage.Object
	for _, dir := range dirs {
		objects, err := fs.List(context.Background(), dir.URL())
		if err != nil {
			return nil, err
		}

		for _, obj := range objects {
			if !obj.IsDir() && obj.Mode()&iofs.ModeSymlink != 0 {
				result = append(result, obj)
			}
		}
	}
	return result, nil
}

func exCollectorDirs(objects []storage.Object, baseUrl string) []storage.Object {
	var result []storage.Object

	for _, obj := range objects {
		if !obj.IsDir() || obj.URL() == baseUrl {
			continue
		}

		_, err := strconv.Atoi(obj.Name())
		if err != nil {
			continue
		}

		result = append(result, obj)
	}
	return result
}

func getAllSymlinks(fs afs.Service, dirs []storage.Object) ([]storage.Object, error) {
	var result []storage.Object
	for _, dir := range dirs {
		objects, err := fs.List(context.Background(), dir.URL())
		if err != nil {
			return nil, err
		}

		for _, obj := range objects {
			if !obj.IsDir() && obj.Mode()&iofs.ModeSymlink != 0 {
				result = append(result, obj)
			}
		}
	}

	return result, nil
}

func createSymLinks(objects []storage.Object, dst string) error {
	for _, obj := range objects {
		if obj.IsDir() {
			continue
		}

		f := url.Path(obj.URL())
		link := url.Path(url.Join(dst, obj.Name()))
		err := os.Symlink(f, link)
		if err != nil {
			return err
		}
	}
	return nil
}

func brokenLinks(fs afs.Service, dirs []storage.Object) ([]storage.Object, error) {
	result := []storage.Object{}

	for _, dir := range dirs {
		if !dir.IsDir() {
			continue
		}

		objects, err := fs.List(context.Background(), dir.URL())
		if err != nil {
			return nil, err
		}

		for _, obj := range objects {
			broken, err := isBrokenLink(obj)
			if err != nil {
				return nil, err
			}

			if broken {
				result = append(result, obj)
			}
		}
	}
	return result, nil
}

func isBrokenLink(obj storage.Object) (bool, error) {
	if obj.Mode()&iofs.ModeSymlink == 0 {
		return false, nil
	}

	_, err := os.Stat(url.Path(obj.URL()))
	if os.IsNotExist(err) {
		return true, nil
	}

	return false, err
}

func getLogFiles(baseObjects []storage.Object, cfg *config.Config) ([]storage.Object, error) {
	var result []storage.Object
	prefix := url.Normalize(cfg.Stream.URL, file.Scheme)
	for _, obj := range baseObjects {
		if obj.Mode().IsRegular() && strings.HasPrefix(obj.URL(), prefix) {
			result = append(result, obj)
		}
	}
	return result, nil
}
