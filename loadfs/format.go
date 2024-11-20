package loadfs

import (
	"github.com/viant/rta/shared"
	"github.com/viant/toolbox"
	"strings"
	"time"
)

const (
	year   = "${yyyy}"
	month  = "${MM}"
	day    = "${dd}"
	hour   = "${HH}"
	minute = "${mm}"
	second = "${ss}"
)

// Format represents a format
type Format struct {
	hostIP         string
	hostIpInfo     string
	timeStartIndex int
	timeEndIndex   int
	timeLayout     string
}

// Init initializes the format
func (r *Format) Init(URL string) error {
	var err error
	r.hostIP, err = shared.GetLocalIPv4()
	if err != nil {
		return err
	}

	if r.hostIP == "::1" {
		r.hostIP = "127.0.0.1"
		r.hostIpInfo = "localhost"
	} else {
		r.hostIpInfo = strings.Replace(r.hostIP, ".", "", -1)
	}

	r.timeEndIndex = strings.Index(URL, "]")
	r.timeStartIndex = strings.Index(URL, "[")
	if r.timeStartIndex != -1 && r.timeEndIndex != -1 {
		timeTemplate := URL[r.timeStartIndex+1 : r.timeEndIndex]
		r.timeLayout = toolbox.DateFormatToLayout(timeTemplate)
	}

	return nil
}

func (r *Format) replaceDateTokensInURL(t *time.Time, URL string) string {
	result := URL

	if index := strings.Index(result, year); index != -1 {
		result = strings.ReplaceAll(result, year, t.Format("2006"))
	}
	if index := strings.Index(result, month); index != -1 {
		result = strings.ReplaceAll(result, month, t.Format("01"))
	}
	if index := strings.Index(result, day); index != -1 {
		result = strings.ReplaceAll(result, day, t.Format("02"))
	}
	if index := strings.Index(result, hour); index != -1 {
		result = strings.ReplaceAll(result, hour, t.Format("15"))
	}
	if index := strings.Index(result, minute); index != -1 {
		result = strings.ReplaceAll(result, minute, t.Format("04"))
	}
	if index := strings.Index(result, second); index != -1 {
		result = strings.ReplaceAll(result, second, t.Format("05"))
	}

	return result
}

func (r *Format) expandURL(URL string, t *time.Time, batchId, collectorInstanceId, hostIpInfo string, category string) string {
	if r.timeEndIndex > 0 {
		timeValue := t.Format(r.timeLayout)
		URL = URL[:r.timeStartIndex] + timeValue + URL[r.timeEndIndex+1:]
	}
	URL = r.replaceDateTokensInURL(t, URL)
	URL = r.replaceTokensInURL(URL, batchId, collectorInstanceId, hostIpInfo, category)
	return URL
}

func (r *Format) replaceTokensInURL(URL, batchId, collectorInstanceId, hostIpInfo, category string) string {
	URL = strings.ReplaceAll(URL, "${collectorId}", collectorInstanceId)
	URL = strings.ReplaceAll(URL, "${batchId}", batchId)
	URL = strings.ReplaceAll(URL, "${hostIpInfo}", hostIpInfo)
	URL = strings.ReplaceAll(URL, "${category}", category)
	return URL
}
