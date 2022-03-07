package shared

import "strings"

const (
	PendingSuffix         = "-pending"
	DefaultMaxMessageSize = 2048
	DefaultConcurrency    = 8
)

func BatchID(URL string) string {
	index := strings.LastIndex(URL, "_")
	return URL[index+1:]
}
