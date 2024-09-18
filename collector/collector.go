package collector

// Collector represents a collector
type Collector interface {
	Collect(record interface{}) error
	CollectAll(record ...interface{}) error
	ID() string
}
