package proxy

import "time"

type (
	Config struct {
		Endpoint      Endpoint
		MaxCollectors int
	}

	Endpoint struct {
		URL                 string
		MaxIdleConnections  int `json:"MaxIdleConnections"`
		MaxIdleConnsPerHost int `json:"MaxIdleConnsPerHost"`

		idleConnTimeout   time.Duration
		IdleConnTimeoutMs int `json:"IdleConnTimeoutMs"`

		RequestTimeoutMs int `json:"RequestTimeoutMs"`
		requestTimeout   time.Duration

		ResponseHeaderTimeoutMs int `json:"ResponseHeaderTimeoutMs"`
		responseHeaderTimeout   time.Duration

		KeepAliveTimeMs int `json:"KeepAliveTimeMs"`
		keepAliveTime   time.Duration

		Content Content
	}

	Content struct {
		CompressionSizeKb int
	}
)

func (c *Content) CompressionSize() int {
	return 1024 * c.CompressionSizeKb
}

func (e *Endpoint) Init() {
	if e.MaxIdleConnections == 0 {
		e.MaxIdleConnections = 500
	}
	if e.MaxIdleConnsPerHost == 0 {
		e.MaxIdleConnections = 5
	}
}

func (e *Endpoint) IdleConnTimeout() time.Duration {
	if e.idleConnTimeout != 0 {
		return e.idleConnTimeout
	}
	if e.IdleConnTimeoutMs == 0 {
		e.IdleConnTimeoutMs = 300
	}
	e.idleConnTimeout = time.Millisecond * time.Duration(e.IdleConnTimeoutMs)
	return e.idleConnTimeout
}

func (e *Endpoint) RequestTimeout() time.Duration {
	if e.requestTimeout != 0 {
		return e.requestTimeout
	}
	if e.RequestTimeoutMs == 0 {
		e.RequestTimeoutMs = 5000
	}
	e.requestTimeout = time.Millisecond * time.Duration(e.RequestTimeoutMs)
	return e.requestTimeout
}

func (e *Endpoint) ResponseHeaderTimeout() time.Duration {
	if e.responseHeaderTimeout != 0 {
		return e.responseHeaderTimeout
	}
	if e.ResponseHeaderTimeoutMs == 0 {
		e.ResponseHeaderTimeoutMs = 540
	}
	e.responseHeaderTimeout = time.Millisecond * time.Duration(e.ResponseHeaderTimeoutMs)
	return e.responseHeaderTimeout
}

func (e *Endpoint) KeepAliveTime() time.Duration {
	if e.keepAliveTime != 0 {
		return e.keepAliveTime
	}
	e.keepAliveTime = time.Millisecond * time.Duration(e.KeepAliveTimeMs)
	return e.keepAliveTime
}

func (e *Endpoint) Clone() *Endpoint {
	result := *e
	return &result
}

func (c *Config) Clone() *Config {
	result := *c
	result.Endpoint = *c.Endpoint.Clone()
	return &result
}
