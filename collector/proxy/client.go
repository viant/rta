package proxy

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/pkg/errors"
	"io/ioutil"
	"net"
	"net/http"
	"sync"
)

type Client struct {
	config *Config
	client *http.Client
	mux    sync.RWMutex
}

func (c *Client) Load(ctx context.Context, data interface{}, batchID string) error {
	request := &Request{BatchID: batchID, Records: data}
	httpClient := c.httpClient(&c.config.Endpoint)
	httpRequest, err := request.httpRequest(&c.config.Endpoint)
	if err != nil {
		return err
	}
	httpResponse, err := httpClient.Do(httpRequest)
	if err != nil {
		return err
	}
	return c.handleResponse(httpResponse)
}

func (c *Client) handleResponse(httpResponse *http.Response) error {
	if httpResponse.Body == nil {
		return fmt.Errorf("response body was empty")
	}
	defer httpResponse.Body.Close()
	responseData, err := ioutil.ReadAll(httpResponse.Body)
	if err != nil {
		return err
	}
	response := &Response{}
	err = json.Unmarshal(responseData, response)
	if err != nil {
		return err
	}
	if response.Error != "" {
		return errors.New(response.Error)
	}
	return nil
}

func (c *Client) httpClient(endpoint *Endpoint) *http.Client {
	c.mux.RLock()
	client := c.client
	c.mux.RUnlock()
	if client != nil {
		return client
	}
	endpoint.Init()
	roundTripper := &http.Transport{
		Dial: (&net.Dialer{
			Timeout:   endpoint.RequestTimeout(),
			KeepAlive: endpoint.KeepAliveTime(),
		}).Dial,
		MaxIdleConns:          endpoint.MaxIdleConnections,
		IdleConnTimeout:       endpoint.IdleConnTimeout(),
		MaxIdleConnsPerHost:   endpoint.MaxIdleConnsPerHost,
		ResponseHeaderTimeout: endpoint.ResponseHeaderTimeout(),
		ExpectContinueTimeout: endpoint.RequestTimeout(),
		DisableCompression:    true,
	}
	c.mux.Lock()
	client = &http.Client{Transport: roundTripper}
	c.client = client
	c.mux.Unlock()
	return client
}
