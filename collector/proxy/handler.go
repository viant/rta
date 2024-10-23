package proxy

import (
	"encoding/json"
	"fmt"
	"github.com/francoispqt/gojay"
	"github.com/viant/rta/collector"
	"github.com/viant/rta/shared"
	"github.com/viant/xunsafe"
	"io/ioutil"
	"net/http"
	"reflect"
	"sync"
)

type (
	Handler struct {
		collectors        []*Collector
		targetType        reflect.Type
		index             map[string]int
		useNamedCollector bool
	}

	Collector struct {
		id string
		collector.Collector
		xSlice *xunsafe.Slice
	}
)

func (c *Collector) SliceLen(source interface{}) int {
	slicePtr := xunsafe.AsPointer(source)
	return c.xSlice.Len(slicePtr)
}

func (c *Collector) CollectAll(source interface{}, from, to int, errors *shared.Errors, wg *sync.WaitGroup) {
	if wg != nil {
		defer wg.Done()
	}
	if c.xSlice.Type.Elem().Kind() == reflect.Interface {
		converted := reflect.ValueOf(source).Elem().Convert(reflect.TypeOf([]interface{}{}))
		var iSlice = converted.Interface().([]interface{})
		if err := c.Collector.CollectAll((iSlice)[from:to]...); err != nil {
			errors.Add(err)
			return
		}
		return
	}

	slicePtr := xunsafe.AsPointer(source)
	for i := from; i < to; i++ {
		record := c.xSlice.ValuePointerAt(slicePtr, i)
		if err := c.Collector.Collect(record); err != nil {
			errors.Add(err)
			return
		}
	}
}

func (h *Handler) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	data, err := h.readPayload(request)
	if err != nil {
		h.buildResponse(writer, err)
		return
	}
	err = h.Handle(data)
	h.buildResponse(writer, err)
}

func (h *Handler) buildResponse(writer http.ResponseWriter, err error) {
	resp := &Response{Status: "ok"}
	if err != nil {
		resp.Status = "err"
		resp.Error = err.Error()
	}
	data, err := json.Marshal(resp)
	if err != nil {
		http.Error(writer, err.Error(), http.StatusInternalServerError)
		return
	}
	writer.Write(data)
}

func (h *Handler) Handle(data []byte) error {
	sliceValuePtr := reflect.New(h.targetType)
	request := &Request{}
	request.Records = sliceValuePtr.Interface()
	if err := gojay.Unmarshal(data, request); err != nil {
		fmt.Printf("rta-proxy-handler-handle: unmarshal error: %v, data size: %d data: %s", err, len(data), string(data))
		return err
	}

	recordCount := h.collectors[0].SliceLen(request.Records)
	if recordCount == 0 {
		return nil
	}

	collectorCnt := len(h.collectors)
	if collectorCnt == 0 {
		return fmt.Errorf("rta collector proxy handler: no collectors defined")
	}
	errors := &shared.Errors{}
	if h.useNamedCollector {
		idx, ok := h.index[request.CollectorID]
		if !ok {
			return fmt.Errorf("failed to lookup collector: %v", request.CollectorID)
		}
		h.collectors[idx].CollectAll(request.Records, 0, recordCount, errors, nil)
		return errors.First()
	}

	if collectorCnt == 1 {
		h.collectors[0].CollectAll(request.Records, 0, recordCount, errors, nil)
	} else {

		wg := sync.WaitGroup{}
		chunkSize := recordCount / collectorCnt
		for n := 0; n < collectorCnt; n++ {
			end := (n + 1) * chunkSize
			if n == collectorCnt-1 {
				end = recordCount
			}
			wg.Add(1)
			go h.collectors[n].CollectAll(request.Records, n*chunkSize, end, errors, &wg)
		}
		wg.Wait()
	}
	return errors.First()
}

func (h *Handler) readPayload(request *http.Request) ([]byte, error) {
	data, err := ioutil.ReadAll(request.Body)
	if err != nil {
		return nil, err
	}
	data, err = uncompressContentIfNeeded(request, data)
	return data, err
}

func NewHandler(targetSliceType reflect.Type, useNamedCollector bool, collectors ...collector.Collector) *Handler {
	var result = &Handler{targetType: targetSliceType, index: make(map[string]int), useNamedCollector: useNamedCollector}
	result.collectors = make([]*Collector, len(collectors))
	for i, aCollector := range collectors {
		result.collectors[i] = &Collector{id: aCollector.ID(), Collector: aCollector, xSlice: xunsafe.NewSlice(targetSliceType)}
		if useNamedCollector {
			result.index[aCollector.ID()] = i
		}
	}
	return result
}
