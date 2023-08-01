package proxy

import (
	"encoding/json"
	"github.com/francoispqt/gojay"
	"github.com/viant/rta/collector"
	"github.com/viant/xunsafe"
	"io/ioutil"
	"net/http"
	"reflect"
	"sync"
	"unsafe"
)

type Handler struct {
	collector  collector.Collector
	targetType reflect.Type
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

const collectorThread = 4

func (h *Handler) Handle(data []byte) (err error) {
	sliceValuePtr := reflect.New(h.targetType)
	request := &Request{}
	request.Records = sliceValuePtr.Interface()
	if err := gojay.Unmarshal(data, request); err != nil {
		return err
	}
	slicePtr := xunsafe.AsPointer(sliceValuePtr.Interface())
	xSlice := xunsafe.NewSlice(h.targetType, slicePtr)
	sliceLen := xSlice.Len(slicePtr)
	threadRanges := partedSliceUpperBounds(sliceLen, collectorThread)
	wg := sync.WaitGroup{}
	wg.Add(len(threadRanges))
	lowerBound := 0
	for j := 0; j < len(threadRanges); j++ {
		upperBound := threadRanges[j]
		if j > 0 {
			lowerBound = threadRanges[j-1]
		} else {
			lowerBound = 0
		}

		go func(lBound, uBound int) {
			defer wg.Done()
			cErr := h.collectinBackground(lBound, uBound, xSlice, slicePtr)
			if cErr != nil {
				err = cErr
			}
		}(lowerBound, upperBound)
	}

	wg.Wait()
	return err
}

func (h *Handler) collectinBackground(lowerBound int, upperBound int, xSlice *xunsafe.Slice, slicePtr unsafe.Pointer) error {
	for i := lowerBound; i < upperBound; i++ {

		record := xSlice.ValuePointerAt(slicePtr, i)
		if err := h.collector.Collect(record); err != nil {
			return err
		}
	}
	return nil
}

// partedSliceUpperBounds produces slice of upper bounds for given slice length and divisor
// We assume that every part of slice will have len > 0
func partedSliceUpperBounds(sliceLen int, divisor int) []int {
	if divisor == 0 {
		divisor = 1
	}

	delta := sliceLen / divisor
	partsCnt := divisor
	if delta < 1 {
		delta = 1
		partsCnt = sliceLen
	}

	var ranges = make([]int, partsCnt)

	for i := 1; i <= partsCnt; i++ {
		upperBound := i * delta
		if i == partsCnt {
			upperBound = sliceLen
		}
		ranges[i-1] = upperBound
	}
	return ranges
}

func (h *Handler) readPayload(request *http.Request) ([]byte, error) {
	data, err := ioutil.ReadAll(request.Body)
	if err != nil {
		return nil, err
	}
	data, err = uncompressContentIfNeeded(request, data)
	return data, err
}

func NewHandler(aCollector collector.Collector, targetSliceType reflect.Type) *Handler {
	return &Handler{collector: aCollector, targetType: targetSliceType}
}
