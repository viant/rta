package proxy

import (
	"encoding/json"
	"github.com/francoispqt/gojay"
	"github.com/viant/rta/collector"
	"github.com/viant/xunsafe"
	"io/ioutil"
	"net/http"
	"reflect"
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

func (h *Handler) Handle(data []byte) error {
	sliceValuePtr := reflect.New(h.targetType)
	request := &Request{}
	request.Records = sliceValuePtr.Interface()
	if err := gojay.Unmarshal(data, request); err != nil {
		return err
	}
	slicePtr := xunsafe.AsPointer(sliceValuePtr.Interface())
	xSlice := xunsafe.NewSlice(h.targetType, slicePtr)
	for i := 0; i < xSlice.Len(slicePtr); i++ {
		record := xSlice.ValuePointerAt(slicePtr, i)
		if err := h.collector.Collect(record); err != nil {
			return err
		}
	}
	return nil
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
