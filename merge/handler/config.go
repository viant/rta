package handler

import (
	"encoding/json"
	"net/http"
)

const ConfigURI = "/v1/api/config/"

type handler struct {
	Config interface{}
}

func (h *handler) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	JSON, _ := json.Marshal(h.Config)
	writer.Header().Set("Content-Type", "application/json")
	writer.Write(JSON)
}

func NewHandler(config interface{}) http.Handler {
	return &handler{Config: config}
}
