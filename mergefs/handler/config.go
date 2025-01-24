package handler

import (
	"encoding/json"
	"net/http"
)

const ConfigURI = "/v1/api/config/"

type handler struct {
	Config interface{}
}

// ServeHTTP writes the configuration to the response.
func (h *handler) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	JSON, _ := json.Marshal(h.Config)
	writer.Header().Set("Content-Type", "application/json")
	writer.Write(JSON)
}

// NewHandler creates a new handler with the given configuration.
func NewHandler(config interface{}) http.Handler {
	return &handler{Config: config}
}
