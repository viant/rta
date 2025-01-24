package handler

import (
	"log"
	"net/http"
)

const StatusURI = "/status/"

// StatusOK returns up status
func StatusOK(writer http.ResponseWriter, request *http.Request) {
	if request.Body != nil {
		if err := request.Body.Close(); err != nil {
			log.Printf("rta mergefs - failed to close request body: %v", err)
		}
	}
	writer.WriteHeader(http.StatusOK)
}
