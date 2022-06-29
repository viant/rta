package handler

import "net/http"

const StatusURI = "/status/"

//StatusOK returns up status
func StatusOK(writer http.ResponseWriter, request *http.Request) {
	if request.ContentLength > 0 {
		_ = request.Body.Close()
	}
	writer.WriteHeader(http.StatusOK)
}
