package proxy

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"strings"
)

func compressContent(data []byte) ([]byte, error) {
	buf := new(bytes.Buffer)
	writer := gzip.NewWriter(buf)
	var err error
	if _, err = io.Copy(writer, bytes.NewReader(data)); err != nil && err != io.EOF {
		return nil, err
	}
	if err = writer.Flush(); err != nil {
		return nil, err
	}
	if err = writer.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func uncompressContentIfNeeded(request *http.Request, data []byte) ([]byte, error) {
	encoding := request.Header.Get("Content-Encoding")
	if encoding == "" {
		return data, nil
	}

	if strings.ToLower(encoding) != "gzip" {
		return nil, fmt.Errorf("unsported encoding: %v", encoding)
	}
	reader, err := gzip.NewReader(bytes.NewReader(data))
	if err != nil {
		return nil, err
	}
	return ioutil.ReadAll(reader)
}
