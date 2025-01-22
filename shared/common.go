package shared

import (
	"fmt"
	"io"
)

const (
	Active   = 1
	InActive = 0
	Deleted  = -1
)

// CloseWithErrorHandling closes the closer and handles the error
func CloseWithErrorHandling(c io.Closer, err *error) {
	if c == nil {
		return
	}

	if cerr := c.Close(); cerr != nil {
		if err != nil && *err != nil {
			*err = fmt.Errorf("%w; close error: %v", *err, cerr)
		} else {
			*err = cerr
		}
	}
}
