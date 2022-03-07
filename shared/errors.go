package shared

import "sync"

type Errors struct {
	Errors []error
	mux    sync.Mutex
}

func (e *Errors) Add(err error) {
	if err == nil {
		return
	}
	e.mux.Lock()
	defer e.mux.Unlock()
	e.Errors = append(e.Errors, err)
}

func (e *Errors) First() error {
	if len(e.Errors) == 0 {
		return nil
	}
	return e.Errors[0]
}
