//go:build amd64 && !nosimd

package fmap

import (
	_ "unsafe"
)

const (
	groupSize = 16
)
