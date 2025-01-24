package main

import (
	"github.com/viant/rta/mergefs"
	"os"
)

func main() {
	mergefs.RunApp(os.Args[1])
}
