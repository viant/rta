package main

import (
	"fmt"
	"github.com/viant/rta/mergefs"
	"os"
)

func main() {
	err := mergefs.RunApp(os.Args[1])
	if err != nil {
		fmt.Println(err)
	}
}
