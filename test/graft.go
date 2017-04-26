package main

import (
	"fmt"
	"github.com/lostz/graft"
	"time"
)

func main() {
	//graft.TestNew()

	chanState := make(chan bool, 100)
	//peers []string, me string, port int, chanState chan bool)

	r, err := graft.New([]string{"10.8.65.94:6001", "10.8.65.94:6002"}, "10.8.65.94:6000", 6000, chanState)
	if err != nil {
		return
	}
	for {
		fmt.Println(r.IsLeader())
		time.Sleep(1 * time.Second)
		//	r.Stop()
		//	time.Sleep(1 * time.Second)
		//	return
	}

}
