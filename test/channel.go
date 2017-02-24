package main

import (
	"fmt"
	"time"
)

type a struct {
	b int
}

func main() {
	var ch chan *a
	ch = make(chan *a, 1024)

	for true {
		time.Sleep(time.Second)
		go readThread(ch)

	}
	for i := 1; i <= 100*1024*1024*1024; i++ {
		time.Sleep(time.Second)
		tmp := a{b: i}
		ch <- &tmp
	}
	//fmt.Println("waitting for reading...")

}

func writeThread(ch chan *a, i int) {
	tmp := a{b: i}
	ch <- &tmp
}

func readThread(ch chan *a) {
	if len(ch) != 0 {
		a := <-ch
		fmt.Println(a.b)
	}
}
