package main

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"strconv"
	"sync"
	//"time"
)

var Wg sync.WaitGroup

func wr(out string) {

	defer Wg.Add(-1)

	in, err := os.Open("/tmp/testbig.data")
	if err != nil {
		fmt.Println(err)
	}

	defer in.Close()

	o, err := os.OpenFile(out, os.O_RDWR|os.O_APPEND|os.O_CREATE, 0666)
	//o, err := os.Create(out)
	if err != nil {
		fmt.Println("---Createfile err:")
		fmt.Println(err)
		return
	}
	defer o.Close()
	reader := bufio.NewReader(in)
	writer := bufio.NewWriter(o)
	eof := false
	buf := make([]byte, 1024*1024)
	for !eof {
		//line, err := reader.ReadString('\n') //每次读取到换行的地方
		n, err := reader.Read(buf)
		if err == io.EOF {
			err = nil
			eof = true
		} else if err != nil {
			fmt.Println("---read err:")
			fmt.Println(err)
			break
		}
		_, err = writer.Write(buf[:n])
		if err != nil {
			fmt.Println("---write err:")
			fmt.Println(err)
			break
		}
		writer.Flush() //将缓冲区的数据写入到io.Writer接口
	}
}

func main() {
	for i := 0; i < 1; i++ {
		out := "/tmp/mnt4/jbig" + strconv.Itoa(i)
		Wg.Add(1)
		go wr(out)
		//time.Sleep(time.Second)
	}
	Wg.Wait()
}
