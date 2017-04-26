package main

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"strconv"
	"sync"
)

var Wg sync.WaitGroup

func wr(out string) {

	defer Wg.Add(-1)

	in, err := os.Open("/tmp/mnt4/testbigbig.data")
	if err != nil {
		fmt.Println(err)
	}

	defer in.Close()

	o, err1 := os.OpenFile(out, os.O_RDWR|os.O_APPEND|os.O_CREATE, 0666)
	if err1 != nil { //打开文件
		fmt.Println("文件存在")
	}
	/*else {
		o, err = os.Create(out) //创建文件
		fmt.Println("文件不存在")
	}*/

	defer o.Close()

	_, err = in.Seek(3227918032, 0)
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
	for i := 1; i < 4; i++ {

		out := "/root/jlsxx" + strconv.Itoa(i)
		//o, _ := os.OpenFile(out,os.O_RDONLY,0666)
		Wg.Add(1)
		go wr(out)
	}
	Wg.Wait()
}
