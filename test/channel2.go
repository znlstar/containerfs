package main

import "fmt"
import "time"
import "strconv"
import "math/rand"

var ch0 chan []int
var ch1 chan []int
var ch2 chan []int


func foo(i int,v chan string) {
    var a string
    rand.Seed(int64(time.Now().Nanosecond()))
    for i:=0;i<5;i++{
	a += strconv.Itoa(rand.Intn(20))
    }
    fmt.Printf("### the ch:%v have value:%v ###\n",i,a)
    v <- a // 发消息：我执行完啦！  
}


func main() {
    var m map[int]chan string
    m = make(map[int]chan string)
    for i:=0;i<3;i++{
	m[i] = make(chan string, 5)
    }


    //ch0 = make(chan []int, 5) // 缓冲1000个数据
    //ch1 = make(chan []int, 5)
    //ch2 = make(chan []int, 5)

    for i, v := range m {
            go foo(i,v)
    }

    for i:=0;i<3;i++ {
	fmt.Printf("*** read ch:%v value:%v ***\n",i,<-m[i])
    }
}
