/*

100w: 150ms
[root@localhost tools]# ./map
init start:
2017-01-20 09:36:30.493913113 +0800 CST
init end:
2017-01-20 09:36:31.684920345 +0800 CST
search end:
2017-01-20 09:36:31.836435935 +0800 CST
encode end:
2017-01-20 09:36:39.211576438 +0800 CST
save end:
2017-01-20 09:36:43.216535676 +0800 CST

50w: 15ms
[root@localhost benchmark]# ./map
init start:
2017-01-20 15:22:31.025096831 +0800 CST
init end:
2017-01-20 15:22:31.532721708 +0800 CST
search end:
2017-01-20 15:22:31.547883246 +0800 CST
encode end:
2017-01-20 15:22:34.917958661 +0800 CST
save end:
2017-01-20 15:22:36.997377969 +0800 CST

10w: 3ms
[root@localhost benchmark]# ./map
init start:
2017-01-20 15:21:39.658829011 +0800 CST
init end:
2017-01-20 15:21:39.752701258 +0800 CST
search end:
2017-01-20 15:21:39.755650307 +0800 CST
encode end:
2017-01-20 15:21:40.342956545 +0800 CST
save end:
2017-01-20 15:21:40.79561728 +0800 CST
*/
package main

import (
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"time"
)

var smalldb map[string]map[string]string
var logstring string

func main() {
	smalldb = make(map[string]map[string]string)
	smallsubdb := make(map[string]string)
	logstring = "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"
	str1 := "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"
	fmt.Println("init start:")
	fmt.Println(time.Now())
	for i := 0; i < 500001; i++ {
		str2 := str1 + strconv.Itoa(i)
		smallsubdb[str2] = logstring
	}
	smalldb["FirstSubDB"] = smallsubdb
	fmt.Println("init end:")
	fmt.Println(time.Now())

	for k, _ := range smalldb["FirstSubDB"] {
		if k == "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx1000000" {
			fmt.Println("I am 500000")
		}
	}

	fmt.Println("search end:")
	fmt.Println(time.Now())
	b, _ := json.Marshal(smalldb)

	fmt.Println("encode end:")
	fmt.Println(time.Now())
	f, _ := os.Create("./smalldb.json") //创建文件
	defer f.Close()
	f.Write(b) //写入文件(字节数组)
	f.Sync()

	fmt.Println("save end:")
	fmt.Println(time.Now())
}
