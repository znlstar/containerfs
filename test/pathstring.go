package main

import (
	"fmt"
	"strings"
)

func getParentName(in string) (parentName string) {
	tmp := strings.Split(in, "/")
	parentName = tmp[len(tmp)-2]
	if parentName == "" {
		parentName = "/"
	}
	return
}

func getKeyStringByFullPath(in string) {
	tmp := strings.Split(in, "/")
	fmt.Println(tmp)
}

func get(key string) string {
	InodeDB := make(map[string]string)
	InodeDB["2"] = "2"
	return InodeDB[key]
}

func main() {
	//    fmt.Println(getParentName("/home/zxc/a"))
	//    fmt.Println(getParentName("/home/zxc"))
	//    fmt.Println(getParentName("/home"))
	getKeyStringByFullPath("/home/a/b/c")
	if a := get("1"); a != "" {
		fmt.Println("yes")
	} else {
		fmt.Println("no")
	}
	b := get("2")
	fmt.Println(b)
}
