package main

import (
	"encoding/json"
	"fmt"
)

type inode struct {
	Name string
	Size int
	Info []string
}

var testmapall map[string]map[string]inode

func main() {
	testmapall = make(map[string]map[string]inode)
	testmap := make(map[string]inode)
	testmap["str1"] = inode{Name: "str1", Size: 4, Info: []string{"aaa", "bbb"}}
	tmp := inode{Name: "str2", Size: 5}
	testmap["str2"] = tmp

	testmapall["SubMap1"] = testmap
	testmapall["SubMap2"] = testmap

	b, _ := json.Marshal(testmapall)
	fmt.Println(string(b))

	fmt.Println(testmapall)
}
