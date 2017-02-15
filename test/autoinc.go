package main

import (
	"fmt"
	"ipd.org/containerfs/utils"
)

func main() {
	ai := utils.New(1, 1)
	id1 := ai.Id()
	ai.Id()
	ai.Id()
	ai.Id()
	ai.Id()
	ai.Id()
	id7 := ai.Id()
	ai.Close()
	fmt.Println(id1)
	fmt.Println(id7)
}
