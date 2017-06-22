package raftopt

import (
	"fmt"
)

type Address struct {
	Heartbeat string
	Replicate string
	Grpc      string
}

var AddrDatabase = make(map[uint64]*Address)

func AddInit(ips []string) {
	fmt.Println("IPS:")
	for i := range ips {
		AddrDatabase[uint64(i+1)] = &Address{
			Heartbeat: fmt.Sprintf("%s:99%d1", ips[i], i),
			Replicate: fmt.Sprintf("%s:99%d2", ips[i], i),
			Grpc:      fmt.Sprintf("%s:99%d3", ips[i], i),
		}
		fmt.Println(AddrDatabase[uint64(i+1)])
	}
}
