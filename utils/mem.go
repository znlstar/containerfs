// Copyright (c) 2017, TIG All rights reserved.
// Use of this source code is governed by a Apache License 2.0 that can be found in the LICENSE file.

package utils

import (
	"runtime"
	"syscall"
)

//MemStatus is wrapper of mem status
type MemStatus struct {
	All   uint64 `json:"all"`
	Used  uint64 `json:"used"`
	Free  uint64 `json:"free"`
	Usage float64
}

//MemStat is tool for collecting mem status info
func MemStat() MemStatus {
	memStat := new(runtime.MemStats)
	runtime.ReadMemStats(memStat)
	mem := MemStatus{}

	//only for linux/mac
	//system memory usage
	sysInfo := new(syscall.Sysinfo_t)
	err := syscall.Sysinfo(sysInfo)
	if err == nil {
		mem.All = sysInfo.Totalram
		mem.Free = sysInfo.Freeram
		mem.Used = mem.All - mem.Free
		mem.Usage = float64(mem.Used) / float64(mem.All)
	}
	return mem
}
