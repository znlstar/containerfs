// Copyright (c) 2017, TIG All rights reserved.
// Use of this source code is governed by a Apache License 2.0 that can be found in the LICENSE file.package utils

package utils

import (
	"runtime"
)

// Version info
var (
	Release string
	Build   string
	Go      string
)

//Version is a tool for gets version info
func Version() string {
	if Release == "" {
		Release = "0.0.0"
	}
	return "Release: " + Release + " Build: " + Build + " Go: " + runtime.Version() + " " + runtime.GOOS + " " + runtime.GOARCH
}
