package utils

import (
	"runtime"
)

var (
	Release string
	Build   string
	Go      string
)

// Version ...
func Version() string {
	if Release == "" {
		Release = "0.0.0"
	}
	return "Release: " + Release + " Build: " + Build + " Go: " + runtime.Version() + " " + runtime.GOOS + " " + runtime.GOARCH
}
