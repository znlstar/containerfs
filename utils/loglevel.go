package utils

import (
	"github.com/tiglabs/containerfs/logger"
	"io"
	"net/http"
)

// Logleveldebug ...
func Logleveldebug(w http.ResponseWriter, req *http.Request) {
	logger.SetLevel(logger.DEBUG)
	io.WriteString(w, "ok!\n")
}

// Loglevelerror ...
func Loglevelerror(w http.ResponseWriter, req *http.Request) {
	logger.SetLevel(logger.ERROR)
	io.WriteString(w, "ok!\n")
}
