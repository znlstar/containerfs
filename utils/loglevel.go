package utils

import (
	"github.com/tiglabs/containerfs/logger"
	"io"
	"net/http"
)

func Logleveldebug(w http.ResponseWriter, req *http.Request) {
	logger.SetLevel(logger.DEBUG)
	io.WriteString(w, "ok!\n")
}

func Loglevelerror(w http.ResponseWriter, req *http.Request) {
	logger.SetLevel(logger.ERROR)
	io.WriteString(w, "ok!\n")
}
