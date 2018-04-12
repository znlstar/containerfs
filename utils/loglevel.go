// Copyright (c) 2017, tig.jd.com. All rights reserved.
// Use of this source code is governed by a Apache License 2.0 that can be found in the LICENSE file.

package utils

import (
	"github.com/tiglabs/containerfs/logger"
	"io"
	"net/http"
)

//Logleveldebug is tool for setting log level dynamicly to debug
func Logleveldebug(w http.ResponseWriter, req *http.Request) {
	logger.SetLevel(logger.DEBUG)
	io.WriteString(w, "ok!\n")
}

//Loglevelerror is tool for setting log level dynamicly to error
func Loglevelerror(w http.ResponseWriter, req *http.Request) {
	logger.SetLevel(logger.ERROR)
	io.WriteString(w, "ok!\n")
}
