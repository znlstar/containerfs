// Copyright (c) 2017, tig.jd.com. All rights reserved.
// Use of this source code is governed by a Apache License 2.0 that can be found in the LICENSE file.

package utils

import (
	"os"
)

//LocalPathExists check existence of path
func LocalPathExists(path string) (bool, error) {
	_, err := os.Stat(path)
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return false, err
}
