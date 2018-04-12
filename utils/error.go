// Copyright (c) 2017, TIG All rights reserved.
// Use of this source code is governed by a Apache License 2.0 that can be found in the LICENSE file.

package utils

import (
	"errors"
)

//global file system error
var (
	ErrNotLeader = errors.New("not leader")
)
