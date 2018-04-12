// Copyright (c) 2017, tig.jd.com. All rights reserved.
// Use of this source code is governed by a Apache License 2.0 that can be found in the LICENSE file.

package utils

import (
	"math/rand"
	"time"
)

// enerateRandomNumber generates a new sequence for random
func GenerateRandomNumber(start int, end int, count int) []int {
	if end < start || (end-start) < count {
		return nil
	}

	nums := make([]int, 0)
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	for len(nums) < count {
		num := r.Intn((end - start)) + start

		exist := false
		for _, v := range nums {
			if v == num {
				exist = true
				break
			}
		}

		if !exist {
			nums = append(nums, num)
		}
	}

	return nums
}
