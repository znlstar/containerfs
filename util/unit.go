package util

const (
	_  = iota
	KB = 1 << (10 * iota)
	MB
	GB
	DefaultVolSize     = 120 * GB
	TaskWorkerInterval = 1
)
