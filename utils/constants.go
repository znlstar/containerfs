package utils

import (
	"time"
)

//cfs global errno
const (
	EOK        = iota
	ENOTFOUND  = 20
	ENOENT     = 21
	ENOTDEFIND = 22
)

//filetype
const (
	INODE_DIR     = 1
	INODE_FILE    = 2
	INODE_SYMLINK = 3
)

//fuse cache conf
const (
	FUSE_ATTR_CACHE_LIFE   = time.Second
	FUSE_LOOKUP_CACHE_LIFE = time.Second
)

//block and chunk

const (
	BlkSizeG       = 16
	BlockGroupSize = int64(BlkSizeG * 1024 * 1024 * 1024)
	ChunkSize      = 64 * 1024 * 1024
)
