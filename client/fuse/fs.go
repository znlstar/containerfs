// Copyright (c) 2017, TIG All rights reserved.
// Use of this source code is governed by a Apache License 2.0 that can be found in the LICENSE file.

package fuse

import (
	"syscall"

	bfuse "bazil.org/fuse"
	"bazil.org/fuse/fs"
	"github.com/tiglabs/containerfs/cfs"
	"golang.org/x/net/context"
)

// SIZE ...
const (
	BLOCK_SIZE     = 4096
	MAX_READ_AHEAD = 128 * 1024
)

// FS to store CFS handler
type FS struct {
	cfs *cfs.CFS
}

var _ = fs.FS(&FS{})

// Root to create root directory
func (fs *FS) Root() (fs.Node, error) {
	n := newDir(fs, 0, nil, "")
	return n, nil
}

// Statfs to get stat of the filesystem
func (fs *FS) Statfs(ctx context.Context, req *bfuse.StatfsRequest, resp *bfuse.StatfsResponse) error {
	err, ret := fs.cfs.GetFSInfo()
	if err != 0 {
		return bfuse.Errno(syscall.EIO)
	}
	resp.Bsize = BLOCK_SIZE
	resp.Frsize = resp.Bsize
	resp.Blocks = ret.TotalSpace / uint64(resp.Bsize)
	resp.Bfree = ret.FreeSpace / uint64(resp.Bsize)
	resp.Bavail = ret.FreeSpace / uint64(resp.Bsize)
	return nil
}

// Mount to mount the filesystem by UUID
func Mount(uuid, mountPoint string, isReadOnly int) error {

	cfs := cfs.OpenFileSystem(uuid)
	if cfs == nil {
		return bfuse.Errno(syscall.EIO)
	}

	defer cfs.CloseFileSystem()

	if isReadOnly == 0 {
		c, err := bfuse.Mount(
			mountPoint,
			bfuse.AllowOther(),
			bfuse.MaxReadahead(MAX_READ_AHEAD),
			bfuse.AsyncRead(),
			bfuse.WritebackCache(),
			bfuse.FSName("ContainerFS-"+uuid),
			bfuse.LocalVolume(),
			bfuse.VolumeName("ContainerFS-"+uuid))
		if err != nil {
			return err
		}
		defer c.Close()

		filesys := &FS{
			cfs: cfs,
		}
		if err := fs.Serve(c, filesys); err != nil {
			return err
		}
		// check if the mount process has an error to report
		<-c.Ready
		return c.MountError
	}

	c, err := bfuse.Mount(
		mountPoint,
		bfuse.ReadOnly(),
		bfuse.AllowOther(),
		bfuse.MaxReadahead(MAX_READ_AHEAD),
		bfuse.AsyncRead(),
		bfuse.WritebackCache(),
		bfuse.FSName("ContainerFS-"+uuid),
		bfuse.LocalVolume(),
		bfuse.VolumeName("ContainerFS-"+uuid))
	if err != nil {
		return err
	}
	defer c.Close()

	filesys := &FS{
		cfs: cfs,
	}
	if err := fs.Serve(c, filesys); err != nil {
		return err
	}
	// check if the mount process has an error to report
	<-c.Ready
	return c.MountError
}
