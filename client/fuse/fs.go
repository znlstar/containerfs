package fuse

import (
	"syscall"

	bfuse "bazil.org/fuse"
	"bazil.org/fuse/fs"
	"github.com/tiglabs/containerfs/cfs"
	"golang.org/x/net/context"
)

// FS struct
type FS struct {
	cfs *cfs.CFS
}

var _ = fs.FS(&FS{})

// Root ...
func (fs *FS) Root() (fs.Node, error) {
	n := newDir(fs, 0, nil, "")
	return n, nil
}

// Statfs ...
func (fs *FS) Statfs(ctx context.Context, req *bfuse.StatfsRequest, resp *bfuse.StatfsResponse) error {
	err, ret := fs.cfs.GetFSInfo()
	if err != 0 {
		return bfuse.Errno(syscall.EIO)
	}
	resp.Bsize = 4 * 1024
	resp.Frsize = resp.Bsize
	resp.Blocks = ret.TotalSpace / uint64(resp.Bsize)
	resp.Bfree = ret.FreeSpace / uint64(resp.Bsize)
	resp.Bavail = ret.FreeSpace / uint64(resp.Bsize)
	return nil
}

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
			bfuse.MaxReadahead(128*1024),
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
		if err := c.MountError; err != nil {
			return err
		}

		return nil
	}

	c, err := bfuse.Mount(
		mountPoint,
		bfuse.ReadOnly(),
		bfuse.AllowOther(),
		bfuse.MaxReadahead(128*1024),
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
	if err := c.MountError; err != nil {
		return err
	}

	return nil
}
