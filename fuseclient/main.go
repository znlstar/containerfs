package main

import (
	"errors"
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	"bazil.org/fuse"
	"bazil.org/fuse/fs"
	"golang.org/x/net/context"

	cfs "ipd.org/containerfs/fs"
)

type FS struct {
	cfs *cfs.CFS
}

type Dir struct {
	mu   sync.RWMutex
	fs   *FS
	path string
}
type File struct {
	mu      sync.RWMutex
	dir     *Dir
	name    string
	writers uint
}

func main() {
	err := mount(os.Args[1], os.Args[2])
	if err != nil {
		log.Fatal(err)
	}
}

func mount(uuid, mountpoint string) error {
	cfs := cfs.OpenFileSystem(uuid)
	c, err := fuse.Mount(mountpoint,
		fuse.FSName("ContainerFS-"+uuid),
		fuse.LocalVolume(),
		fuse.VolumeName("ContainerFS-"+uuid))
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

var _ = fs.FS(&FS{})

func (f *FS) Root() (fs.Node, error) {
	n := &Dir{
		fs:   f,
		path: "/",
	}
	return n, nil
}

var _ = fs.Node(&Dir{})

func (d *Dir) Attr(ctx context.Context, a *fuse.Attr) error {
	a.Mode = os.ModeDir | 0755
	return nil
}

func (d *Dir) ReadDirAll(ctx context.Context) ([]fuse.Dirent, error) {
	fmt.Println("ReadDirAll...")

	var res []fuse.Dirent

	ret, inodes := d.fs.cfs.List(d.path)
	if ret == 2 {
		return nil, errors.New("dir no longer exists")
	}
	if ret != 0 {
		return nil, errors.New("error")
	}
	for _, v := range inodes {
		de := fuse.Dirent{
			Name: v.Name,
		}
		if v.InodeType {
			de.Type = fuse.DT_File
		} else {
			de.Type = fuse.DT_Dir
		}
		res = append(res, de)
	}
	return res, nil
}

var _ = fs.NodeStringLookuper(&Dir{})

func (d *Dir) Lookup(ctx context.Context, name string) (fs.Node, error) {
	fmt.Println("Lookup...")
	fmt.Println(d.path + name)

	var n fs.Node
	ret, inode := d.fs.cfs.Stat(d.path + name)
	fmt.Println(ret)

	if ret == 2 {
		return nil, fuse.ENOENT
	}

	if ret != 0 {
		return nil, fuse.ENOENT
	}
	if inode.InodeType {
		n = &File{
			dir:  d,
			name: name,
		}
	} else {
		n = &Dir{
			fs:   d.fs,
			path: d.path + name,
		}

	}
	return n, nil
}

var _ = fs.NodeMkdirer(&Dir{})

func (d *Dir) Mkdir(ctx context.Context, req *fuse.MkdirRequest) (fs.Node, error) {
	fmt.Println("mkdir...")
	fmt.Println(req)

	ret := d.fs.cfs.CreateDir(d.path + req.Name)
	if ret == -1 {
		fmt.Print("create dir failed\n")
		return nil, errors.New("create dir failed")
	}
	if ret == 1 {
		fmt.Print("not allowed\n")
		return nil, errors.New("not allowed")
	}
	if ret == 2 {
		fmt.Print("no parent path\n")
		return nil, errors.New("no parent path")
	}
	if ret == 17 {
		fmt.Print("already exist\n")
		return nil, errors.New("already exist")
	}

	n := &Dir{
		fs:   d.fs,
		path: d.path + req.Name,
	}
	return n, nil
}

var _ = fs.NodeCreater(&Dir{})

func (d *Dir) Create(ctx context.Context, req *fuse.CreateRequest, resp *fuse.CreateResponse) (fs.Node, fs.Handle, error) {
	ret, _ := d.fs.cfs.OpenFile(d.path+req.Name, cfs.O_WRONLY)
	if ret != 0 {
		fmt.Println("touch failed")
		return nil, nil, errors.New("create file failed")
	}

	f := &File{
		dir:     d,
		name:    req.Name,
		writers: 1,
		// file is empty at Create time, no need to set data
	}
	return f, f, nil
}

var _ = fs.NodeRemover(&Dir{})

func (d *Dir) Remove(ctx context.Context, req *fuse.RemoveRequest) error {
	fmt.Println("remove...")
	fmt.Println(req)
	ret := d.fs.cfs.DeleteDir(d.path + req.Name)
	if ret != 0 {
		if ret == 2 {
			fmt.Println("not allowed")
			return errors.New("not allowed")
		} else {
			fmt.Println("delete dir failed")
			return errors.New("delete dir failed")
		}
	}
	return nil
}

var _ = fs.Node(&File{})
var _ = fs.Handle(&File{})

func (f *File) load(fn func([]byte)) error {
	return nil
}

func (f *File) Attr(ctx context.Context, a *fuse.Attr) error {

	f.mu.Lock()
	defer f.mu.Unlock()
	fmt.Println("Attr...")
	fmt.Println(a)
	ret, inode := f.dir.fs.cfs.Stat(f.dir.path + f.name)
	fmt.Println(f.dir.path + f.name)
	fmt.Println(ret)
	fmt.Println(inode)

	a.Ctime = time.Unix(inode.ModifiTime, 0)
	a.Mtime = time.Unix(inode.ModifiTime, 0)
	a.Atime = time.Unix(inode.AccessTime, 0)
	a.Size = uint64(inode.FileSize)
	a.Inode = uint64(inode.InodeID)
	a.Mode = 0666

	return nil
}

var _ = fs.NodeOpener(&File{})

func (f *File) Open(ctx context.Context, req *fuse.OpenRequest, resp *fuse.OpenResponse) (fs.Handle, error) {

	return f, nil
}

var _ = fs.HandleReleaser(&File{})

func (f *File) Release(ctx context.Context, req *fuse.ReleaseRequest) error {

	return nil
}

var _ = fs.HandleReader(&File{})

func (f *File) Read(ctx context.Context, req *fuse.ReadRequest, resp *fuse.ReadResponse) error {

	return nil
}

var _ = fs.HandleWriter(&File{})

const maxInt = int(^uint(0) >> 1)

func (f *File) Write(ctx context.Context, req *fuse.WriteRequest, resp *fuse.WriteResponse) error {
	return nil
}

var _ = fs.HandleFlusher(&File{})

func (f *File) Flush(ctx context.Context, req *fuse.FlushRequest) error {

	return nil
}

var _ = fs.NodeSetattrer(&File{})

func (f *File) Setattr(ctx context.Context, req *fuse.SetattrRequest, resp *fuse.SetattrResponse) error {

	return nil
}
