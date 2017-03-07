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
	cfile   *cfs.CFile
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

	fmt.Println(d)

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

	var fullPath string
	if d.path == "/" {
		fullPath = d.path + name
	} else {
		fullPath = d.path + "/" + name
	}

	fmt.Println(fullPath)

	var n fs.Node
	ret, inode := d.fs.cfs.Stat(fullPath)
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
			path: fullPath,
		}

	}
	return n, nil
}

var _ = fs.NodeMkdirer(&Dir{})

func (d *Dir) Mkdir(ctx context.Context, req *fuse.MkdirRequest) (fs.Node, error) {
	fmt.Println("mkdir...")
	fmt.Println(req)

	var fullPath string

	if d.path == "/" {
		fullPath = d.path + req.Name
	} else {
		fullPath = d.path + "/" + req.Name
	}

	ret := d.fs.cfs.CreateDir(fullPath)
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
		path: fullPath,
	}
	return n, nil
}

var _ = fs.NodeCreater(&Dir{})

func (d *Dir) Create(ctx context.Context, req *fuse.CreateRequest, resp *fuse.CreateResponse) (fs.Node, fs.Handle, error) {
	fmt.Println("Create in dir ,req: ")
	fmt.Println(req.Flags)
	var fullPath string

	if d.path == "/" {
		fullPath = d.path + req.Name
	} else {
		fullPath = d.path + "/" + req.Name
	}
	ret, cfile := d.fs.cfs.OpenFile(fullPath, int(req.Flags))
	if ret != 0 {
		fmt.Println("Create file failed")
		return nil, nil, errors.New("create file failed")
	}

	f := &File{
		dir:     d,
		name:    req.Name,
		writers: 1,
		cfile:   cfile,
	}
	return f, f, nil
}

var _ = fs.NodeRemover(&Dir{})

func (d *Dir) Remove(ctx context.Context, req *fuse.RemoveRequest) error {
	fmt.Println("remove...")
	fmt.Println(req)

	var fullPath string

	if d.path == "/" {
		fullPath = d.path + req.Name
	} else {
		fullPath = d.path + "/" + req.Name
	}
	if req.Dir {

		ret := d.fs.cfs.DeleteDir(fullPath)
		if ret != 0 {
			if ret == 2 {
				fmt.Println("not allowed")
				return errors.New("not allowed")
			} else {
				fmt.Println("delete dir failed")
				return errors.New("delete dir failed")
			}
		}
	} else {
		ret := d.fs.cfs.DeleteFile(fullPath)
		if ret != 0 {
			if ret == 2 {
				fmt.Println("not allowed")
				return errors.New("not allowed")
			} else {
				fmt.Println("delete file failed")
				return errors.New("delete file failed")
			}
		}
	}
	return nil
}

var _ = fs.Node(&File{})
var _ = fs.Handle(&File{})

func (f *File) Attr(ctx context.Context, a *fuse.Attr) error {

	f.mu.Lock()
	defer f.mu.Unlock()
	fmt.Println("Attr...")
	fmt.Println(a)

	var fullPath string

	if f.dir.path == "/" {
		fullPath = f.dir.path + f.name
	} else {
		fullPath = f.dir.path + "/" + f.name
	}

	ret, inode := f.dir.fs.cfs.Stat(fullPath)
	fmt.Println(f.dir.path + f.name)
	fmt.Println(ret)
	fmt.Println(inode)

	a.Ctime = time.Unix(inode.ModifiTime, 0)
	a.Mtime = time.Unix(inode.ModifiTime, 0)
	a.Atime = time.Unix(inode.AccessTime, 0)
	a.Size = uint64(inode.FileSize)
	a.Inode = uint64(inode.InodeID)
	a.Blocks = uint64(len(inode.ChunkIDs))
	a.BlockSize = 64 * 1024 * 1024
	a.Mode = 0666

	return nil
}

var _ = fs.NodeOpener(&File{})

func (f *File) Open(ctx context.Context, req *fuse.OpenRequest, resp *fuse.OpenResponse) (fs.Handle, error) {
	var ret int32
	fmt.Println("Open in file ...")
	fmt.Println(req.Flags)
	//if req.Flags.IsReadOnly() {
	// we don't need to track read-only handles
	//	return f, nil
	//}
	var fullPath string

	if f.dir.path == "/" {
		fullPath = f.dir.path + f.name
	} else {
		fullPath = f.dir.path + "/" + f.name
	}

	ret, f.cfile = f.dir.fs.cfs.OpenFile(fullPath, int(req.Flags))
	if ret != 0 {
		fmt.Println("open file failed")
		return nil, errors.New("open file failed")
	}
	f.writers++
	return f, nil
}

var _ = fs.HandleReleaser(&File{})

func (f *File) Release(ctx context.Context, req *fuse.ReleaseRequest) error {
	fmt.Println("Release in file ...")
	if req.Flags.IsReadOnly() {
		// we don't need to track read-only handles
		return nil
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	f.writers--
	f.cfile.Close()
	return nil
}

var _ = fs.HandleReader(&File{})

var d time.Duration
var d1 time.Duration

func (f *File) Read(ctx context.Context, req *fuse.ReadRequest, resp *fuse.ReadResponse) error {

	t1 := time.Now()
	length := f.cfile.Readf(&resp.Data, req.Offset, int64(req.Size))
	t2 := time.Now()

	d += t2.Sub(t1)

	if req.Offset == 4096 {
		fmt.Println("first in Read ...")
		fmt.Println(time.Now())
	}
	if req.Offset == 64*1024*1024 {
		fmt.Println("64MB in Read ...")
		fmt.Println(time.Now())
		fmt.Printf("Readf cost: %v\n", d)
	}

	if length == int64(req.Size) {
		return nil
	} else {
		fmt.Printf("Read cfile reqsize:%v, have readsize:%v \n", req.Size, length)
	}
	return nil
}

var _ = fs.HandleWriter(&File{})

func (f *File) Write(ctx context.Context, req *fuse.WriteRequest, resp *fuse.WriteResponse) error {
	//fmt.Println("Write in file ...")
	f.cfile.Write(req.Data, int32(len(req.Data)))
	resp.Size = len(req.Data)
	return nil
}

var _ = fs.HandleFlusher(&File{})

func (f *File) Flush(ctx context.Context, req *fuse.FlushRequest) error {
	fmt.Println("Flush .... ")
	//f.cfile.Flush()
	return nil
}

var _ = fs.NodeSetattrer(&File{})

func (f *File) Setattr(ctx context.Context, req *fuse.SetattrRequest, resp *fuse.SetattrResponse) error {

	return nil
}
