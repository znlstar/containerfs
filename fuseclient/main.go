package main

import (
	"bazil.org/fuse"
	"bazil.org/fuse/fs"
	"flag"
	"fmt"
	cfs "github.com/ipdcode/containerfs/fs"
	"github.com/ipdcode/containerfs/logger"
	"golang.org/x/net/context"
	"math"
	"os"
	"runtime/debug"
	"strings"
	"sync"
	"syscall"
	"time"
)

var uuid string
var mountPoint string

// FS struct
type FS struct {
	cfs *cfs.CFS
}

type dir struct {
	inode  uint64
	parent *dir
	fs     *FS

	// mu protects the fields below.
	//
	// If multiple dir.mu instances need to be locked at the same
	// time, the locks must be taken in topologically sorted
	// order, parent first.
	//
	// As there can be only one db.Update at a time, those calls
	// must be considered as lock operations too. To avoid lock
	// ordering related deadlocks, never hold mu while calling
	// db.Update.
	mu sync.Mutex

	name string

	// each in-memory child, so we can return the same node on
	// multiple Lookups and know what to do on .save()
	//
	// each child also stores its own name; if the value in the child
	// is an empty string, that means the child has been unlinked
	active map[string]*refcount
}

var _ = fs.FS(&FS{})

// Root ...
func (fs *FS) Root() (fs.Node, error) {
	n := newDir(fs, 0, nil, "")
	return n, nil
}

/*
   Blocks  uint64 // Total data blocks in file system.
   Bfree   uint64 // Free blocks in file system.
   Bavail  uint64 // Free blocks in file system if you're not root.
   Files   uint64 // Total files in file system.
   Ffree   uint64 // Free files in file system.
   Bsize   uint32 // Block size
   Namelen uint32 // Maximum file name length?
   Frsize  uint32 // Fragment size, smallest addressable data size in the file system.
*/

// Statfs ...
func (fs *FS) Statfs(ctx context.Context, req *fuse.StatfsRequest, resp *fuse.StatfsResponse) error {
	err, ret := cfs.GetFSInfo(fs.cfs.VolID)
	if err != 0 {
		return fuse.Errno(syscall.EIO)
	}
	resp.Bsize = 4 * 1024
	resp.Frsize = resp.Bsize
	resp.Blocks = ret.TotalSpace / uint64(resp.Bsize)
	resp.Bfree = ret.FreeSpace / uint64(resp.Bsize)
	resp.Bavail = ret.FreeSpace / uint64(resp.Bsize)
	return nil
}

type refcount struct {
	node   node
	kernel bool
	refs   uint32
}

func newDir(filesys *FS, inode uint64, parent *dir, name string) *dir {
	d := &dir{
		inode:  inode,
		name:   name,
		parent: parent,
		fs:     filesys,
		active: make(map[string]*refcount),
	}
	return d
}

var _ node = (*dir)(nil)
var _ fs.Node = (*dir)(nil)
var _ fs.NodeCreater = (*dir)(nil)
var _ fs.NodeForgetter = (*dir)(nil)
var _ fs.NodeMkdirer = (*dir)(nil)
var _ fs.NodeRemover = (*dir)(nil)
var _ fs.NodeRenamer = (*dir)(nil)
var _ fs.NodeStringLookuper = (*dir)(nil)
var _ fs.HandleReadDirAller = (*dir)(nil)

func (d *dir) setName(name string) {

	d.mu.Lock()
	d.name = name
	d.mu.Unlock()

}

func (d *dir) setParentInode(pdir *dir) {

	d.mu.Lock()
	defer d.mu.Unlock()
	d.parent = pdir
}

// Attr ...
func (d *dir) Attr(ctx context.Context, a *fuse.Attr) error {

	logger.Debug("Dir Attr")

	a.Mode = os.ModeDir | 0755
	a.Inode = d.inode
	a.Valid = time.Minute
	/*
		if d.parent == nil {
			a.Mode = os.ModeDir | 0755
			a.Inode = d.inode
		} else {
			ret, inode, inodeInfo := d.fs.cfs.GetInodeInfoDirect(d.parent.inode, d.name)
			if ret != 0 {
				return nil
			}

			a.Ctime = time.Unix(inodeInfo.ModifiTime, 0)
			a.Mtime = time.Unix(inodeInfo.ModifiTime, 0)
			a.Atime = time.Unix(inodeInfo.AccessTime, 0)
			a.Inode = uint64(inode)
		}
	*/
	return nil
}

func (d *dir) Lookup(ctx context.Context, name string) (fs.Node, error) {

	d.mu.Lock()
	defer d.mu.Unlock()

	logger.Debug("Dir Lookup")

	if a, ok := d.active[name]; ok {
		return a.node, nil
	}

	ret, inodeType, inode := d.fs.cfs.StatDirect(d.inode, name)

	if ret == 2 {
		return nil, fuse.ENOENT
	}
	if ret != 0 {
		return nil, fuse.ENOENT
	}
	n, _ := d.reviveNode(inodeType, inode, name)

	a := &refcount{node: n}
	d.active[name] = a

	a.kernel = true

	return a.node, nil
}

func (d *dir) reviveDir(inode uint64, name string) (*dir, error) {
	child := newDir(d.fs, inode, d, name)
	return child, nil
}

func (d *dir) reviveNode(inodeType bool, inode uint64, name string) (node, error) {
	if inodeType {
		child := &File{
			inode:  inode,
			name:   name,
			parent: d,
		}
		return child, nil
	}
	child, _ := d.reviveDir(inode, name)
	return child, nil

}

// ReadDirAll ...
func (d *dir) ReadDirAll(ctx context.Context) ([]fuse.Dirent, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	logger.Debug("Dir ReadDirAll")

	var res []fuse.Dirent
	ret, dirents := d.fs.cfs.ListDirect(d.inode)

	if ret == 2 {
		return nil, fuse.Errno(syscall.ENOENT)
	}
	if ret != 0 {
		return nil, fuse.Errno(syscall.EIO)
	}
	for _, v := range dirents {
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

// Create ...
func (d *dir) Create(ctx context.Context, req *fuse.CreateRequest, resp *fuse.CreateResponse) (fs.Node, fs.Handle, error) {
	logger.Debug("Create file start ,  name %v parentino %v parentname %v", req.Name, d.inode, d.name)

	d.mu.Lock()
	defer d.mu.Unlock()

	/*
		if a, ok := d.active[req.Name]; ok {
			logger.Info("asked to create with existing node: %q %#v", req.Name, a.node)
			return a.node, a.node, nil
		}
	*/

	//logger.Debug("Create file get locker ,  name %v parentino %v parentname %v", req.Name, d.inode, d.name)

	ret, cfile := d.fs.cfs.CreateFileDirect(d.inode, req.Name, int(req.Flags))
	if ret != 0 {
		if ret == 17 {
			return nil, nil, fuse.Errno(syscall.EEXIST)

		}
		return nil, nil, fuse.Errno(syscall.EIO)

	}

	child := &File{
		inode:   cfile.Inode,
		name:    req.Name,
		parent:  d,
		handles: 1,
		writers: 1,
		cfile:   cfile,
	}

	d.active[req.Name] = &refcount{node: child}

	logger.Debug("Create file end , inode %v name %v parentino %v parentname %v", cfile.Inode, req.Name, d.inode, d.name)

	return child, child, nil
}

func (d *dir) forgetChild(name string, child node) {
	if name == "" {
		return
	}

	d.mu.Lock()
	defer d.mu.Unlock()

	a, ok := d.active[name]
	if !ok {
		return
	}

	a.kernel = false
	if a.refs == 0 {
		delete(d.active, name)
	}
}

func (d *dir) Forget() {

	if d.parent == nil {
		return
	}

	d.mu.Lock()
	name := d.name
	d.mu.Unlock()

	d.parent.forgetChild(name, d)
}

// Mkdir ...
func (d *dir) Mkdir(ctx context.Context, req *fuse.MkdirRequest) (fs.Node, error) {

	logger.Debug("Mkdir start ,  name %v parentino %v parentname %v", req.Name, d.inode, d.name)

	d.mu.Lock()
	defer d.mu.Unlock()

	//logger.Debug("Mkdir get locker ,  name %v parentino %v parentname %v", req.Name, d.inode, d.name)

	ret, inode := d.fs.cfs.CreateDirDirect(d.inode, req.Name)
	if ret == -1 {
		return nil, fuse.Errno(syscall.EIO)
	}
	if ret == 1 {
		return nil, fuse.Errno(syscall.EPERM)
	}
	if ret == 2 {
		return nil, fuse.Errno(syscall.ENOENT)
	}
	if ret == 17 {
		return nil, fuse.Errno(syscall.EEXIST)
	}

	logger.Debug("Mkdir end , inode %v name %v parentino %v parentname %v", inode, req.Name, d.inode, d.name)

	child := newDir(d.fs, inode, d, req.Name)

	d.active[req.Name] = &refcount{node: child, kernel: true}

	return child, nil
}

// Remove ...
func (d *dir) Remove(ctx context.Context, req *fuse.RemoveRequest) error {

	logger.Debug("Remove start , name %v parentino %v parentname %v", req.Name, d.inode, d.name)

	d.mu.Lock()
	defer d.mu.Unlock()

	//logger.Debug("Remove get locker , name %v parentino %v parentname %v", req.Name, d.inode, d.name)

	if req.Dir {
		ret := d.fs.cfs.DeleteDirDirect(d.inode, req.Name)
		if ret != 0 {
			if ret == 2 {
				return fuse.Errno(syscall.EPERM)
			}
			return fuse.Errno(syscall.EIO)

		}
	} else {

		ret := d.fs.cfs.DeleteFileDirect(d.inode, req.Name)
		if ret != 0 {
			if ret == 2 {
				return fuse.Errno(syscall.EPERM)
			}
			return fuse.Errno(syscall.EIO)
		}
	}

	if a, ok := d.active[req.Name]; ok {
		delete(d.active, req.Name)
		a.node.setName("")
	}
	logger.Debug("Remove end , name %v parentino %v parentname %v", req.Name, d.inode, d.name)

	return nil
}

// Rename ...
func (d *dir) Rename(ctx context.Context, req *fuse.RenameRequest, newDir fs.Node) error {

	logger.Debug("Rename start d.inode %v, req.OldName %v, newDir.(*dir).inode %v , req.NewName %v", d.inode, req.OldName, newDir.(*dir).inode, req.NewName)

	d.mu.Lock()
	defer d.mu.Unlock()

	ret, _, _ := d.fs.cfs.StatDirect(newDir.(*dir).inode, req.NewName)
	if ret == 0 {
		logger.Error("Rename Failed , newName in newDir is already exsit")
		return fuse.Errno(syscall.EPERM)
	}

	if newDir != d {

		logger.Debug("Rename d.inode %v, req.OldName %v, newDir.(*dir).inode %v , req.NewName %v", d.inode, req.OldName, newDir.(*dir).inode, req.NewName)

		ret := d.fs.cfs.RenameDirect(d.inode, req.OldName, newDir.(*dir).inode, req.NewName)
		if ret != 0 {
			if ret == 2 {
				return fuse.Errno(syscall.ENOENT)
			} else if ret == 1 || ret == 17 {
				return fuse.Errno(syscall.EPERM)
			} else {
				return fuse.Errno(syscall.EIO)
			}
		}

		if aOld, ok := d.active[req.OldName]; ok {
			delete(d.active, req.OldName)
			aOld.node.setName(req.NewName)
			aOld.node.setParentInode(newDir.(*dir))
			//d.active[req.NewName] = aOld

		}

	} else {

		logger.Debug("Rename d.inode %v, req.OldName %v, newDir.(*dir).inode %v , req.NewName %v", d.inode, req.OldName, newDir.(*dir).inode, req.NewName)

		ret := d.fs.cfs.RenameDirect(d.inode, req.OldName, d.inode, req.NewName)
		if ret != 0 {
			if ret == 2 {
				return fuse.Errno(syscall.ENOENT)
			} else if ret == 1 || ret == 17 {
				return fuse.Errno(syscall.EPERM)
			} else {
				return fuse.Errno(syscall.EIO)
			}
		}

		if a, ok := d.active[req.NewName]; ok {
			a.node.setName("")
		}

		if aOld, ok := d.active[req.OldName]; ok {
			aOld.node.setName(req.NewName)
			delete(d.active, req.OldName)
			d.active[req.NewName] = aOld
		}
	}

	logger.Debug("Rename end d.inode %v, req.OldName %v, newDir.(*dir).inode %v , req.NewName %v", d.inode, req.OldName, newDir.(*dir).inode, req.NewName)

	return nil
}

type node interface {
	fs.Node
	setName(name string)
	setParentInode(pdir *dir)
}

// File struct
type File struct {
	mu    sync.Mutex
	inode uint64

	parent  *dir
	name    string
	writers uint
	handles uint32
	cfile   *cfs.CFile
}

var _ node = (*File)(nil)
var _ = fs.Node(&File{})
var _ = fs.Handle(&File{})

func (f *File) setName(name string) {

	f.mu.Lock()
	f.name = name
	f.mu.Unlock()

}

func (f *File) setParentInode(pdir *dir) {

	f.mu.Lock()
	f.parent = pdir
	f.mu.Unlock()
}

// Attr ...
func (f *File) Attr(ctx context.Context, a *fuse.Attr) error {

	logger.Debug("File Attr")

	f.mu.Lock()
	defer f.mu.Unlock()

	ret, inode, inodeInfo := f.parent.fs.cfs.GetInodeInfoDirect(f.parent.inode, f.name)
	if ret != 0 || inodeInfo == nil {
		return nil
	}

	a.Ctime = time.Unix(inodeInfo.ModifiTime, 0)
	a.Mtime = time.Unix(inodeInfo.ModifiTime, 0)
	a.Atime = time.Unix(inodeInfo.AccessTime, 0)
	a.Size = uint64(inodeInfo.FileSize)
	a.Inode = uint64(inode)

	a.BlockSize = 4 * 1024
	a.Blocks = uint64(math.Ceil(float64(a.Size) / float64(a.BlockSize)))
	a.Mode = 0666
	a.Valid = time.Minute

	return nil
}

var _ = fs.NodeOpener(&File{})

// Open ...
func (f *File) Open(ctx context.Context, req *fuse.OpenRequest, resp *fuse.OpenResponse) (fs.Handle, error) {
	var ret int32

	logger.Debug("Open start : name %v inode %v Flags %v pinode %v pname %v", f.name, f.inode, req.Flags, f.parent.inode, f.parent.name)

	f.mu.Lock()
	defer f.mu.Unlock()

	//logger.Debug("Open get locker : name %v inode %v Flags %v pinode %v pname %v", f.name, f.inode, req.Flags, f.parent.inode, f.parent.name)

	if int(req.Flags)&os.O_TRUNC != 0 {
		return nil, fuse.Errno(syscall.EPERM)
	}

	if f.writers > 0 {
		if int(req.Flags)&os.O_WRONLY != 0 || int(req.Flags)&os.O_RDWR != 0 {
			logger.Error("Open failed writers > 0")
			return nil, fuse.Errno(syscall.EPERM)
		}
	}

	if f.cfile == nil && f.handles == 0 {
		ret, f.cfile = f.parent.fs.cfs.OpenFileDirect(f.parent.inode, f.name, int(req.Flags))
		if ret != 0 {
			logger.Error("Open failed OpenFileDirect ret %v", ret)
			return nil, fuse.Errno(syscall.EIO)
		}
	} else {
		f.parent.fs.cfs.UpdateOpenFileDirect(f.parent.inode, f.name, f.cfile, int(req.Flags))
	}

	tmp := f.handles + 1
	f.handles = tmp

	if int(req.Flags)&os.O_WRONLY != 0 || int(req.Flags)&os.O_RDWR != 0 {
		tmp := f.writers + 1
		f.writers = tmp
	}

	logger.Debug("Open end : name %v inode %v Flags %v pinode %v pname %v", f.name, f.inode, req.Flags, f.parent.inode, f.parent.name)

	resp.Flags = fuse.OpenDirectIO
	return f, nil
}

var _ = fs.HandleReleaser(&File{})

// Release ...
func (f *File) Release(ctx context.Context, req *fuse.ReleaseRequest) error {

	logger.Debug("Release start : name %v pinode %v pname %v", f.name, f.parent.inode, f.parent.name)

	f.mu.Lock()
	defer f.mu.Unlock()

	f.handles--

	if int(req.Flags)&os.O_WRONLY != 0 || int(req.Flags)&os.O_RDWR != 0 {
		f.writers--
	}

	if f.handles == 0 {
		f.cfile = nil
	}

	logger.Debug("Release end : name %v pinode %v pname %v", f.name, f.parent.inode, f.parent.name)

	return nil
}

var _ = fs.HandleReader(&File{})

// Read ...
func (f *File) Read(ctx context.Context, req *fuse.ReadRequest, resp *fuse.ReadResponse) error {

	f.mu.Lock()
	defer f.mu.Unlock()
	if _, ok := f.cfile.ReaderMap[req.Handle]; !ok {
		rdinfo := cfs.ReaderInfo{}
		rdinfo.LastOffset = int64(0)
		f.cfile.ReaderMap[req.Handle] = &rdinfo
	}
	if req.Offset == f.cfile.FileSize {

		logger.Debug("Request Read file offset equal filesize")
		return nil
	}

	length := f.cfile.Read(req.Handle, &resp.Data, req.Offset, int64(req.Size))
	if length != int64(req.Size) {
		logger.Debug("== Read reqsize:%v, but return datasize:%v ==\n", req.Size, length)
	}
	if length < 0 {
		logger.Error("Request Read file I/O Error(return data from cfs less than zero)")
		return fuse.Errno(syscall.EIO)
	}
	return nil
}

var _ = fs.HandleWriter(&File{})

// Write ...
func (f *File) Write(ctx context.Context, req *fuse.WriteRequest, resp *fuse.WriteResponse) error {

	f.mu.Lock()
	defer f.mu.Unlock()

	w := f.cfile.Write(req.Data, int32(len(req.Data)))
	if w != int32(len(req.Data)) {
		if w == -1 {
			logger.Error("Write Failed Err:ENOSPC")
			return fuse.Errno(syscall.ENOSPC)
		}
		logger.Error("Write Failed Err:EIO")
		return fuse.Errno(syscall.EIO)

	}
	resp.Size = int(w)

	return nil
}

var _ = fs.HandleFlusher(&File{})

// Flush ...
func (f *File) Flush(ctx context.Context, req *fuse.FlushRequest) error {

	//logger.Debug("Flush start : name %v ,inode %v, pinode %v pname %v", f.name, f.inode, f.parent.inode, f.parent.name)

	f.mu.Lock()
	defer f.mu.Unlock()

	if ret := f.cfile.Flush(); ret != 0 {
		logger.Error("Flush Flush err ...")
		return fuse.Errno(syscall.EIO)
	}

	return nil
}

var _ fs.NodeFsyncer = (*File)(nil)

// Fsync ...
func (f *File) Fsync(ctx context.Context, req *fuse.FsyncRequest) error {

	logger.Debug("Fsync start : name %v ,inode %v, pinode %v pname %v", f.name, f.inode, f.parent.inode, f.parent.name)

	f.mu.Lock()
	defer f.mu.Unlock()

	if ret := f.cfile.Flush(); ret != 0 {
		logger.Error("Fsync Flush err ...")
		return fuse.Errno(syscall.EIO)
	}

	logger.Debug("Fsync end : name %v ,inode %v, pinode %v pname %v", f.name, f.inode, f.parent.inode, f.parent.name)

	return nil
}

var _ = fs.NodeSetattrer(&File{})

// Setattr ...
func (f *File) Setattr(ctx context.Context, req *fuse.SetattrRequest, resp *fuse.SetattrResponse) error {
	return nil
}

func main() {

	addr2 := flag.String("metanode", "127.0.0.1:9903,127.0.0.1:9913,127.0.0.1:9923", "ContainerFS metanode hosts")
	buffertype := flag.Int("buffertype", 0, "ContainerFS per file buffertype : 0 512KB 1 256KB 2 128KB")
	uuid := flag.String("uuid", "xxx", "ContainerFS Volume UUID")
	mountPoint := flag.String("mountpoint", "/mnt", "ContainerFS MountPoint")
	log := flag.String("log", "/export/Logs/containerfs/logs/", "ContainerFS log level")
	loglevel := flag.String("loglevel", "error", "ContainerFS log level")
	isReadOnly := flag.Int("readonly", 0, "Is readonly Volume 1 for ture ,0 for false")

	flag.Parse()

	cfs.MetaNodePeers = strings.Split(*addr2, ",")

	switch *buffertype {
	case 0:
		cfs.BufferSize = 512 * 1024
	case 1:
		cfs.BufferSize = 256 * 1024
	case 2:
		cfs.BufferSize = 128 * 1024
	default:
		cfs.BufferSize = 512 * 1024
	}

	logger.SetConsole(true)
	logger.SetRollingFile(*log, "fuse.log", 10, 100, logger.MB) //each 100M rolling

	switch *loglevel {

	case "error":
		logger.SetLevel(logger.ERROR)
	case "debug":
		logger.SetLevel(logger.DEBUG)
	case "info":
		logger.SetLevel(logger.INFO)
	default:
		logger.SetLevel(logger.ERROR)
	}

	defer func() {
		if err := recover(); err != nil {
			logger.Error("panic !!! :%v", err)
			logger.Error("stacks:%v", string(debug.Stack()))
		}
	}()

	cfs.MetaNodeAddr, _ = cfs.GetLeader(*uuid)
	fmt.Printf("Leader:%v\n", cfs.MetaNodeAddr)
	ticker := time.NewTicker(time.Second * 60)
	go func() {
		for range ticker.C {
			cfs.MetaNodeAddr, _ = cfs.GetLeader(*uuid)
			fmt.Printf("Leader:%v\n", cfs.MetaNodeAddr)
		}
	}()

	//allocate volume blkgrp
	tic := time.NewTicker(30 * time.Second)
	go func() {
		for range tic.C {
			ok, ret := cfs.GetFSInfo(*uuid)
			if ok != 0 {
				logger.Error("ExpandVol once volume:%v failed, GetFSInfo error", *uuid)
				continue
			}
			if float64(ret.FreeSpace)/float64(ret.TotalSpace) > 0.1 {
				continue
			}

			logger.Debug("Need ExpandVol once volume:%v -- totalsize:%v -- freesize:%v", *uuid, ret.TotalSpace, ret.FreeSpace)
			ok = cfs.ExpandVolRS(*uuid, *mountPoint)
			if ok == -1 {
				logger.Error("Expand volume: %v one time error", *uuid)
			} else if ok == -2 {
				logger.Error("Expand volume: %v by another client, so this client not need expand", *uuid)
			} else if ok == 1 {
				logger.Debug("Expand volume: %v one time sucess", *uuid)
			}
		}
	}()

	err := mount(*uuid, *mountPoint, *isReadOnly)
	if err != nil {
		fmt.Println("mount failed ...", err)
	}
}

func closeConns(fs *cfs.CFS) {

	if fs.Conn != nil {
		fs.Conn.Close()
	}
	for _, v := range fs.DataConn {
		if v != nil {
			v.Close()
		}
	}

}

func mount(uuid, mountPoint string, isReadOnly int) error {

	cfs := cfs.OpenFileSystem(uuid)
	if cfs == nil {
		return fuse.Errno(syscall.EIO)
	}

	defer closeConns(cfs)

	if isReadOnly == 0 {
		c, err := fuse.Mount(
			mountPoint,
			fuse.AllowOther(),
			fuse.MaxReadahead(128*1024),
			fuse.AsyncRead(),
			fuse.WritebackCache(),
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

	c, err := fuse.Mount(
		mountPoint,
		fuse.ReadOnly(),
		fuse.AllowOther(),
		fuse.MaxReadahead(128*1024),
		fuse.AsyncRead(),
		fuse.WritebackCache(),
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
