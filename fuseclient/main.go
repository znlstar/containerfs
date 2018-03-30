package main

import (
	"flag"
	"fmt"
	"io"
	"math"
	"net/http"
	_ "net/http/pprof"
	"os"
	"strings"
	"sync"
	"syscall"
	"time"

	"bazil.org/fuse"
	"bazil.org/fuse/fs"
	"github.com/tiglabs/containerfs/fs"
	"github.com/tiglabs/containerfs/logger"
	"github.com/tiglabs/containerfs/utils"
	"golang.org/x/net/context"
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
	err, ret := fs.cfs.GetFSInfo()
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
var _ fs.NodeFsyncer = (*dir)(nil)
var _ fs.NodeSymlinker = (*dir)(nil)
var _ fs.NodeRequestLookuper = (*dir)(nil)
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
	a.Valid = utils.FUSE_ATTR_CACHE_LIFE
	if d.parent != nil {
		logger.Debug("d p inode:%v", d.parent.inode)
		ret, inode, inodeInfo := d.fs.cfs.GetInodeInfoDirect(d.parent.inode, d.name)
		if ret == 0 {
		} else if ret == utils.ENOTFOUND {
			d.mu.Lock()
			//clean dirty cache in dir map
			delete(d.parent.active, d.name)
			d.mu.Unlock()
			return fuse.Errno(syscall.ENOENT)
		} else {
			return fuse.Errno(syscall.EIO)
		}
		a.Ctime = time.Unix(inodeInfo.ModifiTime, 0)
		a.Mtime = time.Unix(inodeInfo.ModifiTime, 0)
		a.Atime = time.Unix(inodeInfo.AccessTime, 0)
		a.Inode = uint64(inode)
	}
	return nil
}

func (d *dir) Lookup(ctx context.Context, req *fuse.LookupRequest, resp *fuse.LookupResponse) (fs.Node, error) {

	d.mu.Lock()
	defer d.mu.Unlock()

	logger.Debug("Dir Lookup %v in dir %v", req.Name, d.name)
	resp.EntryValid = utils.FUSE_LOOKUP_CACHE_LIFE
	if a, ok := d.active[req.Name]; ok {
		return a.node, nil
	}

	ret, inodeType, inode := d.fs.cfs.StatDirect(d.inode, req.Name)
	if ret != 0 {
		return nil, fuse.ENOENT
	}
	n, _ := d.reviveNode(inodeType, inode, req.Name)

	a := &refcount{node: n}
	d.active[req.Name] = a

	a.kernel = true

	return a.node, nil
}

func (d *dir) reviveDir(inode uint64, name string) (*dir, error) {
	child := newDir(d.fs, inode, d, name)
	return child, nil
}

func (d *dir) reviveNode(inodeType uint32, inode uint64, name string) (node, error) {
	if inodeType == utils.INODE_DIR {
		child, _ := d.reviveDir(inode, name)
		return child, nil
	} else {
		child := &File{
			inode:    inode,
			name:     name,
			parent:   d,
			fileType: inodeType,
		}
		return child, nil

	}
	return nil, nil

}

// ReadDirAll ...
func (d *dir) ReadDirAll(ctx context.Context) ([]fuse.Dirent, error) {

	logger.Debug("Dir ReadDirAll")

	d.mu.Lock()
	defer d.mu.Unlock()

	var res []fuse.Dirent
	var ginode uint64

	if d.parent != nil {
		ginode = d.parent.inode
	} else {
		ginode = 0
	}
	ret, dirents := d.fs.cfs.ListDirect(d.inode, ginode, d.name)

	if ret == 0 {

	} else if ret == utils.ENOTFOUND {
		if d.parent != nil {
			//clean dirty cache in dir map
			delete(d.parent.active, d.name)
		}
		return nil, fuse.Errno(syscall.EPERM)
	} else if ret == 2 || ret == utils.ENOENT {
		return nil, fuse.Errno(syscall.ENOENT)
	} else {
		return nil, fuse.Errno(syscall.EIO)
	}

	for _, v := range dirents {
		de := fuse.Dirent{
			Name: v.Name,
		}
		if v.InodeType == utils.INODE_DIR {
			de.Type = fuse.DT_Dir
		} else if v.InodeType == utils.INODE_FILE {
			de.Type = fuse.DT_File
		} else if v.InodeType == utils.INODE_SYMLINK {
			de.Type = fuse.DT_Link
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

	logger.Debug("Create file get locker ,  name %v parentino %v parentname %v", req.Name, d.inode, d.name)

	ret, cfile := d.fs.cfs.CreateFileDirect(d.inode, req.Name, int(req.Flags))
	if ret != 0 {
		if ret == 17 {
			return nil, nil, fuse.Errno(syscall.EEXIST)

		}
		return nil, nil, fuse.Errno(syscall.EIO)

	}

	child := &File{
		inode:    cfile.Inode,
		name:     req.Name,
		parent:   d,
		handles:  1,
		writers:  1,
		cfile:    cfile,
		fileType: utils.INODE_FILE,
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
	logger.Debug("Forget dir %v inode %v", d.name, d.inode)
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

	if a, ok := d.active[req.Name]; ok {
		delete(d.active, req.Name)
		a.node.setName("")
	}

	if req.Dir {
		ret := d.fs.cfs.DeleteDirDirect(d.inode, req.Name)
		if ret != 0 {
			if ret == 2 {
				return fuse.Errno(syscall.EPERM)
			}
			return fuse.Errno(syscall.EIO)
		}
	} else {

		var symlimkFlag bool

		if node, ok := d.active[req.Name]; ok {
			if node.node.(*File).fileType == utils.INODE_SYMLINK {
				logger.Debug("symlink file in active ...")
				symlimkFlag = true
			}
		} else {
			ret, inodeType, _ := d.fs.cfs.StatDirect(d.inode, req.Name)
			if ret != 0 {
				return fuse.Errno(syscall.EIO)
			} else {
				if inodeType == utils.INODE_SYMLINK {
					symlimkFlag = true
				}
			}

		}

		if symlimkFlag {

			ret := d.fs.cfs.DeleteSymLinkDirect(d.inode, req.Name)

			logger.Debug("symlink DeleteSymLinkDirect ret  %v", ret)

			if ret != 0 {
				if ret == utils.ENOTFOUND {
					return nil
				} else if ret == 2 {
					return fuse.Errno(syscall.EPERM)
				} else {
					return fuse.Errno(syscall.EIO)
				}
			}

		} else {

			ret := d.fs.cfs.DeleteFileDirect(d.inode, req.Name)
			if ret != 0 {
				if ret == utils.ENOTFOUND {
					return nil
				} else if ret == 2 {
					return fuse.Errno(syscall.EPERM)
				} else {
					return fuse.Errno(syscall.EIO)
				}
			}

		}

	}

	logger.Debug("Remove end , name %v parentino %v parentname %v", req.Name, d.inode, d.name)

	return nil
}

// Rename ...
func (d *dir) Rename(ctx context.Context, req *fuse.RenameRequest, newDir fs.Node) error {

	logger.Debug("Rename start d.inode %v, req.OldName %v, newDir.(*dir).inode %v , req.NewName %v", d.inode, req.OldName, newDir.(*dir).inode, req.NewName)

	d.mu.Lock()
	defer d.mu.Unlock()

	ret, inodeType, _ := d.fs.cfs.StatDirect(newDir.(*dir).inode, req.NewName)
	if ret == 0 {
		logger.Debug("newName in newDir is already exsit, inodeType: %v", inodeType)
		if utils.INODE_DIR == inodeType {
			logger.Error("Rename newName %v in newDir %v is an exsit dir, un-supportted rename", req.NewName, d.name)
			return fuse.Errno(syscall.EPERM)
		} else if utils.INODE_FILE == inodeType {
			ret = d.fs.cfs.DeleteFileDirect(newDir.(*dir).inode, req.NewName)
			if ret != 0 {
				logger.Error("Rename Delete the exist newName %v in newDir %v failed!", req.NewName, d.name)
				return fuse.Errno(syscall.EPERM)
			}
		} else if utils.INODE_SYMLINK == inodeType {
			ret = d.fs.cfs.DeleteSymLinkDirect(newDir.(*dir).inode, req.NewName)
			if ret != 0 {
				logger.Error("Rename Delete the exist newName %v in newDir %v failed!", req.NewName, d.name)
				return fuse.Errno(syscall.EPERM)
			}
		}

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
			newDir.(*dir).active[req.NewName] = aOld
		}

	} else {

		logger.Debug("Rename d.inode %v, req.OldName %v, newDir.(*dir).inode %v , req.NewName %v", d.inode, req.OldName, newDir.(*dir).inode, req.NewName)

		ret := d.fs.cfs.RenameDirect(d.inode, req.OldName, d.inode, req.NewName)
		if ret != 0 {
			if ret == utils.ENOTFOUND {
				delete(d.active, req.OldName)
				return fuse.Errno(syscall.ENOENT)
			} else if ret == 2 {
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

// Symlink ...
func (d *dir) Symlink(ctx context.Context, req *fuse.SymlinkRequest) (fs.Node, error) {

	logger.Debug("Symlink req %v", req)

	ret, inode := d.fs.cfs.SymLink(d.inode, req.NewName, req.Target)
	if ret != 0 {
		logger.Error("Symlink ret %v,pinode %v,newname %v,target %v", ret, d.inode, req.NewName, req.Target)
		return nil, fuse.EPERM
	}

	child := &File{
		inode:    inode,
		name:     req.NewName,
		parent:   d,
		handles:  1,
		writers:  1,
		fileType: utils.INODE_SYMLINK,
	}

	d.active[req.NewName] = &refcount{node: child}

	return child, nil
}

// Fsync ...
func (d *dir) Fsync(ctx context.Context, req *fuse.FsyncRequest) error {

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

	fileType uint32
	parent   *dir
	name     string
	writers  uint
	handles  uint32
	cfile    *cfs.CFile
}

var _ node = (*File)(nil)
var _ = fs.Node(&File{})
var _ = fs.Handle(&File{})
var _ = fs.NodeForgetter(&File{})

func (f *File) Forget() {
	logger.Debug("Forget file %v inode %v", f.name, f.inode)
	if f.parent == nil {
		return
	}

	f.mu.Lock()
	name := f.name
	f.mu.Unlock()

	f.parent.forgetChild(name, f)
}

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
	logger.Debug("to get attr for %v in type: %v, parent inode: %v", f.name, f.fileType, f.parent.inode)
	if f.fileType == utils.INODE_FILE {

		ret, inode, inodeInfo := f.parent.fs.cfs.GetInodeInfoDirect(f.parent.inode, f.name)
		logger.Debug("to get attr from ms %v in type: %v, ret: %v", f.name, f.fileType, ret)
		if ret == 0 {
		} else if ret == utils.ENOTFOUND {
			delete(f.parent.active, f.name)
			return fuse.Errno(syscall.ENOENT)
		} else if ret != 0 || inodeInfo == nil {
			return nil
		}
		a.Valid = utils.FUSE_ATTR_CACHE_LIFE
		a.Ctime = time.Unix(inodeInfo.ModifiTime, 0)
		a.Mtime = time.Unix(inodeInfo.ModifiTime, 0)
		a.Atime = time.Unix(inodeInfo.AccessTime, 0)
		a.Size = uint64(inodeInfo.FileSize)
		if f.cfile != nil && a.Size < uint64(f.cfile.FileSizeInCache) {
			a.Size = uint64(f.cfile.FileSizeInCache)
		}
		a.Inode = uint64(inode)

		a.BlockSize = 4 * 1024
		a.Blocks = uint64(math.Ceil(float64(a.Size) / float64(a.BlockSize)))
		a.Mode = 0666
		a.Valid = time.Second

	} else if f.fileType == utils.INODE_SYMLINK {

		logger.Debug("att symlink file pinode %v name %v", f.parent.inode, f.name)

		ret, inode := f.parent.fs.cfs.GetSymLinkInfoDirect(f.parent.inode, f.name)
		if ret == 0 {
		} else if ret == utils.ENOTFOUND {
			delete(f.parent.active, f.name)
			return fuse.Errno(syscall.ENOENT)
		} else if ret != 0 {
			return nil
		}

		logger.Error("GetSymLinkInfoDirect inode %v", inode)

		a.Inode = uint64(inode)
		a.Mode = 0666 | os.ModeSymlink
		a.Valid = 0
	}

	return nil
}

var _ = fs.NodeOpener(&File{})

// Open ...
func (f *File) Open(ctx context.Context, req *fuse.OpenRequest, resp *fuse.OpenResponse) (fs.Handle, error) {

	logger.Debug("Open start : name %v inode %v Flags %v pinode %v pname %v", f.name, f.inode, req.Flags, f.parent.inode, f.parent.name)

	if int(req.Flags)&os.O_TRUNC != 0 {
		return nil, fuse.Errno(syscall.EPERM)
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	//logger.Debug("Open get locker : name %v inode %v Flags %v pinode %v pname %v", f.name, f.inode, req.Flags, f.parent.inode, f.parent.name)

	if f.writers > 0 {
		if int(req.Flags)&os.O_WRONLY != 0 || int(req.Flags)&os.O_RDWR != 0 {
			logger.Error("Open failed writers > 0")
			return nil, fuse.Errno(syscall.EPERM)
		}
	}

	var ret int32

	if f.cfile == nil && f.handles == 0 {
		ret, f.cfile = f.parent.fs.cfs.OpenFileDirect(f.parent.inode, f.name, int(req.Flags))

		if ret == 0 {
		} else if ret == utils.ENOTFOUND {
			//clean dirty cache in dir map
			delete(f.parent.active, f.name)

			if int(req.Flags) != os.O_RDONLY && (int(req.Flags)&os.O_CREATE > 0 || int(req.Flags)&^(os.O_WRONLY|os.O_TRUNC) == 0) {
				logger.Debug("open an deleted file, create new file %v with flag: %v", f.name, req.Flags)
				ret, f.cfile = f.parent.fs.cfs.CreateFileDirect(f.parent.inode, f.name, int(req.Flags))
				if ret != 0 {
					if ret == 17 {
						return nil, fuse.Errno(syscall.EEXIST)
					}
					return nil, fuse.Errno(syscall.EIO)
				}

				f.inode = f.cfile.Inode
				f.handles = 0
				f.writers = 0
				f.parent.active[f.name] = &refcount{node: f}
			} else {
				return nil, fuse.Errno(syscall.ENOENT)
			}
		} else {
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

	var err error
	if int(req.Flags)&os.O_WRONLY != 0 || int(req.Flags)&os.O_RDWR != 0 {
		f.writers--
		if ret := f.cfile.CloseWrite(); ret != 0 {
			logger.Error("Release CloseWrite err ...")
			err = fuse.Errno(syscall.EIO)
		}
	}
	f.handles--
	if f.handles == 0 {
		f.cfile.Close()
		f.cfile = nil
	}
	logger.Debug("Release end : name %v pinode %v pname %v", f.name, f.parent.inode, f.parent.name)

	return err
}

var _ = fs.HandleReader(&File{})

// Read ...
func (f *File) Read(ctx context.Context, req *fuse.ReadRequest, resp *fuse.ReadResponse) error {

	f.mu.Lock()
	defer f.mu.Unlock()

	if req.Offset == f.cfile.FileSizeInCache {

		logger.Debug("Request Read file offset equal filesize")
		return nil
	}

	length := f.cfile.Read(&resp.Data, req.Offset, int64(req.Size))
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

	w := f.cfile.Write(req.Data, req.Offset, int32(len(req.Data)))
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

var _ = fs.NodeReadlinker(&File{})

// ReadLink ...
func (f *File) Readlink(ctx context.Context, req *fuse.ReadlinkRequest) (string, error) {

	logger.Debug("ReadLink ...")

	ret, target := f.parent.fs.cfs.ReadLink(f.inode)
	if ret != 0 {
		logger.Error("ReadLink req ret %v", ret)
		return "", fuse.EIO
	}

	return target, nil

}

func logleveldebug(w http.ResponseWriter, req *http.Request) {
	logger.SetLevel(logger.DEBUG)
	io.WriteString(w, "ok!\n")
}

func loglevelerror(w http.ResponseWriter, req *http.Request) {
	logger.SetLevel(logger.ERROR)
	io.WriteString(w, "ok!\n")
}

func main() {

	var peers string

	flag.StringVar(&peers, "volmgr", "10.8.64.216,10.8.64.217,10.8.64.218", "ContainerFS VolMgr Host")

	buffertype := flag.Int("buffertype", 0, "ContainerFS per file buffertype : 0 512KB 1 256KB 2 128KB")
	uuid := flag.String("uuid", "xxx", "ContainerFS Volume UUID")
	mountPoint := flag.String("mountpoint", "/mnt", "ContainerFS MountPoint")
	log := flag.String("log", "/export/Logs/containerfs/logs/", "ContainerFS log level")
	loglevel := flag.String("loglevel", "error", "ContainerFS log level")
	isReadOnly := flag.Int("readonly", 0, "Is readonly Volume 1 for ture ,0 for false")
	writeBuff := flag.Int("writebuffer", 0, "Write buffer size in mb, must be no larger than 3")
	flag.Parse()
	if len(os.Args) >= 2 && (os.Args[1] == "version") {
		fmt.Println(utils.Version())
		os.Exit(0)
	}

	if *writeBuff < 0 || *writeBuff > 3 {
		fmt.Println("bad writeBuff, must be no larger than 3")
		os.Exit(0)
	}
	tmp := strings.Split(peers, ",")
	if len(tmp) != 3 {
		fmt.Println("bad peers, must be 3 peer")
		os.Exit(0)
	}
	cfs.VolMgrHosts = make([]string, 3)
	cfs.VolMgrHosts[0] = tmp[0] + ":7703"
	cfs.VolMgrHosts[1] = tmp[1] + ":7713"
	cfs.VolMgrHosts[2] = tmp[2] + ":7723"

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

	cfs.WriteBufferSize = *writeBuff * 1024 * 1024

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

	/*
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
	*/

	http.HandleFunc("/logleveldebug", logleveldebug)
	http.HandleFunc("/loglevelerror", loglevelerror)
	go func() {
		http.ListenAndServe(":10000", nil)
	}()

	err := mount(*uuid, *mountPoint, *isReadOnly)
	if err != nil {
		fmt.Println("mount failed ...", err)
	}
}

func closeConns(c *cfs.CFS) {

	if c.MetaNodeConn != nil {
		c.MetaNodeConn.Close()
	}

	if c.VolMgrConn != nil {
		c.VolMgrConn.Close()
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
