package cfs

import (
	"encoding/json"
	"fmt"
	"golang.org/x/net/context"
	"google.golang.org/grpc"

	//dp "ipd.org/containerfs/datanode/protobuf"
	mp "ipd.org/containerfs/metanode/protobuf"
	vp "ipd.org/containerfs/volmgr/protobuf"
	"strconv"
)

const (
	volMgrAddress   = "10.8.65.94:10001"
	mataNodeAddress = "10.8.65.94:10002"
)

const (
	O_RDONLY = 0
	O_WRONLY = 1
	O_RDWR   = 2
)

type CFS struct {
	volID string
}

func CreateVol(name string, capacity string) {
	//fmt.Println("createVol...")
	// send to volmgr to allcate a new vol
	conn, err := grpc.Dial(volMgrAddress, grpc.WithInsecure())
	if err != nil {
		fmt.Printf("did not connect: %v", err)
	}
	defer conn.Close()
	vc := vp.NewVolMgrClient(conn)
	spaceQuota, _ := strconv.Atoi(capacity)
	pCreateVolReq := &vp.CreateVolReq{VolName: name, SpaceQuota: int32(spaceQuota)}
	pCreateVolAck, _ := vc.CreateVol(context.Background(), pCreateVolReq)

	//fmt.Println(pCreateVolAck)

	// send to metadata to registry a new map
	conn2, err2 := grpc.Dial(mataNodeAddress, grpc.WithInsecure())
	if err2 != nil {
		fmt.Printf("did not connect: %v", err2)
	}
	defer conn2.Close()
	mc := mp.NewMetaNodeClient(conn2)
	pmCreateNameSpaceReq := &mp.CreateNameSpaceReq{VolID: pCreateVolAck.UUID}
	mc.CreateNameSpace(context.Background(), pmCreateNameSpaceReq)
}

func GetVolInfo(name string) {
	//fmt.Println("GetVolInfo...")
	// send to volmgr to allcate a new vol
	conn, err := grpc.Dial(volMgrAddress, grpc.WithInsecure())
	if err != nil {
		fmt.Printf("did not connect: %v", err)
	}
	defer conn.Close()
	vc := vp.NewVolMgrClient(conn)
	pGetVolInfoReq := &vp.GetVolInfoReq{UUID: name}
	pGetVolInfoAck, _ := vc.GetVolInfo(context.Background(), pGetVolInfoReq)

	//fmt.Println(pGetVolInfoAck)

	b, _ := json.Marshal(pGetVolInfoAck)
	fmt.Println(string(b))
}

func OpenFileSystem(UUID string) *CFS {
	cfs := CFS{volID: UUID}
	return &cfs
}

func (cfs *CFS) CreateDir(path string) {
	//fmt.Println("CreateDir...")
	conn, err := grpc.Dial(mataNodeAddress, grpc.WithInsecure())
	if err != nil {
		fmt.Printf("did not connect: %v", err)
	}
	defer conn.Close()
	mc := mp.NewMetaNodeClient(conn)
	pCreateDirReq := &mp.CreateDirReq{FullPathName: path, VolID: cfs.volID}
	pCreateDirAck, _ := mc.CreateDir(context.Background(), pCreateDirReq)
	if pCreateDirAck.Ret == 1 {
		fmt.Print("not allowed\n")
		return
	}
	if pCreateDirAck.Ret == 2 {
		fmt.Print("no parent path\n")
		return
	}
	if pCreateDirAck.Ret == 17 {
		fmt.Print("already exist\n")
		return
	}

}
func (cfs *CFS) Stat(path string) {
	conn, err := grpc.Dial(mataNodeAddress, grpc.WithInsecure())
	if err != nil {
		fmt.Printf("did not connect: %v", err)
	}
	defer conn.Close()
	mc := mp.NewMetaNodeClient(conn)
	pStatReq := &mp.StatReq{FullPathName: path, VolID: cfs.volID}
	pStatAck, _ := mc.Stat(context.Background(), pStatReq)
	if pStatAck.Ret == 2 {
		fmt.Print("not existed\n")
		return
	}
	b, _ := json.Marshal(pStatAck)
	fmt.Println(string(b))
}
func (cfs *CFS) List(path string) {
	conn, err := grpc.Dial(mataNodeAddress, grpc.WithInsecure())
	if err != nil {
		fmt.Printf("did not connect: %v", err)
	}
	defer conn.Close()
	mc := mp.NewMetaNodeClient(conn)
	pListReq := &mp.ListReq{FullPathName: path, VolID: cfs.volID}
	pListAck, _ := mc.List(context.Background(), pListReq)
	if pListAck.Ret == 2 {
		fmt.Print("not existed\n")
		return
	}
	for _, value := range pListAck.InodeInfos {
		fmt.Println(value.Name)
	}
}
func (cfs *CFS) ListAll(path string) {
	conn, err := grpc.Dial(mataNodeAddress, grpc.WithInsecure())
	if err != nil {
		fmt.Printf("did not connect: %v", err)
	}
	defer conn.Close()
	mc := mp.NewMetaNodeClient(conn)
	pListReq := &mp.ListReq{FullPathName: path, VolID: cfs.volID}
	pListAck, _ := mc.List(context.Background(), pListReq)
	if pListAck.Ret == 2 {
		fmt.Print("not existed\n")
		return
	}
	for _, value := range pListAck.InodeInfos {
		fmt.Println(value)
	}

}
func (cfs *CFS) DeleteDir(path string) {
	conn, err := grpc.Dial(mataNodeAddress, grpc.WithInsecure())
	if err != nil {
		fmt.Printf("did not connect: %v", err)
	}
	defer conn.Close()
	mc := mp.NewMetaNodeClient(conn)
	pDeleteDirReq := &mp.DeleteDirReq{FullPathName: path, VolID: cfs.volID}
	pDeleteDirAck, _ := mc.DeleteDir(context.Background(), pDeleteDirReq)
	if pDeleteDirAck.Ret == 1 {
		fmt.Println("not allowed")
	}
}

func (cfs *CFS) Rename(path1 string, path2 string) {
	conn, err := grpc.Dial(mataNodeAddress, grpc.WithInsecure())
	if err != nil {
		fmt.Printf("did not connect: %v", err)
	}
	defer conn.Close()
	mc := mp.NewMetaNodeClient(conn)
	pRenameReq := &mp.RenameReq{FullPathName1: path1, FullPathName2: path2, VolID: cfs.volID}
	pRenameAck, _ := mc.Rename(context.Background(), pRenameReq)
	if pRenameAck.Ret == 2 {
		fmt.Println("not existed")
	}
	if pRenameAck.Ret == 1 {
		fmt.Println("not allowed")
	}
}

func (cfs *CFS) OpenFile(path string, flags int32) (int32, *CFile) {
	if flags == O_RDONLY {
		if ret := cfs.CreateFile(path); ret != 0 {
			return ret, nil
		}
	}
	if flags == O_WRONLY {

	}
	cfile := CFile{Path: path, OpenFlag: flags, cfs: cfs}
	return 0, &cfile
}

func (cfs *CFS) CreateFile(path string) int32 {
	conn, err := grpc.Dial(mataNodeAddress, grpc.WithInsecure())
	if err != nil {
		fmt.Printf("did not connect: %v", err)
	}
	defer conn.Close()
	mc := mp.NewMetaNodeClient(conn)
	pCreateFileReq := &mp.CreateFileReq{FullPathName: path, VolID: cfs.volID}
	pCreateFileAck, _ := mc.CreateFile(context.Background(), pCreateFileReq)
	if pCreateFileAck.Ret == 1 {
		fmt.Print("not allowed\n")
		return 1
	}
	if pCreateFileAck.Ret == 2 {
		fmt.Print("no parent path\n")
		return 2
	}
	if pCreateFileAck.Ret == 17 {
		fmt.Print("already exist\n")
		return 17
	}
	return 0
}

type CFile struct {
	cfs      *CFS
	Path     string
	OpenFlag int32

	// for write
	writeOffset int64  // user write offset
	chunkID     int64  // current writing chunk
	writeBuf    []byte // local writing buffer
	lastSeq     int32  // last sequence number

}

func (cfile *CFile) Read(buf []byte, read_size int32, offset int64, reada bool) int32 {
	return 0
}

func (cfile *CFile) Seek(offset int64, whence int32) int64 {
	return 0
}

func (cfile *CFile) Write(buf []byte, write_size int32) int32 {
	return 0
}

func (cfile *CFile) Flush() int32 {
	return 0
}

func (cfile *CFile) Sync() int32 {
	return 0
}

func (cfile *CFile) Close() int32 {
	return 0
}
