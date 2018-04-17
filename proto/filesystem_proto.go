package proto

import "time"

type CreateNameSpaceRequest struct {
	Name string
}

type CreateNameSpaceResponse struct {
	Status int
	Result string
}

type CreateMetaRangeRequst struct {
	MetaId  string
	GroupId int
	Members []string
}

type CreateMetaRangeResponse struct {
	Status uint8
	Result string
}

type CreateRequest struct {
	ParentId uint64
	Name     string
}

type CreateResponse struct {
	ParentId uint64
	Name     string
	Mode     uint8
	Status   int
	Result   string
}

type OpenFileRequest struct {
	ParentId uint64
	Name     string
	Externts []string
}
type InodeInfo struct {
	Inode      uint64
	Name       string
	ModifyTime time.Time
	CreateTime time.Time
	AccessTime time.Time
}

type OpenFileResponse struct {
	ParentId uint64
	Name     string
	Externts []string
	InodeInfo
	Status int
	Result string
}

type DeleteRequest struct {
	ParentId uint64
	Name     string
}

type DeleteRsponse struct {
	Status int
	Result string
}

type RenameRequest struct {
	SrcParentId uint64
	SrcName     string
	DstParentId uint64
	DstName     string
	Status      int
	Result      string
}

type RenameResponse struct {
	Status int
	Result string
}

type ListDirRequest struct {
	ParaentId uint64
}

type ListDirResponse struct {
	ParaentId uint64
	Childrens []InodeInfo
}
