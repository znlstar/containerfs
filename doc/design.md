## Containerfs Design

## Architecture

See the later sections for more details of each Containerfs component.
![image](architecture.png)

#### Volume  
Volume is a Containerfs instance.  One Containerfs cluster can host millions of volumes.
A volume has one metadata table and unlimited number of block groups

#### Inode  
Inode is a data structure recording the file or directory attributes, indexed with a unique 64bit integer.

#### Metadata Table 
A metadata table is a sorted key-value map from "parent inodeID + name" to inode attributes.
One volume is associated with a metadata table.
Metadata tables are replicated via Raft, and not sharded by design. 

#### Block Group  
Fix-sized replicated storage unit of file extents

#### MetaNode
Hosts one or multiple metadata tables

#### DataNode
Hosts one or multiple block groups

#### Volume Manager  
VolMgr holds all the cluster-level metadata, like the volume space quota, nodes status. 

#### Client
FUSE  
Linux kernel

## Communication

## Core Functions

#### Volume create

#### open I/O flow

#### write I/O flow

#### read I/O flow


## Struct
#### volume namespace
<pre>
&nbsp;type KvStateMachine struct {
&nbsp;	id      uint64
&nbsp;	applied uint64
&nbsp;	raft    *raft.RaftServer
&nbsp;
&nbsp;	dentryItem btree.DentryKV
&nbsp;	inodeItem  btree.InodeKV
&nbsp;	bgItem     btree.BGKV
&nbsp;
&nbsp;	dentryData     *btree.BTree
&nbsp;	inodeData      *btree.BTree
&nbsp;	blockGroupData *btree.BTree
&nbsp;
&nbsp;	chunkIDLocker sync.Mutex
&nbsp;	chunkID       uint64
&nbsp;
&nbsp;	inodeIDLocker sync.Mutex
&nbsp;	inodeID       uint64
&nbsp;}
</pre>

#### dentry
<pre>
&nbsp;type Dirent struct {
&nbsp;	InodeType bool   `protobuf:"varint,1,opt,name=InodeType" json:"InodeType,omitempty"`
&nbsp;	Inode     uint64 `protobuf:"varint,2,opt,name=Inode" json:"Inode,omitempty"`
&nbsp;}
</pre>

#### inode
<pre>
&nbsp;type InodeInfo struct {
&nbsp;	ModifiTime int64        `protobuf:"varint,1,opt,name=ModifiTime" json:"ModifiTime,omitempty"`
&nbsp;	AccessTime int64        `protobuf:"varint,2,opt,name=AccessTime" json:"AccessTime,omitempty"`
&nbsp;	Link       uint32       `protobuf:"varint,3,opt,name=Link" json:"Link,omitempty"`
&nbsp;	FileSize   int64        `protobuf:"varint,4,opt,name=FileSize" json:"FileSize,omitempty"`
&nbsp;	Chunks     []*ChunkInfo `protobuf:"bytes,5,rep,name=Chunks" json:"Chunks,omitempty"`
&nbsp;}
</pre>

#### block group
<pre>
&nbsp;type BlockGroup struct {
&nbsp;	BlockGroupID uint32       `protobuf:"varint,1,opt,name=BlockGroupID" json:"BlockGroupID,omitempty"`
&nbsp;	FreeSize     int64        `protobuf:"varint,2,opt,name=FreeSize" json:"FreeSize,omitempty"`
&nbsp;	Status       int32        `protobuf:"varint,3,opt,name=Status" json:"Status,omitempty"`
&nbsp;	BlockInfos   []*BlockInfo `protobuf:"bytes,4,rep,name=BlockInfos" json:"BlockInfos,omitempty"`
&nbsp;}
&nbsp;
&nbsp;type BlockInfo struct {
&nbsp;	BlockID      uint32 `protobuf:"varint,1,opt,name=BlockID" json:"BlockID,omitempty"`
&nbsp;	DataNodeIP   int32  `protobuf:"varint,2,opt,name=DataNodeIP" json:"DataNodeIP,omitempty"`
&nbsp;	DataNodePort int32  `protobuf:"varint,3,opt,name=DataNodePort" json:"DataNodePort,omitempty"`
&nbsp;	Status       int32  `protobuf:"varint,4,opt,name=Status" json:"Status,omitempty"`
&nbsp;}
</pre>

#### chunk
<pre>
&nbsp;type ChunkInfo struct {
&nbsp;	ChunkID      uint64  `protobuf:"varint,1,opt,name=ChunkID" json:"ChunkID,omitempty"`
&nbsp;	ChunkSize    int32   `protobuf:"varint,2,opt,name=ChunkSize" json:"ChunkSize,omitempty"`
&nbsp;	BlockGroupID uint32  `protobuf:"varint,3,opt,name=BlockGroupID" json:"BlockGroupID,omitempty"`
&nbsp;	Status       []int32 `protobuf:"varint,4,rep,packed,name=Status" json:"Status,omitempty"`
&nbsp;}
</pre>
