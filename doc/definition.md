# definition of containerfs

# concepts

cluster, volume, namespace, directory, file, inode, extent, blockgroup

datanode, metanode

# cluster metadata 

VolumeTable

BlockGroupTable

DataNodeTable

MetaNodeTable

# volume metadata

namespace = inode table (itable) + directory table (dtable)

itable: ino --> marshalled attributes(ctime, mtime, .., extentMap)

extent = <startBlockNo, endBlockNo>
extentMap = extent --> blockgroupID

dtable: <parentIno, name> --> childIno

both itable and dtable are sorted key-value maps implemented as leveldb/rocksdb

each namespace is replicated by a raft group

# blockgroup

a blockgroup contains extents/segments, i.e. continous blocks, where each extent is stored as a seperate file.

three replicas form a chained replication: head -> middle -> tail



