# definition of containerfs

# concepts

cluster, volume, namespace, directory, file, inode, block

extent (a list of continous blocks), shard (data repl unit)

datanode, volmanager, metadata service

# cluster metadata 

VolumeTable

ShardTable

DataNodeTable

# volume metadata

keyspace = inode table (itable) + directory table (dtable)

itable: ino --> <ctime, mtime, .., extentArray>

extentArray = marshalled[<startOffset, endOffset, shardID>]

dtable: <parentIno, name> --> childIno

# metadata storage

currently we adopts vitess

# shard

shards host extents, where each extent is stored as a seperate file named as <volumeID, ino, startOffset>

# shard replication

master <-> slave -> backup (optional, and another IDC maybe)

# failure recorvery

crash-stop model?

# operations

1. mkfs

2. resizefs

3. statfs


