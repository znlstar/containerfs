## Containerfs Design
The architecture of Containerfs is data symmetric and mata centrally (raft group); This design enables following features.

* High performance  
All the matadata been storaged on mem and persistence on SSD in mata node, This avoid overmuch calculate like Hash-Type-Storage.

* Linear scalability in performance and capacity  
When more performance or capacity is needed, Containerfs can be grown
linearly by simply adding new datanode to the cluster.

* No single point of failure  
Even if a machine fails, the data is still accessible through other machines.

* Easy administration  


## Architecture

Containerfs is a distributed file system and provides an instant volume to Containerfs client (fuse).
See the later sections for more details of each Containerfs component.
![image](https://github.com/zhengxiaochuan-3/containerfs/blob/adddoc20170119/doc/architecture.png)

#### Volume  
Volume is a Containerfs instant. Each Containerfs cluster may have multiple volumes.
A volume has one matadata table and multiple (Number of replication) block groups

#### Inode  
Inode is a mata struct to record the file or directory attributes.
Every Inode has a unique  32 bit inodeID in volume.

#### Chunk  
Chunk is a fixed file data obj (64MB default), a file is divided by chunk and storage on data node.
Every Chunk has a unique 64 bit ID in volume.

#### Block   
Block is a fixed space (1GB default), Each physic data disk produce multiple (total capacity / 1GB) blocks.
Every Block has a globle unique  32 bit ID.
One physic disk runing one datanode server program, when server startup, the disk mountpoint ( /datanode1/ Etc.) be created numble of capacity(GB) Directories, like /datanode1/block-UUID1/ /datanode1/block-UUID2/ /datanode1/block-UUID2/

#### Matadata table  
Metadata table is a sorted key-value map from "parent inodeID + name" to inode attributes.
Metadata table been persistenced periodic, and been loaded at server startup . 
A metadata table record all the inodes in one volume. Each volume has one metadata table.

#### Block Group  
Each volume has multiple (Number of replication) block groups. When volume been created (100GB Etc.) only one block (1GB) been allocated from data node to this block group, and the blockID be record to volume-info table.
Along with the file chunk being writed to block, when total chunk size over 1GB, another block will be allocated to this group. Similar to the word "copy-on-write", you can call it "allocate-on-write"

#### Mata node  
Mata node is a server daemon, holds one or multiple matadata table in mem.
All the inode attributes be recorded on mata node.
Raft protocol be used to keep a strong consistency between mata nodes

#### Data node  
Data node is a server daemon, holds data blocks in disk.
All the file chunk be read or write to data node blocks.

#### Volume manager  
Volume manager is a server daemon, holds all the cluster-level metadata, like the volume space quota, nodes status.
Background data storage may use mysql.

#### Client  
A private client: provide private api for app.  
A fuse mountable client: provide mountpoint for posix operate.

## Communication
client stub grpc  
server streamed grpc

## Process

#### cluster startup
1. Volume manager startup.  
2. Mata nodes startup, load persistence matadata, jion the leader,leader send heartbeat info to Volume Manager.  
3. Data node startup, send heartbeat info to Volume Manager, report the physic disk status, if health then add or update to Volume Manager table, if bad then delete from Volume Manager block table.  


#### Volume create
1. Cmdtool or RESTful api create a volume  
2. allocate volumeID, create a volume record in volume-info table  
3. send message to mata node create a new map  
4. allocate a block from block table, update volume record  

#### open I/O flow
1. client send open request to mata node to apply for a inodeID  
2. mata node response a inodeID and the first data node ip and blockID (apply from volume manager, also manager will record this block to volume-info table)  


#### write I/O flow
1. client send write request to data node  
2. client send message to mata node to update mata (blockID,chunk cnt)  
3. mata node responese a data node ip and blockID (if block used full, mata node will apply a new one from volume manager,also manager will record this block to volume-info table)  
4. loop util end  


#### read I/O flow
1. client send read request to data node  
2. client send message to mata node to update mata  
3. mata node responese a data node ip and blockID (if block used full, mata node will apply a new one from volume manager)  
4. loop util end  


#### replication
Data replication of Containerfs is simple. We assume that there is only
one writer, so write collision cannot happen.  Clients can send write
request to the target data node in parallel, and send read requests to one of
the target data node.



## Struct

#### inode
<pre>
&nbsp;struct Inode {
&nbsp; // todo
&nbsp;}
</pre>

#### todo

## Volume manager sql tables
todo
 
## Structure pic
todo   
=======
## Containerfs Design
The architecture of Containerfs is data symmetric and mata centrally (raft group); This design enables following features.

* High performance  
All the matadata been storaged on mem and persistence on SSD in mata node, This avoid overmuch calculate like Hash-Type-Storage.

* Linear scalability in performance and capacity  
When more performance or capacity is needed, Containerfs can be grown
linearly by simply adding new datanode to the cluster.

* No single point of failure  
Even if a machine fails, the data is still accessible through other machines.

* Easy administration  


## Architecture

Containerfs is a distributed file system and provides an instant volume to Containerfs client (fuse).
See the later sections for more details of each Containerfs component.

#### Volume  
Volume is a Containerfs instant. Each Containerfs cluster may have multiple volumes.
A volume has one matadata table and multiple (Number of replication) block groups

#### Inode  
Inode is a mata struct to record the file or directory attributes.
Every Inode has a unique  32 bit inodeID in volume.

#### Chunk  
Chunk is a fixed file data obj (64MB default), a file is divided by chunk and storage on data node.
Every Chunk has a unique 64 bit ID in volume.

#### Block   
Block is a fixed space (1GB default), Each physic data disk produce multiple (total capacity / 1GB) blocks.
Every Block has a globle unique  32 bit ID.
One physic disk runing one datanode server program, when server startup, the disk mountpoint ( /datanode1/ Etc.) be created numble of capacity(GB) Directories, like /datanode1/block-UUID1/ /datanode1/block-UUID2/ /datanode1/block-UUID2/

#### Matadata table  
Metadata table is a sorted key-value map from "parent inodeID + name" to inode attributes.
Metadata table been persistenced periodic, and been loaded at server startup . 
A metadata table record all the inodes in one volume. Each volume has one metadata table.

#### Block Group  
Each volume has multiple (Number of replication) block groups. When volume been created (100GB Etc.) only one block (1GB) been allocated from data node to this block group, and the blockID be record to volume-info table.
Along with the file chunk being writed to block, when total chunk size over 1GB, another block will be allocated to this group. Similar to the word "copy-on-write", you can call it "allocate-on-write"

#### Mata node  
Mata node is a server daemon, holds one or multiple matadata table in mem.
All the inode attributes be recorded on mata node.
Raft protocol be used to keep a strong consistency between mata nodes

#### Data node  
Data node is a server daemon, holds data blocks in disk.
All the file chunk be read or write to data node blocks.

#### Volume manager  
Volume manager is a server daemon, holds all the cluster-level metadata, like the volume space quota, nodes status.
Background data storage may use mysql.

#### Client  
A private client: provide private api for app.  
A fuse mountable client: provide mountpoint for posix operate.

## Communication
client stub grpc  
server streamed grpc

## Process

#### cluster startup
1. Volume manager startup.  
2. Mata nodes startup, load persistence matadata, jion the leader,leader send heartbeat info to Volume Manager.  
3. Data node startup, send heartbeat info to Volume Manager, report the physic disk status, if health then add or update to Volume Manager table, if bad then delete from Volume Manager block table.  


#### Volume create
1. Cmdtool or RESTful api create a volume  
2. allocate volumeID, create a volume record in volume-info table  
3. send message to mata node create a new map  
4. allocate a block from block table, update volume record  

#### open I/O flow
1. client send open request to mata node to apply for a inodeID  
2. mata node response a inodeID and the first data node ip and blockID (apply from volume manager, also manager will record this block to volume-info table)  


#### write I/O flow
1. client send write request to data node  
2. client send message to mata node to update mata (blockID,chunk cnt)  
3. mata node responese a data node ip and blockID (if block used full, mata node will apply a new one from volume manager,also manager will record this block to volume-info table)  
4. loop util end  


#### read I/O flow
1. client send read request to data node  
2. client send message to mata node to update mata  
3. mata node responese a data node ip and blockID (if block used full, mata node will apply a new one from volume manager)  
4. loop util end  


#### replication
Data replication of Containerfs is simple. We assume that there is only
one writer, so write collision cannot happen.  Clients can send write
request to the target data node in parallel, and send read requests to one of
the target data node.



## Struct

#### inode
<pre>
&nbsp;struct Inode {
&nbsp; // todo
&nbsp;}
</pre>

#### todo

## Volume manager sql tables
todo
 
## Structure pic
todo      

