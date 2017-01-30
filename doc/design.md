## Containerfs Design

## Architecture

See the later sections for more details of each Containerfs component.
![image](https://github.com/zhengxiaochuan-3/containerfs/blob/adddoc20170119/doc/architecture.png)

#### Volume  
Volume is a Containerfs instance.  One Containerfs cluster can host millions of volumes.
A volume has one matadata table and unlimited number of block groups

#### Inode  
Inode is a data structure recording the file or directory attributes, indexed with a unique 64bit integer.

#### Matadata Table 
A metadata table is a sorted key-value map from "parent inodeID + name" to inode attributes.
One volume is associated with a metadata table.
Metadata tables are replicated via Raft, and not sharded by design. 

#### Extent
<offset, lenght>

#### Block Group  
Fix-sized replicated storage unit of file extents

#### MetaNode
Hosts one or multiple metadata tables

#### DataNode
Hosts one or multiple block groups

#### Volume Manager  
VolMgr holds all the cluster-level metadata, like the volume space quota, nodes status. 
Data storage options: 1. MGR; 2. replicated VolMgr via Raft.

#### Client
FUSE
Linux kernel

## Communication

## Core Functions

#### cluster startup


#### Volume create
1. Cmdtool or RESTful api create a volume  
2. allocate volumeID, create a volume record in volume-info table  
3. send message to mata node create a new map  
4. allocate a block from block table, update volume record  

#### open I/O flow

#### write I/O flow

#### read I/O flow


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

