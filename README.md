read me docs

目录说明：

store模块：
vol的分类，vol按照类型分为tinyVol和externtVol，分别对应tinyStore和externtVol存储引擎
其中externtStore专为fuse模块开发而设计,而tinyStore则是一个k,v存储子系统
raftStore，其自带了raft和rocksdb，持久化rocksdb，通过raft保证三副本数据一致性

util:包含连接池，log库，配置文件读取，tryMutex锁和一个set

metanode:其主要用来管理metarange中的inode和pid+name的２个数据结构，并通过raftStore持久化到硬盘以及三副本数据一致，并实现文件系统的相关语义

master:主要做datanode,metanode的管理，并管理volGroup和metaGroup，volGroup成员数据一致性检查。通过raftStore进行持久化

datanode:主要存储vol,根据vol的类型不同，而调用storage中不同的存储引擎，并通过后台线程，自动保证volGroup中３个副本数据一致性。

        


