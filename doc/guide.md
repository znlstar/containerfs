## Containerfs guide

一、本系统有四个组件: 

1、Volume Manager 简称 volmgr : 主要负责集群元数据管理

2、Metadata node 简称 metanode : 主要负责文件系统元数据管理 

3、datanode : 主要负责接收实体数据的读写删除 

4、fuseclient : 主要负责 volume 的挂载和文件读写

二、集群搭建

0、组件分配：

    volmgr   : 192.168.100.16 192.168.100.17 192.168.100.18 
	metanode : 192.168.100.16 192.168.100.17 192.168.100.18
	datanode : 192.168.100.16 192.168.100.17 192.168.100.18 

1、在 192.168.100.16 192.168.100.17 192.168.100.18 启动 volmgr： 

	/home/cfs/cfs-volmgr -host 192.168.100.16 -nodeid 1 -nodepeer 1,2,3 -nodeips 192.168.100.16,192.168.100.17,192.168.100.18 -wal /home/containerfs/volmgr/data  
    /home/cfs/cfs-volmgr -host 192.168.100.17 -nodeid 2 -nodepeer 1,2,3 -nodeips 192.168.100.16,192.168.100.17,192.168.100.18 -wal /home/containerfs/volmgr/data     
    /home/cfs/cfs-volmgr -host 192.168.100.18 -nodeid 3 -nodepeer 1,2,3 -nodeips 192.168.100.16,192.168.100.17,192.168.100.18 -wal /home/containerfs/volmgr/data  

2、在 192.168.100.16 192.168.100.17 192.168.100.18 启动 metanode： 

	/home/cfs/cfs-metanode -host 192.168.100.16 -nodeid 1 -volmgr 192.168.100.16:7703,192.168.100.17:7713,192.168.100.18:7723 -wal /home/containerfs/metanode/data 
	/home/cfs/cfs-metanode -host 192.168.100.17 -nodeid 2 -volmgr 192.168.100.16:7703,192.168.100.17:7713,192.168.100.18:7723 -wal /home/containerfs/metanode/data 
	/home/cfs/cfs-metanode -host 192.168.100.18 -nodeid 3 -volmgr 192.168.100.16:7703,192.168.100.17:7713,192.168.100.18:7723 -wal /home/containerfs/metanode/data 

3、在 192.168.100.16 192.168.100.17 192.168.100.18 启动 datanode：

	/home/cfs/cfs-datanode -host 192.168.100.16:8801 -tier sas -volmgr 192.168.100.16:7703,192.168.100.17:7713,192.168.100.18:7723 -datapath /home/containerfs/datanode1/data
	/home/cfs/cfs-datanode -host 192.168.100.17:8801 -tier sas -volmgr 192.168.100.16:7703,192.168.100.17:7713,192.168.100.18:7723 -datapath /home/containerfs/datanode1/data
	/home/cfs/cfs-datanode -host 192.168.100.18:8801 -tier sas -volmgr 192.168.100.16:7703,192.168.100.17:7713,192.168.100.18:7723 -datapath /home/containerfs/datanode1/data

	-tier参数说明：cfs支持多种类型的存储介质混部, datanode启动带上-tier参数表示该datanode的磁盘介质类型是哪种, 如sata、sas、ssd、nvme等磁盘类型, 不带该参数默认为sas盘

4、在某台机器，使用命令行工具，创建一个 volume： 

	/home/cfs/cfs-CLI -volmgr 192.168.100.16:7703,192.168.100.17:7713,192.168.100.18:7723  -action createvol test 10 sas
	101d18db4043fa26808fce9dc93a6d9f 

	说明：创建volume时支持选择不同存储介质，如sas表示将该volume的data存储到sas盘的datanode上

5、在某客户机，安装 fuse (yum install fuse -y) ,然后挂载步骤5创建的volume：

	/home/cfs/cfs-fuseclient -uuid 101d18db4043fa26808fce9dc93a6d9f -buffertype 1 -volmgr 192.168.100.16:7703,192.168.100.17:7713,192.168.100.18:7723 -mountpoint /mnt/mytest -readonly 0

	[root@node-219 ~]# df -h
	Filesystem                                    Size  Used Avail Use% Mounted on
	/dev/mapper/centos-root                        70G   34G   37G  48% /
	devtmpfs                                       16G     0   16G   0% /dev
	tmpfs                                          16G     0   16G   0% /dev/shm
	tmpfs                                          16G  1.2G   15G   8% /run
	tmpfs                                          16G     0   16G   0% /sys/fs/cgroup
	/dev/sda1                                     497M  166M  331M  34% /boot
	/dev/mapper/centos-home                       150G   45G  106G  30% /home
	tmpfs                                         3.2G     0  3.2G   0% /run/user/0
	ContainerFS-101d18db4043fa26808fce9dc93a6d9f   10G     0   10G   0% /mnt/mytest
