## Containerfs guide

一、本系统有三个组件：

1、meta data node 简称 metanode:主要负责文件系统元数据管理 

2、datanode:主要负责接收实体数据的读写删除 

二、集群搭建

0、组件分配：

	metanode : 192.168.100.216 192.168.100.17 192.168.100.19 
	datanode : 192.168.100.216 192.168.100.17 192.168.100.19 


1、在 192.168.100.216 192.168.100.17 192.168.100.19 启动 metanode： 

	/home/cfs/cfs-metanode -metanode 192.168.100.216 -nodeid 1 -nodepeer 1,2,3 -nodeips 192.168.100.216,192.168.100.17,192.168.100.19 -wal /home/containerfs/metanode/data 
	/home/cfs/cfs-metanode -metanode 192.168.100.17 -nodeid 2 -nodepeer 1,2,3 -nodeips 192.168.100.216,192.168.100.17,192.168.100.19  -wal /home/containerfs/metanode/data 
	/home/cfs/cfs-metanode -metanode 192.168.100.19 -nodeid 3 -nodepeer 1,2,3 -nodeips 192.168.100.216,192.168.100.17,192.168.100.19  -wal /home/containerfs/metanode/data 


2、在 192.168.100.216 192.168.100.17 192.168.100.19 启动 datanode：

	/home/cfs/cfs-datanode -host 192.168.100.216:8801 -tier sas -metanode 192.168.100.216:9903,192.168.100.17:9913,192.168.100.19:9923 -datapath /home/containerfs/datanode1/data
	/home/cfs/cfs-datanode -host 192.168.100.17:8801 -tier sas -metanode 192.168.100.216:9903,192.168.100.17:9913,192.168.100.19:9923 -datapath /home/containerfs/datanode1/data
	/home/cfs/cfs-datanode -host 192.168.100.19:8801 -tier sas -metanode 192.168.100.216:9903,192.168.100.17:9913,192.168.100.19:9923 -datapath /home/containerfs/datanode1/data

	-tier参数说明：cfs支持多种类型的存储介质混部, datanode启动带上-tier参数表示该datanode的磁盘介质类型是哪种, 如sata、sas、ssd、nvme等磁盘类型, 不带该参数默认为sas盘

3、在某台机器，使用命令行工具，创建一个 volume： 

	/home/cfs/cfs-CLI -metanode 192.168.100.216:9903,192.168.100.17:9913,192.168.100.19:9923  -action createvol test 10 sas
	101d18db4043fa26808fce9dc93a6d9f 

	说明：创建volume时支持选择不同存储介质，如sas表示将该volume的data存储到sas盘的datanode上

4、在某客户机，安装 fuse (yum install fuse -y) ,然后挂载步骤5创建的volume：

	/home/cfs/cfs-fuseclient -uuid 101d18db4043fa26808fce9dc93a6d9f -buffertype 1 -metanode 192.168.100.216:9903,192.168.100.17:9913,192.168.100.19:9923 -mountpoint /mnt/mytest -readonly 0

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
