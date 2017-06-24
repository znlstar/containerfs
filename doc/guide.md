## Containerfs guide

一、本系统有三个组件：

1、volume manager 简称 volmgr:主要负责 volume 、blockgroup 、block 的分配管理 

2、meta data node 简称 metanode:主要负责文件系统元数据管理 

3、datanode:主要负责接收实体数据的读写删除 

二、集群搭建

1、组件分配：

	volmgr   : 192.168.100.100
	metanode : 192.168.100.101 192.168.100.102 192.168.100.103
	datanode : 192.168.100.104 192.168.100.105 192.168.100.106

2、组件安装：

	0. volmgr 需要 mysql,请导入cfs-volmgr.sql
	1. 解压 cfs-server.tar.gz 到 /home 
	2. cd /home/cfs-server/ 
	3. dos2unix install.sh
	4. chmod +x install.sh
	5. ./install.sh

	[sql](../volmgr/cfs-volmgr.sql)

3、组件配置：

	1. cd /usr/local/conf
	2. 分别在组件分配规划的服务器中配置 cfs-volmgr.ini cfs-metanode.ini cfs-datanode.ini 

		1) cfs-volmgr.ini 

			host = 192.168.100.100
			port = 10001
			log  = /home/containerfs/volmgr/logs
			loglevel   = debug

			[mysql]
			host   = 127.0.0.1:3306(你的数据库地址) 
			user   = root(你的数据库用户名)
			passwd = root(你的数据库密码)
			db     = containerfs

		2) cfs-metanode.ini

			第一个节点：
		    [metanode]
		    host     = 192.168.100.101
			nodeid   = 1
			peers    = 1,2,3
			ips      = 192.168.100.101,192.168.100.102,192.168.100.103
			waldir    = /home/containerfs/metanode/data
			log      = /home/containerfs/metanode/logs
			loglevel = error
			[volmgr]
			host = 192.168.100.100:10001

			第二个节点：
		    [metanode]
		    host     = 192.168.100.102
			nodeid   = 2
			(其他一样)

			第三个节点：
		    [metanode]
		    host     = 192.168.100.103
			nodeid   = 3
			(其他一样)

		3) cfs-datanode.ini

			host = 192.168.100.104
			port = 10002
			path = /home/containerfs/datanode/data
			log  = /home/containerfs/datanode/logs
			loglevel = error
			[volmgr]
			host = 192.168.100.100
			port = 10001

			一个服务器可以部署一个 datanode ,也可以部署多个,以端口区分,path 对应各自的数据盘挂载路径

4、组件启动：

	service cfs-volmgr start
	service cfs-metanode start
	service cfs-datanode start (如果一个服务器上部署多个 datanode 需要自己增加一下 service )

	[datanode.service](../service/cfs-datanode.service)

三、集群使用

	1、安装客户端
    	解压 cfs-client.tar.gz 到 /home 
    2、创建一个 volume

    	配置 cfs-client.ini

    	[volmgr]
		host = 192.168.100.100:10001
		[metanode]
		host = 192.168.100.101:9903,192.168.100.102:9913,192.168.100.103:9923


		执行: /home/cfs-client/cfs-client /home/cfs-client/cfs-client.ini createvol yourvolname 10
			  (10 代表 10GB , 至少 10GB 以上，且是 10 的倍数)

	3、挂载该 volume 
		1. 如果上一个步骤成功的话，创建命令执行，会返回一个 UUID ，该 UUID 即这个 volume 的唯一标识
		2. 在客户端机器上安装 fuse , yum install fuse
		3. 配置 cfs-fuseclient.ini

			volmgr     = 192.168.100.100:10001
			metanode   = 192.168.100.101:9903,192.168.100.102:9913,192.168.100.103:9923
			uuid       = 623be31a406d9df9803080ff42085ac7
			mountpoint = /tmp/mnt
			log        = /home/containerfs/fuseclient/logs
			loglevel   = debug 

	4、上述步骤执行成功的话，在客户端机器上 df -h 即可看到了挂载后的盘，比如：


			[root@bogon ~]# df -h
			Filesystem                                    Size  Used Avail Use% Mounted on
			/dev/mapper/centos-root                        50G   15G   36G  30% /
			devtmpfs                                      3.7G     0  3.7G   0% /dev
			tmpfs                                         3.7G   60M  3.7G   2% /dev/shm
			tmpfs                                         3.7G  114M  3.6G   4% /run
			tmpfs                                         3.7G     0  3.7G   0% /sys/fs/cgroup
			/dev/sda1                                     497M  336M  162M  68% /boot
			/dev/mapper/centos-home                       181G   11G  170G   6% /home
			tmpfs                                         753M   24K  753M   1% /run/user/1000
			tmpfs                                         753M     0  753M   0% /run/user/0
			ContainerFS-623be31a406d9df9803080ff42085ac7   10G  512M  9.5G   5% /tmp/mnt

	5、/tmp/mnt 目录可以当作本地目录正常使用了，注意:不支持随机读写。



