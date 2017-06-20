在单台机器( centos7.x )部署,包括：redis、cfs-volmgr、cfs-metanode、cfs-datanode、cfs-client、cfs-fuseclient

安装：
1、假设机器IP为 10.8.65.94
2、yum 安装 mysql 和 redis 并启动
3、解压 cfs.tar.gz 到 /home 
4、cd /home/cfs/ 
5、dos2unix install.sh
6、chmod +x install.sh
7、./install.sh

配置：
1、cd /usr/local/conf/ 
2、分别配置 cfs-volmgr.ini cfs-metanode.ini cfs-datanode.ini （简单易懂的配置）

启动：
service cfs-volmgr start
service cfs-metanode start
service cfs-datanode start

创建 volume:

配置 cfs-client.ini 
/home/cfs/cfs-client /home/cfs/cfs-client.ini createvol yourname 40

挂载 volume:
配置 cfs-fuseclient.ini 
/usr/local/bin/cfs-fuseclient /usr/local/conf/cfs-fuseclient.ini
