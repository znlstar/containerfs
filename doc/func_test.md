## Cluster StartUp  

#### 1.volmgr start
<pre>
&nbsp; [root@localhost volmgr]# ./main
</pre>
#### 2.datanode start
<pre>
&nbsp; [root@localhost volmgr]# ./main 10.8.65.94 10003 /mnt 10.8.65.94
</pre>
#### 3.metanode start
<pre>
&nbsp; [root@localhost volmgr]# ./main
</pre>

## Volume Operation

#### 1.create volume
<pre>
&nbsp; [root@localhost client]# ./main createvol test 20  
&nbsp; UUID:"dd28337d409a9ba980303c09726a3c5b" 
</pre>

#### 2.get Volume Info
<pre>
&nbsp; [root@localhost client]# ./main getvolinfo dd28337d409a9ba980303c09726a3c5b  
&nbsp; {"VolInfo":{"VolID":"dd28337d409a9ba980303c09726a3c5b","VolName":"test","SpaceQuota":20,"BlockGroups":[{"BlockGroupID":43,"BlockInfos":[{"BlockID":40,"DataNodeIP":168313182,"DataNodePort":10003}]},{"BlockGroupID":44,"BlockInfos":[{"BlockID":41,"DataNodeIP":168313182,"DataNodePort":10003}]}]}}  
</pre>

## Directory Operation

#### 1.mkdir
<pre>
&nbsp; [root@localhost client]# ./main createdir dd28337d409a9ba980303c09726a3c5b /foo  
&nbsp; [root@localhost client]# ./main createdir dd28337d409a9ba980303c09726a3c5b /bar  
</pre>
#### 2.ls
<pre>
&nbsp; [root@localhost client]# ./main ls dd28337d409a9ba980303c09726a3c5b /  
&nbsp; foo  
&nbsp; bar  
</pre>
#### 3.stat
<pre>
&nbsp;[root@localhost client]# ./main stat dd28337d409a9ba980303c09726a3c5b /foo
&nbsp;{"InodeInfo":{"InodeID":1,"Name":"foo","ModifiTime":1487923724,"AccessTime":1487923724}}
&nbsp;[root@localhost client]# ./main stat dd28337d409a9ba980303c09726a3c5b /bar
&nbsp;{"InodeInfo":{"InodeID":2,"Name":"bar","ModifiTime":1487923727,"AccessTime":1487923727}}
</pre>
#### 4.mv
<pre>
&nbsp;[root@localhost client]# ./main mv dd28337d409a9ba980303c09726a3c5b /foo /foo1
&nbsp;[root@localhost client]# ./main mv dd28337d409a9ba980303c09726a3c5b /bar /bar1
&nbsp;[root@localhost client]# ./main stat dd28337d409a9ba980303c09726a3c5b /foo1
&nbsp;{"InodeInfo":{"InodeID":1,"Name":"foo1","ModifiTime":1487923724,"AccessTime":1487923724}}
&nbsp;[root@localhost client]# ./main stat dd28337d409a9ba980303c09726a3c5b /bar1
&nbsp;{"InodeInfo":{"InodeID":2,"Name":"bar1","ModifiTime":1487923727,"AccessTime":1487923727}}
</pre>

#### 5.delete
<pre>
&nbsp;[root@localhost client]# ./main deletedir dd28337d409a9ba980303c09726a3c5b /foo1
&nbsp;[root@localhost client]# ./main deletedir dd28337d409a9ba980303c09726a3c5b /bar1
&nbsp;[root@localhost client]# 
&nbsp;[root@localhost client]# ./main ls dd28337d409a9ba980303c09726a3c5b /
</pre>
## File Operation

#### 1.touch
<pre>
&nbsp;[root@localhost client]# ./main touch dd28337d409a9ba980303c09726a3c5b /foo.txt
</pre>
#### 2.ls
<pre>
&nbsp;[root@localhost client]# ./main ls dd28337d409a9ba980303c09726a3c5b /foo.txt
&nbsp;foo.txt
&nbsp;[root@localhost client]# ./main ls dd28337d409a9ba980303c09726a3c5b /
&nbsp;foo.txt
</pre>
#### 3.stat
<pre>
&nbsp;[root@localhost client]# ./main stat dd28337d409a9ba980303c09726a3c5b /foo.txt
&nbsp;{"InodeInfo":{"InodeID":3,"Name":"foo.txt","ModifiTime":1487924004,"AccessTime":1487924004,"InodeType":true}}
</pre>
#### 4.mv
<pre>
&nbsp;[root@localhost client]# ./main mv dd28337d409a9ba980303c09726a3c5b /foo.txt /bar.txt
&nbsp;[root@localhost client]# ./main stat dd28337d409a9ba980303c09726a3c5b /bar.txt
&nbsp;{"InodeInfo":{"InodeID":3,"Name":"bar.txt","ModifiTime":1487924004,"AccessTime":1487924004,"InodeType":true}}
</pre>
#### 5.put
<pre>
&nbsp;[root@localhost client]# ./main put dd28337d409a9ba980303c09726a3c5b /home/testbig.data /testbig.data
&nbsp;[root@localhost client]# ./main stat dd28337d409a9ba980303c09726a3c5b /testbig.data
&nbsp;{"InodeInfo":{"InodeID":4,"Name":"testbig.data","ModifiTime":1487924212,"AccessTime":1487924212,"InodeType":true,"ChunkIDs":[1,2,3,4,5]}}

&nbsp;[root@localhost client]# ./main put dd28337d409a9ba980303c09726a3c5b /home/testsmall.data /testsmall.data
&nbsp;[root@localhost client]# ./main stat dd28337d409a9ba980303c09726a3c5b /testsmall.data
&nbsp;{"InodeInfo":{"InodeID":5,"Name":"testsmall.data","ModifiTime":1487924261,"AccessTime":1487924261,"InodeType":true,"ChunkIDs":[6]}}
</pre>
#### 6.get
<pre>
&nbsp;[root@localhost client]# ./main get dd28337d409a9ba980303c09726a3c5b /testsmall.data /home/testsmall.data.new
&nbsp;[root@localhost client]# ./main get dd28337d409a9ba980303c09726a3c5b /testbig.data /home/testbig.data.new

&nbsp;[root@localhost client]# md5sum /home/testbig.data /home/testbig.data.new 
&nbsp;3157bff279007e059a1fbda42d3d829d  /home/testbig.data
&nbsp;3157bff279007e059a1fbda42d3d829d  /home/testbig.data.new

&nbsp;[root@localhost client]# md5sum /home/testsmall.data /home/testsmall.data.new 
&nbsp;dab668a583633334e722128ccd71c93d  /home/testsmall.data
&nbsp;dab668a583633334e722128ccd71c93d  /home/testsmall.data.new
</pre>
