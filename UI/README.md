## Containerfs 控制台使用
一、环境要求

    jdk1.8、maven、tomcat8

二、操作说明

	部署war包
		  1.进入UI目录运行如下命令: mvn clean -P all,dev package -Dmaven.test.skip=true
		  2.将containerfs-service/target和containerfs-web/target 目录下的war包部署到tomcat中去
		  3.修改containerfs-service应用WEB-INF/classes/resources.properties文件中的meta.node.infos，配置元数据节点的信息.如：meta.node.infos=172.18.157.233:9903:Node1,172.18.157.234:9913:Node2,172.18.157.235:9923:Node3，每个元数据节点信息为：ip:port:name,以","隔开
		  4.修改containerfs-web应用的WEB-INF/classes/resources.properties配置文件中的node.rest.service.url,配置服务端URL信息，如：node.rest.service.url=http://127.0.0.1:9999/，如果containerfs-service启动的时候配置了上下文路径，还需要将上下文路径加添进去,如：http://127.0.0.1:9999/serv/
