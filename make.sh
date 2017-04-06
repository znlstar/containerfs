#!/bin/bash 
echo "------------- Start to build ContainerFS -------------"

if [ ! -d "./output" ]; then
  mkdir ./output
else
  rm -rf ./output/*
fi

cd ./proto/mp/
make
cd -

cd ./proto/dp/
make
cd -

cd ./proto/vp/
make
cd -

cd ./client
go build -o cfs-client main.go
cp cfs-client ../output
cd -

cd ./fuseclient
go build -o cfs-fuse-client main.go
cp cfs-fuse-client mountpoint.ini ../output
cd -

cd ./metanode
go build -o cfs-metanode main.go
cp cfs-metanode metanode.ini ../output
cd -

cd ./datanode
go build -o cfs-datanode main.go
cp cfs-datanode datanode.ini ../output
cd -

cd ./volmgr
go build -o cfs-volmgr main.go
cp cfs-volmgr volmgr.ini ../output
cd -

echo "------------- build end -------------"



