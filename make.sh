#!/bin/bash
echo "------------- Start to build ContainerFS -------------"

if [ ! -d "./output" ]; then
  mkdir ./output
else
  rm -rf ./output/*
fi

for dir in ./proto/mp ./proto/dp ./proto/vp ./proto/rp
do
  pushd $dir
  make
  popd
done

for dir in client fuseclient metanode datanode volmgr repair 
do
  pushd $dir
  go get
  go build -o cfs-$dir main.go
  cp cfs-$dir cfs-$dir.ini ../output
  popd
done

cd ./fuseclient_flag
  go get
  go build -o cfs-fuseclient_flag main.go
  cp cfs-fuseclient_flag ../output
cd ..

cd ./client_flag
  go get
  go build -o cfs-client_flag main.go
  cp cfs-client_flag ../output
cd ..

cp ./service/* ./output
cd ./output
tar zcvf cfs.tar.gz ./*

echo "------------- build end -------------"
