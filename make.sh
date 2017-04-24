#!/bin/bash
echo "------------- Start to build ContainerFS -------------"

if [ ! -d "./output" ]; then
  mkdir ./output
else
  rm -rf ./output/*
fi

for dir in ./proto/mp ./proto/dp ./proto/vp
do
  pushd $dir
  make
  popd
done

for dir in client fuseclient metanode datanode volmgr
do
  pushd $dir
  go build -o cfs-$dir main.go
  cp cfs-$dir cfs-$dir.ini ../output
  popd
done

cp ./service/* ./output
cd ./output
tar zcvf cfs.tar.gz ./*

echo "------------- build end -------------"
