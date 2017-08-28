#!/bin/bash
echo "------------- Start to build ContainerFS -------------"

if [ ! -d "./output" ]; then
  mkdir ./output
else
  rm -rf ./output/*
fi

for dir in ./proto/mp ./proto/dp ./proto/vp ./proto/rp  ./proto/kvp
do
  pushd $dir
  make
  popd
done

for dir in CLI fuseclient metanode datanode volmgr repair 
do
  pushd $dir
  go get
  go build -o cfs-$dir main.go
  cp cfs-$dir  ../output
  rm -rf cfs-$dir
  popd
done

echo "------------- build end -------------"
