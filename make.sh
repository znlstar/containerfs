#!/bin/bash
echo "------------- Start to build ContainerFS -------------"

if [ ! -d "./output" ]; then
  mkdir ./output
else
  rm -rf ./output/*
fi

for dir in ./proto/mp ./proto/dp ./proto/kvp ./proto/vp
do
  pushd $dir
  make
  popd
done

for dir in CLI fuseclient metanode datanode volmgr
do
  pushd $dir
  go get
  go build -ldflags "-X github.com/tiglabs/containerfs/utils.Release=`git tag|head -n 1` -X github.com/tiglabs/containerfs/utils.Build=`git rev-parse HEAD`" -o cfs-$dir 
  cp cfs-$dir  ../output
  rm -rf cfs-$dir
  popd
done

echo "------------- build end -------------"
