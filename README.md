[![Go Report Card](https://goreportcard.com/badge/github.com/tiglabs/containerfs)](https://goreportcard.com/report/github.com/tiglabs/containerfs)
[![Build Status](https://travis-ci.org/tiglabs/containerfs.svg?branch=master)](https://travis-ci.org/tiglabs/containerfs)
# ContainerFS
![image](doc/logo.png) 

A cluster filesystem for the containers. Please see http://containerfs.io/ for current info.

# Roadmap
* 2017Q4  
1. WEB UI ✔
2. ReplGroup Stream Write for DataNodes  ✔
 
* 2018Q1
1. Choose RaftGroup in MetaNode Pool ✔
2. MetaNode Auto Registry Add ✔
3. Full Posix Interface ✔

* 2018Q2
1. Kernel Client
2. SPDK-NVME driver for Datanode

# Concepts

a volume = a metadata table + multiple block groups

# Design

[here](doc/design.md)

# Guide

[startup](doc/guide.md)
[with k8s](doc/k8sCfsPlugin.md)

## Report a Bug

For filing bugs, suggesting improvements, or requesting new features, please open an [issue](https://github.com/tiglabs/containerfs/issues).

# User Case
