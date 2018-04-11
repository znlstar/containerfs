[![Go Report Card](https://goreportcard.com/badge/github.com/tiglabs/containerfs)](https://goreportcard.com/report/github.com/tiglabs/containerfs)
[![Build Status](https://travis-ci.org/tiglabs/containerfs.svg?branch=master)](https://travis-ci.org/tiglabs/containerfs)
# ContainerFS
![image](doc/logo.png) 

## Overview

What is CFS? 

CFS is distributed filesystem and object storage service. And it provides four pragmatic abstractions: 

L1: object store without namespaces - particularly for images or short video etc. Put an object and the system returns an unique key. Objects are immutable and can be delete however. 

L2: object store with plat namespaces - compatible with the S3 API. 

L3: filesystems with hierachical namespaces, random read but append-only write. Big data systems like HBase could be run on it. 

L4: filesystems with hierachical namespaces, random read/write, and complelete filesystem semantics. 

## Architecture

CFS consists of several subsystems: 

* the cluster master

* the metanode cluster

* the blocknode cluster

* the objectnode cluster

## APIs

s3-compatible object storage API 

NFS

FUSE

Java SDK

Go SDK

## Use Cases


