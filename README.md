[![Go Report Card](https://goreportcard.com/badge/github.com/tigcode/containerfs)](https://goreportcard.com/report/github.com/tigcode/containerfs)
[![Build Status](https://travis-ci.org/tigcode/containerfs.svg?branch=master)](https://travis-ci.org/tigcode/containerfs)
# ContainerFS
![image](doc/logo.png) 

A cluster filesystem for the containers. Please see http://containerfs.io/ for current info.

# Roadmap
2017Q4 WEB UI
       ReplGroup for Datanode
       
2018Q1 Kernel Client
       Volume meta auto split

2018Q2 SPDK-NVME driver for Datanode

# Concepts

a volume = a metadata table + multiple block groups

# Design

[here](doc/design.md)

# Guide

[startup](doc/guide.md)
[with k8s](doc/k8sCfsPlugin.md)

# User Case
