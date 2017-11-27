# ContainerFS Metadata Scaling

two approaches are combined to make volume metadata scale horizontally

## Metadata Table Rebalancing

CFS volume master has two scheduling algorithm modules to

1. select target nodes to create replicas

2. periodically move metadata tablets to nodes with more capacity

## Metadata Table Partitioning

for each volume, its metadata table is divided into roughtly equal-size partitions - inode number ranges - called as 'metadata tablets'. 

a metadata tablet is configured as 2^23, which means tablet0 = [0, 2^23 - 1], tablet1 = [2^23, 2^24 - 1], tablet3 = ...

### Dynamic tablet creation

note that tablets are not pre-allocated, by created on demand by the volume master. In other words, for a new cfs volume, just create tablet0 for it, and create tablet1 when tablet0 is to be full..


