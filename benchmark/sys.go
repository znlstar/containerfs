[root@localhost client]# time for i in `seq 1 10000`; do ./main createdir UUID1 /$i;done

real	1m26.843s
user	0m52.902s
sys	0m45.736s

[root@localhost client]# time ./main ls UUID1 / |wc -l
10000

real	0m0.031s
user	0m0.015s
sys	0m0.018s

[root@localhost client]# time ./main stat UUID1 /10000
{"InodeInfo":{"InodeID":30001,"Name":"10000","ModifiTime":1487127859,"AccessTime":1487127859}}

real	0m0.009s
user	0m0.007s
sys	0m0.003s

[root@localhost client]# time ./main deletedir UUID1 /10000

real	0m0.009s
user	0m0.007s
sys	0m0.003s


[root@localhost client]# time ./main ls UUID1 / |wc -l
9999

real	0m0.033s
user	0m0.015s
sys	0m0.024s

