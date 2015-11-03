# perf
Performance comparison between go-couchbase and gocb

Prerequisites : couchbase buckets default and beer-sample should be available

Following  numbers are from my MacBook Pro 8 core cpu and 16g Ram

1. set operations
```
./perf -set=true -documents=3000000 -threads=5
Did 600000 ops in 2m8.422195797s
Did 600000 ops in 2m8.579336516s
Did 600000 ops in 2m8.830333924s
Did 600000 ops in 2m8.928204874s
Did 600000 ops in 2m9.122339822s
**** Did 3000000 ops in 2m9.447697615s. Ops/sec 23175

-- Flush the bucket between comparisons ------

./perf -set=true -engine=gocb -documents=3000000 -threads=5
Did 600000 ops in 4m22.587881236s
Did 600000 ops in 4m22.634094171s
Did 600000 ops in 4m22.688211899s
Did 600000 ops in 4m22.74005549s
Did 600000 ops in 4m22.747291s
**** Did 3000000 ops in 4m23.084925075s. Ops/sec 11403
```
2. Bulk Get operations
```
./perf -documents=3000000 -engine=gocb -threads=5
Did 600000 ops in 41.157999992s
Did 600000 ops in 41.170969531s
Did 600000 ops in 41.183918284s
Did 600000 ops in 41.19642842s
Did 600000 ops in 41.207672576s

Bolt:perf manik$ ./perf -documents=3000000 -threads=5
Did 600000 ops in 30.746101793s
Did 600000 ops in 30.747048523s
Did 600000 ops in 30.760126224s
Did 600000 ops in 30.766951164s
Did 600000 ops in 30.767967134s
**** Did 3000000 ops in 30.768009421s. Ops/sec 97503
```
