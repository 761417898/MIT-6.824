6.824: Distributed Systems (Spring 2016) 

基本流程是shardkv里的client(shardkv/client.go)去查找shardmaster(shardmaster/shardmaster.go)自己要找的数据分片在那个raft集群上，
然后再向对应的raft集群发出读写请求。

此处的raft集群是在raft（raft/raft.go）构建的一个主从复制的kv存储，通过raft保证单一主节点
以及不会发生写丢失。

同样，用于保存数据分片信息的shardmaster也可以通过raft来保障数据的可靠性。
