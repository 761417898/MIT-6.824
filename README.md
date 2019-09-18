6.824: Distributed Systems (Spring 2016) 

https://pdos.csail.mit.edu/6.824/index.html

- [OK] Lab 2: Raft

- [OK] Lab 3: Fault-tolerant Key/Value Service

- [OK] Lab 4: Sharded Key/Value Service

主要的框架已经实现完成，项目逻辑结构见 项目构建.docx，但lab4未实现多种容错处理。

一些不好的或要改进的点如下：

- 增加pre-vote阶段来防止挂掉的follow重新连接上

- follow在回送appendEntries加上实际自己需要的nextIndex，避免leader每次只是把nextIndex--来不断重试。

- client在每次发送get/put/append等请求时都需要先获取最新的集群配置信息，可以在每个raft集群的leader上加个转发的功能。

- lab4的容错怎么实现，单个节点挂掉可以通过raft,若某个raft集群挂掉怎么解决？可以把先前的client当做一个中间节点，用来
  管理集群变更信息，即周期性的向每个集群发送心跳包。
  
