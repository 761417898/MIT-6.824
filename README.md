6.824: Distributed Systems (Spring 2016) 

https://pdos.csail.mit.edu/6.824/index.html

- [OK] Lab 2: Raft

- [OK] Lab 3: Fault-tolerant Key/Value Service

- [OK] Lab 4: Sharded Key/Value Service

主要的框架已经实现完成，项目逻辑结构见 项目总结.docx，但lab4只实现了最基本的功能，lab2 lab3个别test未通过。

一些不好的或要改进的点如下：

- 增加pre-vote阶段来防止挂掉的follow重新连接上

- follow在回送appendEntries加上实际自己需要的nextIndex，避免leader每次只是把nextIndex--来不断重试。

- client在每次发送get/put/append等请求时都需要先获取最新的集群配置信息，可以在每个raft集群的leader上加个转发的功能。

  
