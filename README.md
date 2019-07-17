6.824: Distributed Systems (Spring 2016) 

https://pdos.csail.mit.edu/6.824/index.html

- [OK] Lab 2: Raft

- [OK] Lab 3: Fault-tolerant Key/Value Service

- [OK] Lab 4: Sharded Key/Value Service

There are still some tests that fail. However, the framework for Disdributed K-V Stroage has been finished.


Some unfinished problems like following

- Add pre-vote are to prevent lost followers from reconnecting.

- Follows need to add their nextIndex in reply to AppendEntry to speed up the process of Log Distribution.
  
