package raft

/*
 * 成员变更分两步，提交两条日志，并执行。
 * 1. 包含旧集群所有成员，包含新集群所有成员。在此应用之后的quorum要求既在旧集群是大多数派，又在新集群是大多数派。
 * 2. 包含新集群所有成员
 * 避免了脑裂。
 */

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import "sync"
import "labrpc"
import "time"
import "math/rand"
import "fmt"
//import "log"

 import "bytes"
 import "encoding/gob"



//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

type LogEntry struct {
	Index       int
	Term        int
	Command     interface{}
}

const (
	Follower = iota
	Candidate
	Leader
)

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm int
	voteFor int
	leader int
	role int
	
	elecTimeRefresh chan int
	numVotes int
	
	logApply chan int
	log []LogEntry
	lastApplied int
	commitIndex int
	
	lastLogIndex int
	lastLogTerm int
	
	nextIndex []int
	matchIndex []int
	applyCh chan ApplyMsg
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	term = rf.currentTerm
	isleader = rf.leader == rf.me
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	 w := new(bytes.Buffer)
	 e := gob.NewEncoder(w)
	 e.Encode(rf.currentTerm)
	 e.Encode(rf.voteFor)
	 e.Encode(rf.log)
	 data := w.Bytes()
	 rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	// Your code here (2C).
	// Example:
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	d.Decode(&rf.currentTerm)
	d.Decode(&rf.voteFor)
	d.Decode(&rf.log)
}

func (rf *Raft) readSnapshot() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	data := rf.persister.snapshot
	if len(data) == 0 { // bootstrap without any state?
		return	
	}
	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	
	var lastIncludeIndex int
	var lastIncludeTerm int
	d.Decode(&lastIncludeIndex)
	d.Decode(&lastIncludeTerm)
	
	var newLog []LogEntry
	baseIndex := rf.log[0].Index
	newLog = append(newLog, LogEntry{Index:lastIncludeIndex, Term:lastIncludeTerm})
	for idx := lastIncludeIndex; idx <= rf.lastLogIndex; idx++ {
		newLog = append(newLog, rf.log[idx - baseIndex])
	}
	rf.log = newLog
	rf.lastLogIndex = newLog[len(newLog) - 1].Index
	rf.lastLogTerm = newLog[len(newLog) - 1].Term
	rf.commitIndex = newLog[0].Index
	rf.lastApplied = newLog[0].Index
	rf.persist()
	
	msg := ApplyMsg{UseSnapshot:true, Snapshot:rf.persister.snapshot}
	rf.applyCh <- msg
}

func (rf *Raft) GetPerisistSize() int {
	return rf.persister.RaftStateSize()
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term int
	CandidateId int
	LastLogIndex int
	LastLogTerm int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term int
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term int
	LeaderId int
	PrevLogIndex int
	PrevLogTerm int
	Entries []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term int
	Success bool
	NextIndex int
}

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

type InstallSnapshotReply struct {
	Term int
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	fmt.Println("RequestVote ", args.CandidateId, " ", args.Term)
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.persist()
	defer rf.mu.Unlock()
	reply.VoteGranted = false
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		return
	}
	if args.Term > rf.currentTerm {
		rf.role = Follower
		//这里是重置voteFor，而不是直接投票，因为不一定满足5.4.1中的选举限制
		rf.voteFor = -1
		rf.elecTimeRefresh <- 0
	}
	
	rf.currentTerm = args.Term
	reply.Term = rf.currentTerm
	uptodate := false 
	if (args.LastLogTerm > rf.lastLogTerm) || (args.LastLogTerm == rf.lastLogTerm) && (args.LastLogIndex >= rf.lastLogIndex) {
		//fmt.Println(args.LastLogIndex, " ", args.LastLogTerm, " ", rf.lastLogIndex, " ", rf.lastLogTerm)
		uptodate = true
	}
	if uptodate && (rf.voteFor == -1 || rf.voteFor == args.CandidateId) {
		rf.voteFor = args.CandidateId
		reply.VoteGranted = true
		fmt.Println(rf.me, " vote for ", args.CandidateId)
	}
}

func (rf *Raft) printLog() {
	fmt.Print(rf.me, " term ", rf.currentTerm, " log: ")
	for lidx := 0; lidx < len(rf.log); lidx++ {
		//fmt.Print(rf.log[lidx].Term, " ")
		//fmt.Print(rf.log[lidx].Command.(int), " ")
	}	
	fmt.Println()
}

func (rf *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) {
	// Your code here (2A, 2B).
	//fmt.Println(rf.me, "receive heartbeat")
	rf.mu.Lock()
	defer rf.persist()
	defer rf.mu.Unlock()
	
	reply.Success = false
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		return
	}
	if args.Term > rf.currentTerm || (args.Term == rf.currentTerm && rf.role == Candidate){
		rf.currentTerm = args.Term
		rf.role = Follower
		rf.leader = args.LeaderId
		rf.elecTimeRefresh <- 0
	} else if args.Term == rf.currentTerm {
		//先离开又加入的旧leader，且该leader先发送appendentries给目前leader
		rf.leader = args.LeaderId
		rf.elecTimeRefresh <- 0
	}
	reply.Term = rf.currentTerm
	
	if args.PrevLogIndex == 0 && args.PrevLogTerm == 0 {
		//leader log为空
		rf.commitIndex = args.LeaderCommit
		reply.Success = true
	} else if args.PrevLogIndex == -1 && args.PrevLogTerm == -1 {
		//leader发了他全部的日志，全部接受了
		if args.Entries[0].Index > 1 {
			//通过快照
			return
		}
		rf.log = args.Entries
		rf.lastLogIndex = rf.log[len(rf.log) - 1].Index
		rf.lastLogTerm = rf.log[len(rf.log) - 1].Term
		rf.commitIndex = args.LeaderCommit
		reply.Success = true
	} else {
		if args.PrevLogIndex > rf.lastLogIndex {
			//日志缺失
			reply.NextIndex = rf.lastLogIndex + 1
			reply.Success = false
			return
		} else {
			//检测日志是否一致,若在i位置一致，则把发来的日志加到i后面，否则false
			var i int
			consist := false
			for i = len(rf.log) - 1; i >= 0; i-- {
				if args.PrevLogIndex == rf.log[i].Index && args.PrevLogTerm == rf.log[i].Term {
					//leader发来的条目可以加到i后面
					var newLog []LogEntry
					for idx := 0; idx <= i; idx++ {
						newLog = append(newLog, rf.log[idx])
					}
					for idx := 0; idx < len(args.Entries); idx++ {
						newLog = append(newLog, args.Entries[idx])
					}
					rf.log = newLog
					consist = true
					
					break
				} 
			}
			if !consist {
				reply.Success = false
				reply.NextIndex = rf.commitIndex + 1
				return
			}
		}
		rf.lastLogIndex = rf.log[len(rf.log) - 1].Index
		rf.lastLogTerm = rf.log[len(rf.log) - 1].Term
		rf.commitIndex = args.LeaderCommit
		reply.Success = true	
	}

}

func (rf *Raft) InstallSnapshot(args InstallSnapshotArgs,reply *InstallSnapshotReply) {
	//log.Println("InstallSnapshot")
	rf.mu.Lock()
	defer rf.persist()
	defer rf.mu.Unlock()
	if args.Term > rf.currentTerm || (args.Term == rf.currentTerm && rf.role == Candidate){
		rf.currentTerm = args.Term
		rf.role = Follower
		rf.leader = args.LeaderId
		rf.elecTimeRefresh <- 0
	} else if args.Term == rf.currentTerm {
		//先离开又加入的旧leader，且该leader先发送appendentries给目前leader
		rf.leader = args.LeaderId
		rf.elecTimeRefresh <- 0
	}
	reply.Term = rf.currentTerm
	baseIndex := -1
	if len(rf.log) > 0 {
		baseIndex = rf.log[0].Index
	}
	if args.LastIncludedIndex < baseIndex {
		
	} else {
		var newLog []LogEntry
		newLog = append(newLog, LogEntry{Index:args.LastIncludedIndex, Term:args.LastIncludedTerm})
		if args.LastIncludedIndex < rf.lastLogIndex {
			for idx := args.LastIncludedIndex; idx <= rf.lastLogIndex; idx++ {
				newLog = append(newLog, rf.log[idx - baseIndex])
			}
		}
		fmt.Print(len(newLog))
		rf.log = newLog
		rf.lastLogIndex = newLog[len(newLog) - 1].Index
		rf.lastLogTerm = newLog[len(newLog) - 1].Term
		rf.commitIndex = newLog[0].Index
		rf.lastApplied = newLog[0].Index
		fmt.Print(len(newLog))
		fmt.Println("............................................", rf.commitIndex, " ", rf.lastApplied, len(rf.log),"...................................")
		rf.persister.SaveSnapshot(args.Data)
		rf.persist()
		var msg ApplyMsg
		msg.Index = args.LastIncludedIndex
		msg.Snapshot = args.Data
		msg.UseSnapshot = true
		rf.applyCh <- msg
	}
}


//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if !ok {
		return ok
	}
	if reply.Term > rf.currentTerm {
		rf.role = Follower
		rf.currentTerm = reply.Term
		rf.persist()
		rf.elecTimeRefresh <- 0
	} else {
		//fmt.Println(reply.Term, reply.VoteGranted)
		if reply.VoteGranted == true {
			//fmt.Println(server, " vote for ", rf.me)
			rf.numVotes++
			if rf.numVotes > len(rf.peers) / 2 && rf.role == Candidate{
				rf.role = Leader
				rf.leader = rf.me
				rf.elecTimeRefresh <- 0
			}
		}
	}
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if !ok {
		return ok
	}
	
	if reply.Term > rf.currentTerm {
		rf.role = Follower
		rf.currentTerm = reply.Term
		rf.persist()
		rf.elecTimeRefresh <- 0
		return ok
	}
	if reply.Success == true {
		rf.matchIndex[server] = rf.lastLogIndex
		rf.nextIndex[server] = rf.lastLogIndex + 1
		if rf.matchIndex[server] == 2 && rf.nextIndex[server] == 3 && rf.commitIndex == 1{
		//fmt.Println("-------------------------------  MATCH INDEX CHANGED *****************************************")
		}
	} else {
		
		if rf.nextIndex[server] > 0 {
			rf.nextIndex[server]-- //= reply.NextIndex
		}
	} 
	return ok
}

func (rf *Raft) sendInstallSnapshot(server int,args InstallSnapshotArgs,reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if ok {
		if reply.Term > rf.currentTerm {
			rf.role = Follower
			rf.currentTerm = reply.Term
			rf.persist()
			rf.elecTimeRefresh <- 0
			return ok
		}
	}
	return ok
}
//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.role != Leader {
		isLeader = false
		return index, term, isLeader
	}
	// Your code here (2B).
	rf.lastLogIndex++
	rf.lastLogTerm = rf.currentTerm
	rf.log = append(rf.log, LogEntry{Index : rf.lastLogIndex, Term : rf.lastLogTerm, Command : command})
	rf.persist()
	index = rf.lastLogIndex
	term = rf.lastLogTerm
	isLeader = true
	//lastLogIndex变了,nextIndex也要变
	for i := 0; i < len(rf.peers); i++ {
		rf.nextIndex[i] = rf.lastLogIndex + 1
	}
	//fmt.Println("---------------------------", rf.me, " term ", rf.currentTerm, " add logEntry ", command.(int), rf.lastLogIndex, rf.lastLogTerm, "-----------------------")
	return index, term, isLeader
}

func (rf *Raft) StartSnapshot(snapshot []byte, index int) {
	fmt.Println(rf.me, " Start SnapShot : ", index)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if index <= rf.log[0].Index || index > rf.lastLogIndex {
		return
	}
	baseIndex := rf.log[0].Index
	newLog := rf.log[(index - baseIndex):]
	rf.log = newLog
	
	rf.persist()

	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(newLog[0].Index)
	e.Encode(newLog[0].Term)

	data := w.Bytes()
	data = append(data, snapshot...)
	rf.persister.SaveSnapshot(data)
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
}


func (rf *Raft) broadcastRequestVotes() {
	rf.mu.Lock()
	rf.currentTerm++
	rf.voteFor = rf.me
	rf.numVotes = 1
	rf.persist()
	rf.mu.Unlock()
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		server := i
		var args RequestVoteArgs
		args.CandidateId = rf.me
		args.Term = rf.currentTerm
		args.LastLogIndex = rf.lastLogIndex
		args.LastLogTerm = rf.lastLogTerm
		var reply RequestVoteReply
		reply.VoteGranted = false
		go rf.sendRequestVote(server, args, &reply)
	}
}

/*
 * 2. 主要关注两个变量，nextIndex(要发给该结点的下一条日志，初始化为主节点日志的最后一条+1，即刚开始默认全部同步了)和
 * matchIndex（记录所有follow节点的日志复制到哪了），只有某条日志已复制到大多数节点，才可以被提交。
 * 
 *	提交也有一条限制，即主节点只能提交本周期内的log
 *
 */

func (rf *Raft) broadcastAppendEntries() {
	//外面已经加锁了
	//fmt.Println(rf.me, rf.currentTerm, " send appendentry")
	oldCommitIndex := rf.commitIndex
	//检测是否复制到大多数机器上
	for i := 0; i < len(rf.matchIndex); i++ {
		count := 1
		matIdx := rf.matchIndex[i]
		for j := 0; j < len(rf.matchIndex); j++ {
			if matIdx == rf.matchIndex[j] {
				count++
			}
		}
		if len(rf.log) > 0 {
			baseIndex := rf.log[0].Index
			if count > len(rf.peers) / 2 && matIdx > rf.commitIndex && rf.log[matIdx - baseIndex].Term == rf.currentTerm{
				rf.commitIndex = matIdx
			}
		} else {
			if count > len(rf.peers) / 2 && matIdx > rf.commitIndex {
				rf.commitIndex = matIdx
			}
		}
	}
	if rf.commitIndex != oldCommitIndex {
		//fmt.Println("DEBUG:", rf.commitIndex, " ", oldCommitIndex)
		//fmt.Println("-------------------------------  LOG COMMIT :", rf.commitIndex, " lastapply ", rf.lastApplied, "--------------------------------------------")
		rf.logApply <- 0
	}
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		server := i
		var args AppendEntriesArgs
		args.LeaderId = rf.me
		args.Term = rf.currentTerm
		args.LeaderCommit = rf.commitIndex
		if len(rf.log) == 0 {
			//当前日志为空
			args.Entries = rf.log
			args.PrevLogIndex = 0
			args.PrevLogTerm = 0
		} else {
			baseIndex := rf.log[0].Index
			//fmt.Println("log len = ", len(rf.log), i, rf.nextIndex[i], baseIndex)
			
			//fmt.Println(args.Entries[0].Index)
			if rf.lastLogIndex == baseIndex || (rf.nextIndex[i] - baseIndex - 1) == 0{
				//要发送的日志的第一个条目正好是　rf.log[0]，prev设置为-1
				args.Entries = rf.log
				args.PrevLogIndex = -1
				args.PrevLogTerm = -1
			} else if rf.nextIndex[i] <= baseIndex{
				//fmt.Println("SendInstall")
				var installSnapshotArgs InstallSnapshotArgs
				installSnapshotArgs.Term = rf.currentTerm
				installSnapshotArgs.LeaderId = rf.me
				installSnapshotArgs.LastIncludedIndex = rf.log[0].Index
				installSnapshotArgs.LastIncludedTerm = rf.log[0].Term
				installSnapshotArgs.Data = rf.persister.snapshot
				go func(server int,args InstallSnapshotArgs) {
					var installSnapshotReply InstallSnapshotReply
					rf.sendInstallSnapshot(server, installSnapshotArgs, &installSnapshotReply)
				}(i, installSnapshotArgs)
				rf.matchIndex[i] = installSnapshotArgs.LastIncludedIndex
				rf.nextIndex[i] = installSnapshotArgs.LastIncludedIndex + 1
				continue
			} else {
				args.Entries = rf.log[rf.nextIndex[i] - baseIndex - 1:]
				prevLogArrayIndex := rf.nextIndex[i] - baseIndex - 2
				//fmt.Println("log len = ", len(rf.log), "preLogArrayIndex = ", prevLogArrayIndex, rf.nextIndex[i], baseIndex)
				args.PrevLogIndex = rf.log[prevLogArrayIndex].Index
				args.PrevLogTerm = rf.log[prevLogArrayIndex].Term 
			}
		}
		
		var reply AppendEntriesReply
		go rf.sendAppendEntries(server, args, &reply)
	}
}

/*
 * 1.主节点选举，每个follow在超时时间内没受到心跳(appendEntries)，则变为candidate,term++,投票给自己，并广播requestVotes
 *	收到广播的节点，如果term小于candidate，则修正term并投票给他，如果term等于它且投过票，或者term大于它，则不理睬。
 *	节点处于candidate也要有超时限制，超时后则重新变为candidate,广播请求。   当节点受到大多数票时，成为leader，并立即广播心跳
 *  
 *  投票有个安全性限制，即自己的日志没他的新，才会投票给他。也就是比较最后一条日志的term，term相等就比较index。
 */

func (rf *Raft) electionDaemon() {
	for {	
	switch rf.role {
		case Follower:
			select {
				case <- time.After(time.Duration(rand.Int63() % 333 + 550) * time.Millisecond):
					rf.mu.Lock()
					fmt.Println(rf.me, " term ", rf.currentTerm, " timeout become candidate")
					rf.role = Candidate
					rf.mu.Unlock()
				case <- rf.elecTimeRefresh:
				//	fmt.Println(rf.me, " term ", rf.currentTerm, " follower refresh	")
			}
		case Candidate:
			rf.broadcastRequestVotes()
			select {
				case <- time.After(time.Duration(rand.Int63() % 333 + 550) * time.Millisecond):
					//截止到超时还未处理成功，一是平票，二是断开连接，重置currentTerm
					//单数节点，断开连接
					rf.mu.Lock()
					rf.currentTerm--
					rf.voteFor = -1
					rf.role = Follower
					rf.persist()
					rf.mu.Unlock()
				fmt.Println(rf.me, " term ", rf.currentTerm, " candidate timeout")
				case <- rf.elecTimeRefresh:
				fmt.Println(rf.me, " term ", rf.currentTerm, " candidate refresh	")
			}
		case Leader:
			fmt.Println(rf.me, " term ", rf.currentTerm, " become leader ", " heartbeat ", "with lastApply, lenLOG ", rf.lastApplied, len(rf.log), " ", )
			
			rf.mu.Lock()
			rf.nextIndex = make([]int, len(rf.peers))
			rf.matchIndex = make([]int, len(rf.peers))
			for i := range rf.peers {
				rf.nextIndex[i] = rf.lastLogIndex + 1
				rf.matchIndex[i] = 0
			}
			rf.broadcastAppendEntries()
			rf.logApply <- 0
			rf.mu.Unlock() 
		    
			for rf.role == Leader {
				select {
					case <- time.After(time.Duration(rand.Int63() % 30 + 50) * time.Millisecond):
						rf.broadcastAppendEntries()
					case <- rf.elecTimeRefresh:
						fmt.Println(rf.me, " term ", rf.currentTerm, " Leader2Follower")
				}
			}
	}
	}
}

func (rf *Raft) logDaemon(applyCh chan ApplyMsg) {
	for {
		select {
			case <- rf.logApply:
			case <- time.After(time.Duration(1000) * time.Millisecond):	
		}
		if len(rf.log) == 0 {
			continue
		}
		for rf.lastApplied < rf.commitIndex {
			rf.mu.Lock()
			baseIndex := rf.log[0].Index
			rf.lastApplied++
			logArrayIndex := rf.lastApplied - baseIndex
			var applyMsg ApplyMsg
			//fmt.Println(rf.me, " ", logArrayIndex, " ", len(rf.log), " ", rf.lastApplied, " ", baseIndex)
			rf.mu.Unlock()
			applyMsg.Command = rf.log[logArrayIndex].Command
			applyMsg.Index = rf.lastApplied
			applyMsg.UseSnapshot = false
			applyCh <- applyMsg
		}
		
	}
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//

func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.applyCh = applyCh

	// Your initialization code here (2A, 2B, 2C).
	rf.currentTerm = 0
	rf.leader = -1
	rf.voteFor = -1
	rf.role = Follower
	rf.elecTimeRefresh = make(chan int, 100)
	rf.logApply = make(chan int, 100)
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.lastLogIndex = 0
	rf.lastLogTerm = 0
	rf.readPersist(persister.ReadRaftState())
	rf.readSnapshot()
	if len(rf.log) > 0 {
		if rf.voteFor == rf.me {
			rf.role = Leader
			rf.leader = rf.me
			//rf.currentTerm++
		} else {
			rf.role = Follower
		}
		rf.lastLogIndex = rf.log[len(rf.log) - 1].Index
		rf.lastLogTerm = rf.log[len(rf.log) - 1].Term
		fmt.Println("------------------", rf.log[0].Index, "  ", rf.log[0].Term, " ", rf.voteFor, "  ", rf.currentTerm)
	}
	go rf.electionDaemon()
	// initialize from state persisted before a crash
	
	go rf.logDaemon(applyCh)
	
	return rf
}
