package raft

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

// import "bytes"
// import "encoding/gob"



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
	// w := new(bytes.Buffer)
	// e := gob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := gob.NewDecoder(r)
	// d.Decode(&rf.xxx)
	// d.Decode(&rf.yyy)
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
}




//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term int
	CandidateId int
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

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	//fmt.Println("RequestVote ", args.CandidateId, " ", args.Term)
	// Your code here (2A, 2B).
	rf.mu.Lock()
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
	uptodate := true 
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
		fmt.Print(rf.log[lidx].Command.(int), " ")
	}	
	fmt.Println()
}

func (rf *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
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
		rf.log = args.Entries
		rf.lastLogIndex = rf.log[len(rf.log) - 1].Index
		rf.lastLogTerm = rf.log[len(rf.log) - 1].Term
		rf.commitIndex = args.LeaderCommit
		reply.Success = true
	} else {
		if args.PrevLogIndex > rf.lastLogIndex {
			//日志缺失
			//reply.NextIndex = rf.lastLogIndex + 1
			reply.Success = false
			return
		} else {
			//检测日志是否一致,若在i位置一致，则把发来的日志加到i后面，否则返回false
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
				return
			}
		}
		rf.lastLogIndex = rf.log[len(rf.log) - 1].Index
		rf.lastLogTerm = rf.log[len(rf.log) - 1].Term
		rf.commitIndex = args.LeaderCommit
		reply.Success = true	
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
		rf.nextIndex[server]-- //= reply.NextIndex
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

	index = rf.lastLogIndex
	term = rf.lastLogTerm
	isLeader = true
	//lastLogIndex变了,nextIndex也要变
	for i := 0; i < len(rf.peers); i++ {
		rf.nextIndex[i] = rf.lastLogIndex + 1
	}
	fmt.Println("---------------------------", rf.me, " term ", rf.currentTerm, " add logEntry ", command.(int), rf.lastLogIndex, rf.lastLogTerm, "-----------------------")
	return index, term, isLeader
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
	rf.mu.Unlock()
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		server := i
		var args RequestVoteArgs
		args.CandidateId = rf.me
		args.Term = rf.currentTerm
		var reply RequestVoteReply
		reply.VoteGranted = false
		go rf.sendRequestVote(server, args, &reply)
	}
}

func (rf *Raft) broadcastAppendEntries() {
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
		if count > len(rf.peers) / 2 && matIdx > rf.commitIndex {
			rf.commitIndex = matIdx
		}
		if count > 1 {
		//fmt.Println("-------------------------------  ",count,  rf.commitIndex, matIdx, oldCommitIndex," ****************************")
		}
	}
	if rf.commitIndex != oldCommitIndex {
		fmt.Println("-------------------------------  LOG APPLY *****************************************")
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
			args.Entries = rf.log[rf.nextIndex[i] - baseIndex - 1:]
			//fmt.Println(args.Entries[0].Index)
			if rf.lastLogIndex == baseIndex {
				//要发送的日志的第一个条目正好是　rf.log[0]，prev设置为-1
				args.PrevLogIndex = -1
				args.PrevLogTerm = -1
			} else {
				prevLogArrayIndex := rf.nextIndex[i] - baseIndex - 2
				args.PrevLogIndex = rf.log[prevLogArrayIndex].Index
				args.PrevLogTerm = rf.log[prevLogArrayIndex].Term 
			}
		}
		
		var reply AppendEntriesReply
		go rf.sendAppendEntries(server, args, &reply)
	}
}

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
					rf.currentTerm = 0
					rf.voteFor = -1
					rf.role = Follower
					rf.mu.Unlock()
				fmt.Println(rf.me, " term ", rf.currentTerm, " candidate timeout")
				case <- rf.elecTimeRefresh:
				fmt.Println(rf.me, " term ", rf.currentTerm, " candidate refresh	")
			}
		case Leader:
			fmt.Println(rf.me, " term ", rf.currentTerm, " become leader ", " heartbeat ")
			rf.mu.Lock()
			rf.nextIndex = make([]int, len(rf.peers))
			rf.matchIndex = make([]int, len(rf.peers))
			rf.broadcastAppendEntries()
			for i := range rf.peers {
				rf.nextIndex[i] = rf.lastLogIndex + 1
				rf.matchIndex[i] = 0
			}
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
			case <- time.After(time.Duration(500) * time.Millisecond):	
		}
		if len(rf.log) == 0 {
			continue
		}
		baseIndex := rf.log[0].Index
		for rf.lastApplied < rf.commitIndex {
			rf.lastApplied++
			logArrayIndex := rf.lastApplied - baseIndex
			var applyMsg ApplyMsg
			applyMsg.Command = rf.log[logArrayIndex].Command
			applyMsg.Index = rf.lastApplied
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
	
	go rf.electionDaemon()
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	go rf.logDaemon(applyCh)
	
	return rf
}
