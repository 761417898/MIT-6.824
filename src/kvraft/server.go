package raftkv

import (
	"encoding/gob"
	"labrpc"
	"log"
	"raft"
	"sync"
	"fmt"
	"time"
	"bytes"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}


type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	PutAppend string
	Id int64
	ReqId int
	Key string
	Value string
}

type RaftKV struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	db map[string]string
	order map[int64]int //record reqId of each client
	chs map[int]chan Op
}

func (kv *RaftKV) writeToLog(entry Op) bool {
	logIndex, _, isLeader := kv.rf.Start(entry)
	if !isLeader {
		return false
	}
	fmt.Println("client : ", entry.Id, ", reqId : ", entry.ReqId, ", ", entry.PutAppend, ", key-value : ", entry.Key, " ", entry.Value)
	//clientId := entry.Id
	//reqId := entry.ReqId
	
	kv.mu.Lock()
	ch,ok := kv.chs[logIndex]
	if !ok {
		ch = make(chan Op,1)
		kv.chs[logIndex] = ch
	}
	kv.mu.Unlock()
	select {
		case <- time.After(time.Duration(500) * time.Millisecond):
			return false
		case <- kv.chs[logIndex]:
			return true	
	}
}

func (kv *RaftKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	var op Op
	op.Id = args.Id
	op.Key = args.Key
	op.PutAppend = "Get"
	op.ReqId = args.ReqId
	ok := kv.writeToLog(op)
	if ok {
		reply.IsSuccess = true
		reply.Value = kv.db[args.Key]
	} else {
		reply.IsSuccess = false
		reply.Value = ""
	}
}

func (kv *RaftKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	var op Op
	op.Id = args.Id
	op.Key = args.Key
	op.ReqId = args.ReqId
	op.Value = args.Value
	if args.Op == "Put" {
		op.PutAppend = "Put"
	} else { //"Append"
		op.PutAppend = "Append"
	}
	ok := kv.writeToLog(op)
	if ok {
		reply.IsSuccess = true
	} else {
		reply.IsSuccess = false
	}
}

//
// the tester calls Kill() when a RaftKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *RaftKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *RaftKV) apply(op Op) {
	if op.PutAppend == "Put" {
		kv.db[op.Key] = op.Value
	} else if op.PutAppend == "Append" {
		kv.db[op.Key] = kv.db[op.Key] + op.Value
	} else {
		
	}
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots with persister.SaveSnapshot(),
// and Raft should save its state (including log) with persister.SaveRaftState().
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *RaftKV {
	// call gob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	gob.Register(Op{})

	kv := new(RaftKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.db = make(map[string]string)
	kv.order = make(map[int64]int)
	kv.chs = make(map[int]chan Op)
	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	go func() {
		for {
			applyCh := <- kv.applyCh
			if !applyCh.UseSnapshot {
				log.Println("DEBUG", kv.me, "--", applyCh.Index)	
				op := applyCh.Command.(Op)		
				clientId := op.Id
				reqId := op.ReqId	
				logIndex := applyCh.Index
				kv.mu.Lock()	
				if reqId > kv.order[clientId] {
					kv.apply(op)
					kv.order[clientId] = reqId
					log.Println(kv.me, " LOG APPLY : ", "op.id : ", op.Id,  " op.reqId ", op.ReqId , " type ", op.PutAppend, " key-value ", op.Key, "-", op.Value)	
				}
				
				if ch, ok := kv.chs[logIndex]; ok && ch != nil {
					close(ch)
					delete(kv.chs, logIndex)
				}			
				
				if maxraftstate != -1 && kv.rf.GetPerisistSize() > maxraftstate {
					 log.Println("StartSnapshot...")
					 w := new(bytes.Buffer)
					 e := gob.NewEncoder(w)
				  	 e.Encode(kv.db)
					 e.Encode(kv.order)
					 data := w.Bytes()
					 kv.rf.StartSnapshot(data, applyCh.Index)
				}
				kv.mu.Unlock()
			} else {
				kv.mu.Lock()
				r := bytes.NewBuffer(applyCh.Snapshot)
				d := gob.NewDecoder(r)
				var lastIncludeIndex int
				var lastIncludeTerm int
				d.Decode(&lastIncludeIndex)
				d.Decode(&lastIncludeTerm)
				d.Decode(&kv.db)
				d.Decode(&kv.order)
				if kv.me == 2 {
					log.Println("index2 debug ", lastIncludeIndex, " ", lastIncludeTerm)
				}
				kv.mu.Unlock()
			}
		}
	}()
	return kv
}
