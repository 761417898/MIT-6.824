package shardkv


// import "shardmaster"
import "labrpc"
import "raft"
import "sync"
import "encoding/gob"
import "shardmaster"
import "time"
import "log"



type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Key      string
	Value    string
	Op       string // "Get", "Put" or "Append"
	ClientId int64  // client id
	SeqNo  int    // request sequence number
}

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	masters      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	mck *shardmaster.Clerk
	order map[int64]int //record reqId of each client
	chs map[int64]chan Op
	
	db map[string]string
}

func (kv *ShardKV) writeToLog(entry Op) bool {
	_, _, isLeader := kv.rf.Start(entry)
	if !isLeader {
		return false
	}
	//fmt.Println("client : ", entry.Id, ", reqId : ", entry.ReqId, ", ", entry.PutAppend, ", key-value : ", entry.Key, " ", entry.Value)
	clientId := entry.ClientId
	//reqId := entry.ReqId
	
	kv.mu.Lock()
	ch,ok := kv.chs[clientId]
	if !ok {
		ch = make(chan Op,1)
		kv.chs[clientId] = ch
	}
	kv.mu.Unlock()
	select {
		case <- time.After(time.Duration(500) * time.Millisecond):
			return false
		case op := <- kv.chs[clientId]:
			return entry == op		
	}
}


func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	var op Op
	op.ClientId = args.ClientId
	op.Key = args.Key
	op.Op = "Get"
	op.SeqNo = args.ReqId
	ok := kv.writeToLog(op)
	if ok {
		reply.WrongLeader = false
		reply.Value = kv.db[args.Key]
	} else {
		reply.WrongLeader = true
		reply.Value = ""
	}
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	var op Op
	op.ClientId = args.ClientId
	op.Key = args.Key
	op.SeqNo = args.ReqId
	op.Value = args.Value
	if args.Op == "Put" {
		op.Op = "Put"
	} else { //"Append"
		op.Op = "Append"
	}
	ok := kv.writeToLog(op)
	if ok {
		reply.WrongLeader = false
	} else {
		reply.WrongLeader = true
	}
}

//
// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *ShardKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *ShardKV) apply(op Op) {
	if op.Op == "Put" {
		kv.db[op.Key] = op.Value
	} else if op.Op == "Append" {
		kv.db[op.Key] = kv.db[op.Key] + op.Value
	} else {
		
	}
}

func (kv *ShardKV) applyDaemon() {
	for {
		applyCh := <- kv.applyCh
		if applyCh.UseSnapshot {
			kv.mu.Lock()
			
			kv.mu.Unlock()
			continue
		} else {
			// have client's request? must filter duplicate command
			switch cmd := applyCh.Command.(type) {
				case Op:
					op := cmd
					clientId := op.ClientId
					reqId := op.SeqNo
					log.Println(kv.gid, kv.me, " LOG APPLY ", op.Op, op.Key, op.Value)
					kv.mu.Lock()	
					if reqId > kv.order[clientId] {
						
						kv.apply(op)
						kv.order[clientId] = reqId
					}
					ch,ok := kv.chs[clientId]
					if !ok {
						ch = make(chan Op,1)
						kv.chs[clientId] = ch
					}		
					select {
						case <-kv.chs[clientId]:
						default:
					}
					kv.chs[clientId] <- op	
					kv.mu.Unlock()
				default:
			}
		}
		
	}
}

//
// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots with
// persister.SaveSnapshot(), and Raft should save its state (including
// log) with persister.SaveRaftState().
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardmaster.
//
// pass masters[] to shardmaster.MakeClerk() so you can send
// RPCs to the shardmaster.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use masters[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, masters []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call gob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	gob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.masters = masters
	kv.mck = shardmaster.MakeClerk(masters)
	// Your initialization code here.

	// Use something like this to talk to the shardmaster:
	// kv.mck = shardmaster.MakeClerk(kv.masters)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	kv.order = make(map[int64]int)  
	kv.chs = make(map[int64]chan Op)
	kv.db = make(map[string]string)

	go kv.applyDaemon()

	return kv
}
