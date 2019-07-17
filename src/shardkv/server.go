package shardkv


// import "shardmaster"
import "labrpc"
import "raft"
import "sync"
import "encoding/gob"
import "shardmaster"
import "time"
import "log"
import "bytes"

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
// shardmaster config
type Cfg struct {
	Config shardmaster.Config
}

type Migrate struct {
	Data map[string]string
}

// data migrate args for new shardmaster config
type RequestData struct {
	Shard int // which shard
	Gid int //from group
}

type MigrateArgs struct {
	Shard int
	Gid int
}

type MigrateReply struct {
	WrongLeader bool
	Data map[string]string
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
	servers []*labrpc.ClientEnd
	mck *shardmaster.Clerk
	order map[int64]int //record reqId of each client
	chs map[int]chan Op
	configs       []shardmaster.Config //index 0 means current
	dataMigrate []RequestData
	dataMigrateDone bool
	
	db map[string]string
}

//for debug
func printConfig(cfg shardmaster.Config) {
	log.Println("print num", cfg.Num)
	log.Println("Shards")
	for i := 0; i < len(cfg.Shards); i++ {
		log.Println(i, cfg.Shards[i])
	}
}

func (kv *ShardKV) writeToLog(entry Op) bool {
	logIndex, _, isLeader := kv.rf.Start(entry)
	if !isLeader {
		return false
	}
	//fmt.Println("client : ", entry.Id, ", reqId : ", entry.ReqId, ", ", entry.PutAppend, ", key-value : ", entry.Key, " ", entry.Value)
	//clientId := entry.ClientId
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

func inArray(val int, arr []int) bool{
	for i := 1; i < len(arr); i++ {
		if arr[i] == val {
			return true
		}
	}
	return false
}

func (kv *ShardKV) SendData(args MigrateArgs, reply *MigrateReply) {
	//log.Println("send data11111111111")
	
	_, isLeader := kv.rf.GetState()
	if isLeader {
		//log.Println("send data222222")
		if reply.Data == nil {
	       reply.Data = make(map[string]string)
	    }
		for k, v := range kv.db {
			//log.Println("there is data", k, v)
			if key2shard(k) == args.Shard {
				reply.Data[k] = v
				log.Println("send data", k, v)
			}
		}
		reply.WrongLeader = false
	} else {
		reply.WrongLeader = true
	}
}

func (kv *ShardKV) sendRequest() {	
	_, isLeader := kv.rf.GetState()
	
	if isLeader {
		//log.Println("this is sendRequest.................................")
		var wg sync.WaitGroup
		for i := 1; i < len(kv.dataMigrate); i++ {
			wg.Add(1)
			requestData := kv.dataMigrate[i]
			Shard := requestData.Shard
			gid := kv.configs[0].Shards[Shard]
			//log.Println("request data1111111")
			if servers, ok := kv.configs[0].Groups[gid]; ok {
			// try each server for the shard.
				log.Println(len(servers))
				flag := false
				for !flag {
					for si := 0; si < len(servers) && !flag; si++ {
						srv := kv.make_end(servers[si])
						var args MigrateArgs
						args.Shard = Shard
						var reply MigrateReply
						//log.Println("request data22222")
						ok := srv.Call("ShardKV.SendData", args, &reply)
						if ok && reply.WrongLeader == false {
							flag = true
							log.Println("request data", "from", gid, "for", Shard, " success!!", len(reply.Data))
							migrate := Migrate{Data : reply.Data}
							kv.rf.Start(migrate)
							wg.Done()
						}
					}
				}
			}
		}
		wg.Wait()
		kv.mu.Lock()
		kv.configs[0] = kv.configs[1]
		kv.dataMigrateDone = true
		kv.mu.Unlock()
	}
}

func (kv *ShardKV) switchCfgAndDataMigrate(cfg Cfg) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	kv.dataMigrate = make([]RequestData, 1)
	oldCfg := kv.configs[0]
	newCfg := cfg.Config
	kv.configs[1] = newCfg
	oldCfgShards := make([]int, 1)
	newCfgShards := make([]int, 1)
	for i := 0; i < len(oldCfg.Shards); i++ {
		//i means idx and shard (in config.Shards)
		if oldCfg.Shards[i] == kv.gid {
			oldCfgShards = append(oldCfgShards, i)
		}
	}
	for i := 0; i < len(newCfg.Shards); i++ {
		//i means idx and shard (in config.Shards)
		if newCfg.Shards[i] == kv.gid {
			newCfgShards = append(newCfgShards, i)
		}
	}
	//log.Println(len(newCfgShards), " && ", len(oldCfgShards))
	for idx := 1; idx < len(newCfgShards); idx++ {
		i := newCfgShards[idx] //i means shard, idx means index (in newCfgShards/oldCfgShards)
		if inArray(i, newCfgShards) && !inArray(i, oldCfgShards) &&  oldCfg.Shards[i] != 0{
			var requestData RequestData
			requestData.Shard = i
			requestData.Gid = oldCfg.Shards[i]
			//log.Println(kv.gid, kv.me, "requestdata ", requestData.Shard, "from", requestData.Gid)
			kv.dataMigrate = append(kv.dataMigrate, requestData)
		}
	}
	
	//send request
	if newCfg.Num == 1 {
		kv.configs[0] = kv.configs[1]
		kv.dataMigrateDone = true
		return
	}
	//log.Println("send request with ", len(kv.dataMigrate))
	go kv.sendRequest()
	
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
			r := bytes.NewBuffer(applyCh.Snapshot)
			d := gob.NewDecoder(r)
			var lastIncludeIndex int
			var lastIncludeTerm int
			d.Decode(&lastIncludeIndex)
			d.Decode(&lastIncludeTerm)
			d.Decode(&kv.db)
			d.Decode(&kv.order)
			d.Decode(&kv.configs)
			d.Decode(&kv.dataMigrate)
			d.Decode(&kv.dataMigrateDone)
			kv.mu.Unlock()
			continue
		} else {
			// have client's request? must filter duplicate command
			switch applyCh.Command.(type) {
				case Op:
					op := applyCh.Command.(Op)
					logIndex := applyCh.Index
					clientId := op.ClientId
					reqId := op.SeqNo
					
					kv.mu.Lock()	
					if reqId > kv.order[clientId] {
					//	log.Println(kv.gid, kv.me, " LOG APPLY ", op.Op, op.Key, op.Value)
						kv.apply(op)
						kv.order[clientId] = reqId
					}
					if ch, ok := kv.chs[logIndex]; ok && ch != nil {
						close(ch)
						delete(kv.chs, logIndex)
					}	
					kv.mu.Unlock()
				case Cfg:	
					cfg := applyCh.Command.(Cfg)
					//log.Println(kv.gid, kv.me, "switch config from ", kv.configs[0].Num, " to ", cfg.Config.Num)
					//printConfig(kv.configs[0])
					//printConfig(cfg.Config)
					kv.switchCfgAndDataMigrate(cfg)
				case Migrate:
					migrate := applyCh.Command.(Migrate)
					data := migrate.Data
					//log.Println("receive data", len(data))
					//apply migrate data
					for k,v := range data {
						kv.db[k] = v
					}
			}
			if kv.maxraftstate != -1 && kv.rf.GetPerisistSize() > kv.maxraftstate {
				log.Println("StartSnapshot...")
				w := new(bytes.Buffer)
				e := gob.NewEncoder(w)
				e.Encode(kv.db)
				e.Encode(kv.order)
				e.Encode(kv.configs)
				e.Encode(kv.dataMigrate)
				e.Encode(kv.dataMigrateDone)
				data := w.Bytes()
				kv.rf.StartSnapshot(data, applyCh.Index)
			}
		}
		
	}
}

func (kv *ShardKV) requestNewCfgDaemon() {
	 for {
	 	kv.mu.Lock()
	 	_, isLeader := kv.rf.GetState()
	 	if isLeader {
	 		newCfgNum := kv.configs[0].Num + 1
	 		newCfg := kv.mck.Query(newCfgNum)
	 		if newCfg.Num == kv.configs[0].Num + 1 { //get new config
		 		kv.configs[1] = newCfg
	 			cmd := Cfg{Config : newCfg}
	 			_, isLeader := kv.rf.GetState()
	 			if isLeader && kv.dataMigrateDone {
	 				kv.dataMigrateDone = false
	 				kv.rf.Start(cmd)
	 				//不能只在当前leader计算requestData，避免刚算出来leader就变了
	 			}
	 		}
	 	}
	 	kv.mu.Unlock()
	 	time.Sleep(100 * time.Millisecond)
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
	gob.Register(Cfg{})
	gob.Register(Migrate{})
	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.masters = masters
	kv.mck = shardmaster.MakeClerk(masters)
	kv.servers = servers
	// Your initialization code here.

	// Use something like this to talk to the shardmaster:
	// kv.mck = shardmaster.MakeClerk(kv.masters)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	kv.order = make(map[int64]int)  
	kv.chs = make(map[int]chan Op)
	kv.configs = make([]shardmaster.Config, 2)
	kv.db = make(map[string]string)
	kv.dataMigrate = make([]RequestData,1)
	kv.dataMigrateDone = true
	
	go kv.applyDaemon()
	go kv.requestNewCfgDaemon()

	return kv
}
