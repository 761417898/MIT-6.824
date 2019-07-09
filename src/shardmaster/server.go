package shardmaster


import "raft"
import "labrpc"
import "sync"
import "time"
import "encoding/gob"


type ShardMaster struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.
	order map[int64]int //record reqId of each client
	chs map[int64]chan Op

	configs []Config // indexed by config num
}


type Op struct {
	// Your data here.
	ClientID int64  // for duplicate request detection
	SeqNo    int    // sequence No.
	Op       string // must be one of "Join", "Leave", "Move" and "Query"

	Servers map[int][]string // args of "Join"
	GIDs    []int            // args of "Leave"
	Shard   int              // args of "Move"
	GID     int              // args of "Move"
	Num     int              // args of "Query" desired config number
}

// common function
func (sm *ShardMaster) requestAgree(cmd Op, fillReply func(success bool)) {
	_, _, isLeader := sm.rf.Start(cmd)
	if !isLeader {
		fillReply(false)
	}
	sm.mu.Lock()
	ch,ok := sm.chs[cmd.ClientID]
	if !ok {
		ch = make(chan Op,1)
		sm.chs[cmd.ClientID] = ch
	}
	sm.mu.Unlock()

	select {
		case op := <-ch:
			if op.ClientID == cmd.ClientID && op.SeqNo == cmd.SeqNo {
				fillReply(true)
			} else {
				fillReply(false)
			}
		case <- time.After(time.Duration(5000) * time.Millisecond):
			fillReply(false)
	}
}

func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	cmd := Op{ClientID: args.ClientID, SeqNo: args.SeqNo, Op: "Join", Servers: args.Servers,}
	sm.requestAgree(cmd, func(success bool) {
		if success {
			reply.WrongLeader = false
			reply.Err = OK
		} else {
			reply.WrongLeader = true
			reply.Err = ""
		}
	})
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	cmd := Op{ClientID: args.ClientID, SeqNo: args.SeqNo, Op: "Leave", GIDs: args.GIDs}
	sm.requestAgree(cmd, func(success bool) {
		if success {
			reply.WrongLeader = false
			reply.Err = OK
		} else {
			reply.WrongLeader = true
			reply.Err = ""
		}
	})
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	cmd := Op{ClientID: args.ClientID, SeqNo: args.SeqNo, Op: "Move", Shard: args.Shard, GID: args.GID}
	sm.requestAgree(cmd, func(success bool) {
		if success {
			reply.WrongLeader = false
			reply.Err = OK
		} else {
			reply.WrongLeader = true
			reply.Err = ""
		}
	})
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	cmd := Op{ClientID: args.ClientID, SeqNo: args.SeqNo, Op: "Query", Num: args.Num}
	sm.requestAgree(cmd, func(success bool) {
		if success {
			reply.WrongLeader = false
			reply.Err = OK

			// if success, copy current or past Config
			sm.mu.Lock()
			sm.copyConfig(args.Num, &reply.Config)
			sm.mu.Unlock()
		} else {
			reply.WrongLeader = true
			reply.Err = ""
		}
	})
}


//
// the tester calls Kill() when a ShardMaster instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sm *ShardMaster) Kill() {
	sm.rf.Kill()
	// Your code here, if desired.
}

// needed by shardkv tester
func (sm *ShardMaster) Raft() *raft.Raft {
	return sm.rf
}

type Status struct {
	group, count int
}

// should only be called when holding the lock
// target: minimum shard movement
func (sm *ShardMaster) rebalance(config *Config) {
//核心就两点：均匀分布，集群变化时数据移动尽可能少。	
//这里用的最基本的方法	
//groups raft组数目	
	groups := len(config.Groups)	
	average := len(config.Shards)

	if groups == 0 {
		return
	}
//平均每组需要存至少average个shards	
	average = len(config.Shards) / groups       // shards per replica group
//每组存最少数目shards，一共还剩下left个shards	
	left := len(config.Shards) - groups*average // extra shards after average

	// average may be zero, when group is more than shard
	if average == 0 {
		return
	}

//reverse表示 groupId -> shard mapping的映射
	var reverse = make(map[int][]int)
	for s, g := range config.Shards {
		reverse[g] = append(reverse[g], s)
	}

	// info
	var extra []Status
	var lacking []Status

	cnt1, cnt2 := 0, 0
//用于修正join。若存在新加入的group(假定为k)，则定不存在reverse[k]，则在不移动的情况下,k至少缺少average个shards，把k对应缺少数存到
//lacking，总的缺少数用cnt1表示	
	for k, _ := range config.Groups {
		if _, ok := reverse[k]; !ok {
			lacking = append(lacking, Status{k, average})
			cnt1 += average
		}
	}
	// if some group is removed?
//用于修正leave。k移动走了，但它存的共len(v)个shards还未处理。总的未处理数用cnt2表示	
	for k, v := range reverse {
		if _, ok := config.Groups[k]; !ok {
			extra = append(extra, Status{k, len(v)})
			cnt2 += len(v)
		} else {
			if len(v) > average {
				//extra存储了每个raft组多余的shards数，cnt2存储了总的剩余数
				//这里对于每个raft组的多于数，指的是，大于average+1的那部分
				if left == 0 {
					extra = append(extra, Status{k, len(v) - average})
					cnt2 += len(v) - average
				} else if len(v) == average+1 {
					left--
				} else if len(v) > average+1 {
					extra = append(extra, Status{k, len(v) - average - 1})
					cnt2 += len(v) - average - 1
					left--
				}
			} else if len(v) < average {
				//k相比average还缺少一些，修改lacking和cnt1
				lacking = append(lacking, Status{k, average - len(v)})
				cnt1 += average - len(v)
			}
		}
	}

	// compensation for lacking
//富裕的shards分配的缺少的raft组上。此后的lacking[i]便表示i组最多需要再加lacking[i]个	
	if diff := cnt2 - cnt1; diff > 0 {
		if len(lacking) == 0 {
			cnt := diff
			for k, _ := range config.Groups {
				if len(reverse[k]) == average {
					lacking = append(lacking, Status{k, 0})
					if cnt--; cnt == 0 {
						break
					}
				}
			}
		}
		for i, _ := range lacking {
			lacking[i].count++
			if diff--; diff == 0 {
				break
			}
		}
	}

	// modify reverse
	for len(lacking) != 0 && len(extra) != 0 {
		e, l := extra[0], lacking[0]
		src, dst := e.group, l.group
		if e.count > l.count {
			// move l.count to l
			balance(reverse, src, dst, l.count)
			lacking = lacking[1:]
			extra[0].count -= l.count
		} else if e.count < l.count {
			// move e.count to l
			balance(reverse, src, dst, e.count)
			extra = extra[1:]
			lacking[0].count -= e.count
		} else {
			balance(reverse, src, dst, e.count)
			lacking = lacking[1:]
			extra = extra[1:]
		}
	}

	if len(lacking) != 0 || len(extra) != 0 {
		//DPrintf("extra - lacking: %v <-> %v, %v\n", extra, lacking, config)
		panic("re-balance function bug")
	}

	for k, v := range reverse {
		for _, s := range v {
			config.Shards[s] = k
		}
	}
}

// helper function
func balance(data map[int][]int, src, dst, cnt int) {
	s, d := data[src], data[dst]
	if cnt > len(s) {
		//DPrintf("cnd > len(s): %d <-> %d\n", cnt, len(s))
		panic("Oops...")
	}
	e := s[:cnt]
	d = append(d, e...)
	s = s[cnt:]

	data[src] = s
	data[dst] = d
}

func (sm *ShardMaster) copyConfig(index int, config *Config) {
	if index == -1 || index >= len(sm.configs) {
		index = len(sm.configs) - 1
	}
	config.Num = sm.configs[index].Num
	config.Shards = sm.configs[index].Shards
	config.Groups = make(map[int][]string)
	for k, v := range sm.configs[index].Groups {
		var servers = make([]string, len(v))
		copy(servers, v)
		config.Groups[k] = servers
	}
}

func (sm *ShardMaster) joinConfig(servers map[int][]string) {
	config := Config{}
	sm.copyConfig(-1, &config)
	config.Num++

	// add or update new gid-servers
	for k, v := range servers {
		var servers = make([]string, len(v))
		copy(servers, v)
		config.Groups[k] = servers
	}

	// 2. re-balance
	sm.rebalance(&config)
	sm.configs = append(sm.configs, config)
}

func (sm *ShardMaster) leaveConfig(gids []int) {
	// 1. construct a new configuration
	config := Config{}
	sm.copyConfig(-1, &config)
	config.Num++

	for _, k := range gids {
		delete(config.Groups, k)
	}

	// 2. re-balance
	sm.rebalance(&config)
	sm.configs = append(sm.configs, config)
}

func (sm *ShardMaster) moveConfig(shard int, gid int) {
	config := Config{}
	sm.copyConfig(-1, &config)
	config.Num++
	config.Shards[shard] = gid

	// 2. no need to re-balance
	sm.configs = append(sm.configs, config)
}

func (sm *ShardMaster) apply(op Op) {
	switch op.Op {
		case "Join":
			sm.joinConfig(op.Servers)
		case "Leave":
			sm.leaveConfig(op.GIDs)
		case "Move":
			sm.moveConfig(op.Shard, op.GID)
		case "Query":
			// no need to modify config
		default:
	}
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardMaster {
	sm := new(ShardMaster)
	sm.me = me

	sm.configs = make([]Config, 1)
	sm.configs[0].Groups = map[int][]string{}

	gob.Register(Op{})
	sm.applyCh = make(chan raft.ApplyMsg)
	sm.rf = raft.Make(servers, me, persister, sm.applyCh)
	
	// Your code here.
	sm.order = make(map[int64]int)
	sm.chs = make(map[int64]chan Op)
	
	go func() {
		for {
			applyCh := <- sm.applyCh
			op := applyCh.Command.(Op)		
			clientId := op.ClientID
			reqId := op.SeqNo
			sm.mu.Lock()	
			if reqId > sm.order[clientId] {
				sm.apply(op)
				sm.order[clientId] = reqId
			}
			ch,ok := sm.chs[clientId]
			if !ok {
				ch = make(chan Op,1)
				sm.chs[clientId] = ch
			}		
			select {
				case <-sm.chs[clientId]:
				default:
			}
			sm.chs[clientId] <- op	
			sm.mu.Unlock()
		}
	}()
	return sm
}
