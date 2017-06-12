package shardmaster


import "raft"
import "labrpc"
import "sync"
import (
	"encoding/gob"
	//"bytes"
	"log"
	"time"
)


const Debug = 1

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type ShardMaster struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.

	configs []Config // indexed by config num
	configNum int // 当前最新的 configureNum

	ack map[int64]int // ack {ckId: reqId} 判重 且顺序化

	logResults map[int]chan string // 每次执行完的结构 logResults {logIndex: result}
}


type Op struct {
	// Your data here.

	Kind      string // 操作类型

	Num     int
	GIDs    []int
	Servers map[int][]string
	Shard   int

	CkId int64 // clerk Id
	ReqId int // request Id
}

// 当前leader响应，将client的发来的东西存入本地log 这个是shard部分的 raft协议，存的是configure信息
func (sm *ShardMaster) AppendEntryToLog(entry Op) (bool, string){

	index, _, isLeader := sm.rf.Start(entry)
	if !isLeader{
		return false, "NoLeader"
	}

	// DPrintf("client -> server: Kind %s, key %s, value %s, ckid %d, reqid %d, kvid %d\n", entry.Kind, entry.Key, entry.Value, entry.CkId, entry.ReqId, entry.KvId)

	sm.mu.Lock()
	sm.logResults[index] = make(chan string, 1) // 设置为1 允许异步
	DPrintf("sm %d index %d\n", sm.me, index)
	sm.mu.Unlock()

	var result string
	select {
	case result = <- sm.logResults[index]:
		DPrintf("sm %d index %d consume\n", sm.me, index)

	case <- time.After(2 * time.Second):
		DPrintf("sm %d index %d timeout\n", sm.me, index)
		return false, "Timeout"
	}
	return true, result
}


func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.

	entry := Op{Kind:"Join", Servers:args.Servers, CkId:args.CkId, ReqId:args.ReqId}

	ok, _ := sm.AppendEntryToLog(entry)

	reply.WrongLeader = !ok

}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.

	entry := Op{Kind:"Leave", GIDs:args.GIDs, CkId:args.CkId, ReqId:args.ReqId}

	ok, _ := sm.AppendEntryToLog(entry)

	reply.WrongLeader = !ok
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.

	entry := Op{Kind:"Move", CkId:args.CkId, ReqId:args.ReqId, Shard:args.Shard}
	entry.GIDs = append(entry.GIDs, args.GID)

	ok, _ := sm.AppendEntryToLog(entry)

	reply.WrongLeader = !ok
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	entry := Op{Kind:"Query", Num:args.Num, CkId:args.CkId, ReqId:args.ReqId}

	ok, _ := sm.AppendEntryToLog(entry)

	reply.WrongLeader = !ok

	if ok {
		reply.WrongLeader = false
		sm.mu.Lock()
		num := entry.Num

		// 获得最新config
		if entry.Num == -1 || entry.Num > sm.configNum {
			num = sm.configNum
		}
		reply.Config = sm.configs[num]
		sm.ack[entry.CkId] = entry.ReqId
		reply.Err = OK
		sm.mu.Unlock()
	}
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

	// You may need initialization code here.

	sm.configNum = 0
	sm.ack = make(map[int64]int)
	sm.logResults = make(map[int]chan string)

	// Your code here.


	// 开始等待并处理raft的applyCh
	go func() {
		// 死循环
		for {
			msg := <- sm.applyCh // 等待raft的applyCh


			if msg.UseSnapshot { // applyCh中有snapshot

			} else {

				op := msg.Command.(Op) // 将cmd类型转换为Op

				sm.mu.Lock()

				// 判重，同时顺序化

				var result string

				reqId, ok := sm.ack[op.CkId]
				if !(ok && reqId >= op.ReqId){ // 没有重复 执行op
					sm.excute(op)
					//DPrintf("excute, me %d, msg.Index %d, kind %s, ack[ckid] %d, key %s, value %s, result %s\n", me, msg.Index, op.Kind, sm.ack[op.CkId], op.Servers, op.Num, kv.db[op.Key])
					DPrintf("excute")
				} else {
					DPrintf("duplicate, me %d, msg.Index %d, kind %s, ack[ckid] %d, op.ckid %d, op.Reqid %d\n", me, msg.Index, op.Kind, sm.ack[op.CkId], op.CkId, op.ReqId)
				}

				_, ok2 := sm.logResults[msg.Index]

				DPrintf("0 ok %d, me %d, msg.Index %d\n",ok, me, msg.Index)
				if ok2 { // 当前kv在之前接受了client的请求，且为logResults[msg.Index] 生成了空间
					sm.logResults[msg.Index] <- result
				}
				// 否则 就是其他的raft结点，不需要对client进行反馈
				DPrintf("1 ok %d, me %d, msg.Index %d\n",ok, me, msg.Index)

				sm.mu.Unlock()
			}

		}
	}()

	return sm
}


func (sm *ShardMaster) excute(op Op) {

	switch op.Kind {
	case "Join":
		config := sm.newConfig()
		for gid, servers := range op.Servers {
			_, ok := config.Groups[gid]
			if !ok {
				config.Groups[gid] = servers
				sm.rebalanceJoin(gid)
			}
		}
	case "Leave":
		config := sm.newConfig()
		for _, gid := range op.GIDs {
			_, ok := config.Groups[gid]
			if ok {
				delete(config.Groups, gid)
				sm.rebalanceLeave(gid)
			}
		}
	case "Move":
		config := sm.newConfig()
		if op.GIDs != nil && len(op.GIDs) > 0 {
			config.Shards[op.Shard] = op.GIDs[0]
		}
	case "Query":
	default:
	}
	sm.ack[op.CkId] = op.ReqId
}

func (sm *ShardMaster) newConfig() *Config {
	old := &sm.configs[sm.configNum]
	new := Config{}
	new.Groups = map[int][]string{}
	new.Num = old.Num + 1
	new.Shards = [NShards]int{}
	for gid, servers := range old.Groups {
		new.Groups[gid] = servers
	}
	for i, gid := range old.Shards {
		new.Shards[i] = gid
	}
	sm.configNum++
	sm.configs = append(sm.configs, new)
	return &sm.configs[sm.configNum]
}


func (sm *ShardMaster) getMaxShardCountGID() int {
	config := &sm.configs[sm.configNum]

	for _, gid := range config.Shards {
		if gid == 0 {
			return 0
		}
	}

	count := map[int]int{}
	max := -1
	result := 0

	for gid := range config.Groups {
		count[gid] = 0
	}

	for _, gid := range config.Shards {
		count[gid]++
	}

	for gid, c := range count {
		_, ok := config.Groups[gid]
		if ok && c > max {
			max, result = c, gid
		}
	}

	return result
}

func (sm *ShardMaster) getMinShardCountGID() int {
	config := &sm.configs[sm.configNum]

	count := map[int]int{}
	min := 257
	result := 0

	for gid := range config.Groups {
		count[gid] = 0
	}

	for _, gid := range config.Shards {
		count[gid]++
	}

	for gid, c := range count {
		_, ok := config.Groups[gid]
		if ok && c < min {
			min, result = c, gid
		}
	}

	return result
}

func (sm *ShardMaster) getOneShardByGID(gid int) int {
	config := &sm.configs[sm.configNum]

	for shard, id := range config.Shards {
		if id == gid {
			return shard
		}
	}

	return -1
}

func (sm *ShardMaster) rebalanceJoin(gid int) {
	config := &sm.configs[sm.configNum]
	i := 0

	for {
		if i == NShards/len(config.Groups) {
			break
		}
		max := sm.getMaxShardCountGID()
		shard := sm.getOneShardByGID(max)
		config.Shards[shard] = gid
		i++
	}
}

func (sm *ShardMaster) rebalanceLeave(gid int) {
	config := &sm.configs[sm.configNum]

	for {
		min := sm.getMinShardCountGID()
		shard := sm.getOneShardByGID(gid)
		if shard == -1 {
			break
		}
		config.Shards[shard] = min
	}
}
