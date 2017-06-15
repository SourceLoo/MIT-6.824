package shardkv


import "shardmaster"
import "labrpc"
import "raft"
import "sync"
import (
	"encoding/gob"
	"log"
	"bytes"
	"time"
)

const Debug = 0

func DPrintln(a ...interface{}) {
	if Debug > 0 {
		log.Println(a...)
	}
	return
}
type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.

	Kind string // 操作类型
	Args interface{}
}

type Result struct {
	kind  string // 操作类型
	args  interface{} // request args
	reply interface{} // reply args
}

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	masters      []*labrpc.ClientEnd
	maxraftstate int                                    // snapshot if log grows this big

							    // Your definitions here.

	cfg          shardmaster.Config                     // sm的配置
	mck          *shardmaster.Clerk                     // sm的客户端

	db           [shardmaster.NShards]map[string]string // 存储信息
	// 数据库格式为 db[shardid] [key] value
	// 保证所有server的db一致

	ack          map[int64]int                          // ack {ckId: reqId} 判重 且顺序化

	logResults   map[int]chan Result                    // kv确定执行op后，将获得结果存入channel中，通知client获取
}



// kv 响应client 的3种请求

// 请求1 Get
func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.

	DPrintln("server", kv.gid, kv.me, "receive unique Get", args)
	index, _, isLeader := kv.rf.Start(Op{Kind: Get, Args: *args})
	if !isLeader {
		reply.WrongLeader = true
		return
	}

	// 如果是第一次，则生成channel，等待kv向其中存入数据
	kv.mu.Lock()
	if _, ok := kv.logResults[index]; !ok {
		kv.logResults[index] = make(chan Result, 1)

	}
	chanMsg := kv.logResults[index]
	kv.mu.Unlock()

	select {
	case msg := <- chanMsg: //  kv 通知了 有数据
		if recArgs, ok := msg.args.(GetArgs); !ok {
			reply.WrongLeader = true
		} else {
			// 判断得到的reply 是否对应发送出去的reply
			if args.CkId != recArgs.CkId || args.ReqId != recArgs.ReqId {
				reply.WrongLeader = true
			} else {
				*reply = msg.reply.(GetReply)
				reply.WrongLeader = false
			}
		}
	case <- time.After(200 * time.Millisecond):
		reply.WrongLeader = true
	}

}

// 请求2 PutAppend
func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {

	DPrintln("server", kv.gid, kv.me, "receive unique PutAppend", args)
	index, _, isLeader := kv.rf.Start(Op{Kind: PutAppend, Args: *args})
	if !isLeader {
		reply.WrongLeader = true
		return
	}

	kv.mu.Lock()
	// 如果是第一次，则生成channel，等待kv向其中存入数据
	if _, ok := kv.logResults[index]; !ok {
		kv.logResults[index] = make(chan Result, 1)

	}
	chanMsg := kv.logResults[index]
	kv.mu.Unlock()

	select {
	case msg := <- chanMsg: //  kv 通知了 有数据
		if recArgs, ok := msg.args.(PutAppendArgs); !ok {
			reply.WrongLeader = true
		} else {
			// 判断得到的reply 是否对应发送出去的reply
			if args.CkId != recArgs.CkId || args.ReqId != recArgs.ReqId {
				reply.WrongLeader = true
			} else {
				reply.Err = msg.reply.(PutAppendReply).Err
				reply.WrongLeader = false
			}
		}
	case <- time.After(200 * time.Millisecond):
		reply.WrongLeader = true
	}
}

// 请求3 特殊请求
// 这个request kv 模拟client发送的
func (kv *ShardKV) BroadcastReconfigure(args ReconfigureArgs) bool {

	// 重复3次
	for i := 0; i < 3; i++ {
		index, _, isLeader := kv.rf.Start(Op{Kind:Reconfigure, Args:args})
		if !isLeader {
			return false
		}

		DPrintln("server", kv.gid, kv.me, "sync reconfig:", args)

		// 如果是第一次，则生成channel，等待kv向其中存入数据
		kv.mu.Lock()
		if _, ok := kv.logResults[index]; !ok {
			kv.logResults[index] = make(chan Result, 1)

		}
		chanMsg := kv.logResults[index]
		kv.mu.Unlock()

		select {
		case msg := <- chanMsg:
			// 不需要判断ckId
			if recArgs, ok := msg.args.(ReconfigureArgs); ok {
				if args.Cfg.Num == recArgs.Cfg.Num {
					return true
				}
			}
		case <- time.After(200 * time.Millisecond):
			continue
		}
	}
	return false
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
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, masters []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call gob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	gob.Register(Op{})
	gob.Register(PutAppendArgs{})
	gob.Register(GetArgs{})
	gob.Register(PutAppendReply{})
	gob.Register(GetReply{})
	gob.Register(shardmaster.Config{})
	gob.Register(ReconfigureArgs{})
	gob.Register(ReconfigureReply{})
	gob.Register(TransferArgs{})
	gob.Register(TransferReply{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.masters = masters

	// Your initialization code here.
	kv.mck = shardmaster.MakeClerk(kv.masters) // 生成sm 的client
	kv.logResults = make(map[int]chan Result) // kv 处理op的结果
	kv.applyCh = make(chan raft.ApplyMsg, 1)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	kv.ack = make(map[int64]int)
	for i := 0; i < shardmaster.NShards; i++ { // 对每个shard都存储一个db
		kv.db[i] = make(map[string]string)
	}

	// kv 有两个任务
	go kv.SolveApplyCh() //第一任务是，处理raft协议 commit的请求
	go kv.PollConfig() // 第二个任务是，向sm 获取最新的cfg

	return kv
}





// kv的第一个任务 开始
func (kv *ShardKV) SolveApplyCh() { // 处理applyCh
	for true {
		msg := <- kv.applyCh
		if msg.UseSnapshot { // message中有snapshot，则 更新kv的db
			kv.UseSnapShot(msg.Snapshot)


		} else { // message中没有snapshot，则 执行commit的 op
			var result Result
			request := msg.Command.(Op)
			result.args = request.Args
			result.kind = request.Kind

			result.reply = kv.ExcuteOp(request)

			kv.SendResult(msg.Index, result) // 将处理后的结果 发送给kv的channel，通知客户端可以接受信息了

			kv.CheckSnapshot(msg.Index) // 最后检查是否需要存储snapshot
		}
	}
}

// 当前的message中，使用了snapshot之后，则将snapshot里面的数据读取出来，存入kv的db中
func (kv *ShardKV) UseSnapShot(snapshot []byte) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	var LastIncludedIndex int
	var LastIncludedTerm int // 这里不使用到，但是decode时，需要向其中存入内容

	kv.ack = make(map[int64]int)
	for i := 0; i < shardmaster.NShards; i++ {
		kv.db[i] = make(map[string]string)
	}

	r := bytes.NewBuffer(snapshot)
	d := gob.NewDecoder(r)
	d.Decode(&LastIncludedIndex)
	d.Decode(&LastIncludedTerm)
	d.Decode(&kv.cfg)
	d.Decode(&kv.db)
	d.Decode(&kv.ack)
	DPrintln("server", kv.gid, kv.me, "use snapshot:", kv.cfg, kv.db, kv.ack)
}

// 当前的message中，没有使用了snapshot之后，执行 经过commit的 operation
func (kv *ShardKV) ExcuteOp(request Op) interface{} {
	switch request.Args.(type) {
	case GetArgs:
		return kv.ApplyGet(request.Args.(GetArgs))
	case PutAppendArgs:
		return kv.ApplyPutAppend(request.Args.(PutAppendArgs))
	case ReconfigureArgs:
		return kv.ApplyReconfigure(request.Args.(ReconfigureArgs))
	}
	return nil
}

// 当前的message中，没有使用了snapshot之后，检查执行后，将结果存入logchannel中
func (kv *ShardKV) SendResult(index int, result Result) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if _, ok := kv.logResults[index]; !ok {
		kv.logResults[index] = make(chan Result, 1)
	} else {
		select {
		case <- kv.logResults[index]:
		default:
		}
	}
	kv.logResults[index] <- result
}

// 当前的message中，没有使用了snapshot之后，最后，检查当前是否需要snapshot
func (kv * ShardKV) CheckSnapshot(index int) {
	if kv.maxraftstate != -1 && float64(kv.rf.GetPersistentSize()) > float64(kv.maxraftstate)*0.8 {
		w := new(bytes.Buffer)
		e := gob.NewEncoder(w)
		e.Encode(kv.cfg) // 将config 存入snapshot中
		e.Encode(kv.db)	// 将db 存入snapshot中
		e.Encode(kv.ack)	// 将ack 存入snapshot中
		data := w.Bytes()
		go kv.rf.StartSnapshot(data, index)
	}
}


// 操作1，查询 key
func (kv *ShardKV) ApplyGet(args GetArgs) GetReply {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	/* 流程：

	1. 检查key是否合法
	2. 将key转为shardid，在本地数据库db中，查询

	*/
	var reply GetReply
	if !kv.CheckValidKey(args.Key) {
		reply.Err = ErrWrongGroup
		return reply
	}
	if value, ok := kv.db[key2shard(args.Key)][args.Key]; ok {
		reply.Err = OK
		reply.Value = value
	} else {
		reply.Err = ErrNoKey
	}
	DPrintln("Server", kv.gid, kv.me, "Apply get:", key2shard(args.Key), "->", args.Key, "->", reply.Err, reply.Value)
	return reply
}

// 操作2，增加 或 赋值
func (kv *ShardKV) ApplyPutAppend(args PutAppendArgs) PutAppendReply {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	/* 流程：

	1. 检查key是否合法
	2. 检查key的request是否重复
	3. 将key转为shardid，在本地数据库db中，append  or put
	*/

	var reply PutAppendReply
	if !kv.CheckValidKey(args.Key) {
		reply.Err = ErrWrongGroup
		return reply
	}
	if !kv.CheckDuplicatedReq(args.CkId, args.ReqId) {
		// 赋值
		if args.Op == Put {
			kv.db[key2shard(args.Key)][args.Key] = args.Value
		} else {
			// 增加
			kv.db[key2shard(args.Key)][args.Key] += args.Value
		}
	}
	DPrintln("Server", kv.gid, kv.me, "Apply PutAppend:",
		key2shard(args.Key), "->", args.Key, "->", kv.db[key2shard(args.Key)][args.Key])
	reply.Err = OK
	return reply
}

// 操作3，kv自己 会广播新的config（BroadcastReconfigure），经过raft达成commit后，更新自己cfg
func (kv *ShardKV) ApplyReconfigure(args ReconfigureArgs) ReconfigureReply {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	var reply ReconfigureReply

	// 如果 args中的cfgnum不大于本地的cfgnum，直接舍弃掉

	// 只更新 新的cfg
	/*
	更新的流程：
	1. 将 kv的db全部更新
	2. 将 kv的ack也要更新
	3. 将 cfg更新
	 */

	if args.Cfg.Num > kv.cfg.Num {
		for shardIndex, data := range args.StoreShard {
			for k, v := range data {
				kv.db[shardIndex][k] = v
			}
		}
		for ckId := range args.Ack {
			if _, exist := kv.ack[ckId]; !exist || kv.ack[ckId] < args.Ack[ckId] {
				kv.ack[ckId] = args.Ack[ckId]
			}
		}
		kv.cfg = args.Cfg
		DPrintln("Server", kv.gid, kv.me, "Apply reconfig:", args)
		reply.Err = OK
	}

	return reply
}

// 只在put Append的时候检查是否重复 reqId，在query的时候没有必要检查
func (kv *ShardKV) CheckDuplicatedReq(ckId int64, reqId int) bool {
	if value, ok := kv.ack[ckId]; ok && value >= reqId {
		return true
	}
	kv.ack[ckId] = reqId
	return false
}

// 若当前key，对应的shard，对应的gid，已经不属于当前kv了，不能处理，不合法
func (kv *ShardKV) CheckValidKey(key string) bool {
	shardId := key2shard(key)
	if kv.gid != kv.cfg.Shards[shardId] {
		return false
	}
	return true
}

// 第一个任务结束





// 以下为：kv的第二个任务
func (kv *ShardKV) PollConfig() {

	// 题目 要求kv 每个200ms 向sm寻求 最新的cfg
	for true {
		if _, isLeader := kv.rf.GetState(); isLeader {
			//DPrintln("server", kv.gid, kv.me, "is leader and run poll config")
			latestCfg := kv.mck.Query(-1)


			// 落后的cfg要更新步骤是 one by one
			for i := kv.cfg.Num + 1; i <= latestCfg.Num; i++ {

				args, ok := kv.GetReconfigure(kv.mck.Query(i))

				// 是否成功获取当前cfg
				if !ok {
					break
				}

				// 将获取的cfg广播出去 当成client对kv的request请求
				if !kv.BroadcastReconfigure(args) {
					break
				}
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// 获取最新cfg的同时，要完成shard 数据的转移
func (kv *ShardKV) GetReconfigure(nextCfg shardmaster.Config) (ReconfigureArgs, bool) {

	// 准备reply的数据
	retArgs := ReconfigureArgs{Cfg:nextCfg}
	retArgs.Ack = make(map[int64]int)
	for i := 0; i < shardmaster.NShards; i++ {
		retArgs.StoreShard[i] = make(map[string]string)
	}
	retOk := true

	// kv的gid是固定的：
	// 在当前cfg中，他拥有一部分shardid；1，3，4
	// 在next cfg中，他也拥有一部分 shardid 3，5
	// 要将不同的部分转移，即将 旧的1 4 转移到新的
	transShards := make(map[int][]int)
	for i := 0; i < shardmaster.NShards; i++ {
		if kv.cfg.Shards[i] != kv.gid && nextCfg.Shards[i] == kv.gid {
			gid := kv.cfg.Shards[i]
			if gid != 0 {
				if _, ok := transShards[gid]; !ok {
					transShards[gid] = []int{i}
				} else {
					transShards[gid] = append(transShards[gid], i)
				}
			}
		}
	}

	// 阻塞，等待所有的gid的StoreShard 存入新的kv.gid中
	var ackMutex sync.Mutex
	var wait sync.WaitGroup
	for gid, value := range transShards {	// iterating map
		wait.Add(1)
		go func(gid int, value []int) {
			defer wait.Done()
			var reply TransferReply

			// 发送请求，from gid to kv.gid
			if kv.SendTransferShard(gid, &TransferArgs{ConfigNum:nextCfg.Num, Shards:value}, &reply) {
				ackMutex.Lock()

				for shardIndex, data := range reply.StoreShard {
					for k, v := range data {
						retArgs.StoreShard[shardIndex][k] = v
					}
				}
				for clientId := range reply.Ack {
					if _, exist := retArgs.Ack[clientId]; !exist || retArgs.Ack[clientId] < reply.Ack[clientId] {
						retArgs.Ack[clientId] = reply.Ack[clientId]
						//retArgs.Replies[clientId] = reply.Replies[clientId]
					}
				}
				ackMutex.Unlock()
			} else {
				retOk = false
			}
		} (gid, value)
	}
	wait.Wait()

	DPrintln("server", kv.gid, kv.me, "get reconfig:", retArgs, retOk)
	return retArgs, retOk
}

// 当前kv向gid寻求数据
func (kv *ShardKV) SendTransferShard(gid int, args *TransferArgs, reply *TransferReply) bool {
	for _, server := range kv.cfg.Groups[gid] {
		//DPrintln("server", kv.gid, kv.me, "send transfer to:", gid, server)
		srv := kv.make_end(server)
		ok := srv.Call("ShardKV.TransferShard", args, reply)
		if ok {
			//DPrintln("server", kv.gid, kv.me, "receive transfer reply from:", gid, *reply)
			if reply.Err == OK {
				return true
			} else if reply.Err == ErrNotReady {
				return false
			}
		}
	}
	return false
}

// 完成rpc，被索求数据方gid，将数据存入reply
func (kv *ShardKV) TransferShard(args *TransferArgs, reply *TransferReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	// 判断cfg的num新旧，丢弃旧的cfg的request
	if kv.cfg.Num < args.ConfigNum {
		reply.Err = ErrNotReady
		return
	}

	// at that case, the target shards have been released by prior owner
	// 将db与ack存入reply中
	reply.Err = OK
	reply.Ack = make(map[int64]int)
	for i := 0; i < shardmaster.NShards; i++ {
		reply.StoreShard[i] = make(map[string]string)
	}
	for _, shardIndex := range args.Shards {
		for k, v := range kv.db[shardIndex] {
			reply.StoreShard[shardIndex][k] = v
		}
	}

	for clientId := range kv.ack {
		reply.Ack[clientId] = kv.ack[clientId]
	}
}



/*
Part A总结：

shardmaster：处理config，即gid对应多个shardid、也对应多个gid

- client：发送Query、Join、Leave、Move等请求；要求更新config
- server：处理client的4种请求；关键点：join 与 leave后需要rebalance



config的信息有：

    Shards [NShards]int     // shard -> gid 多个shardid 可能对应同一个gid
    Groups map[int][]string // gid -> servers[] 一个gid由多个server构成

---



Part B总结

shardkv：

- client：发送Get、PutAppend请求；
  要求操作db；发送的key，处理流程：先将key转为shardid，由cfg得知gid，再向gid的kvserver发送request

- server：
  任务一：处理client的2种请求，外加kv自身的BroadcastReconfigure请求
  任务二：向sm获取最新的cfg，完成data转移；
    流程：1. GetReconfigure 获得旧的cfg上gid的data；2. BroadcastReconfigure 启动raft，完成commit

 */

