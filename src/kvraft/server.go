package raftkv

import (
	"encoding/gob"
	"labrpc"
	"log"
	"raft"
	"sync"
	"time"
)

const Debug = 1

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}


// Op 即 传给 raft的 log entry
type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.

	// 类似 args
	Kind string // Get Put Append
	Key string
	Value string

	CkId int64 // clerk Id
	ReqId int // request Id

	KvId int // 响应的client 的 leader的Id
}

type RaftKV struct {
	mu      sync.Mutex
	me      int	// 自己序号
	rf      *raft.Raft	// 自己的另一重角色
	applyCh chan raft.ApplyMsg	// 通信

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.

	db map[string]string // 即本地的kv database

	ack map[int64]int // ack {ckId: reqId} 判重 且顺序化

	logResults map[int]chan string // 每次执行完的结构 logResults {logIndex: result}
}


// 当前leader响应，将client的发来的东西存入本地log
func (kv *RaftKV) AppendEntryToLog(entry Op) (bool, string){

	index, _, isLeader := kv.rf.Start(entry)
	if !isLeader{
		return false, "NoLeader"
	}

	DPrintf("client -> server: Kind %s, key %s, value %s, ckid %d, reqid %d, kvid %d\n", entry.Kind, entry.Key, entry.Value, entry.CkId, entry.ReqId, entry.KvId)

	kv.mu.Lock()
	kv.logResults[index] = make(chan string, 1) // 设置为1 允许异步
	DPrintf("kv %d index %d\n", kv.me, index)
	kv.mu.Unlock()

	var result string
	select {
	case result = <- kv.logResults[index]:
		DPrintf("kv %d index %d consume\n", kv.me, index)

	case <- time.After(1 * time.Second):
		DPrintf("kv %d index %d timeout\n", kv.me, index)
		return false, "Timeout"
	}
	return true, result
}

func (kv *RaftKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.

	entry := Op{Kind:"Get", Key:args.Key, CkId:args.CkId, ReqId:args.ReqId, KvId:kv.me}
	ok, result := kv.AppendEntryToLog(entry)

	reply.WrongLeader = !ok
	reply.Value = result
}

func (kv *RaftKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.

	entry := Op{Kind:args.Op, Key:args.Key, Value:args.Value, CkId:args.CkId, ReqId:args.ReqId, KvId:kv.me}
	ok, _ := kv.AppendEntryToLog(entry)

	reply.WrongLeader = !ok
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

	// You may need initialization code here.

	kv.db = make(map[string]string)
	kv.ack = make(map[int64]int)
	kv.logResults = make(map[int]chan string)

	// 这个结点server是kv，他需要存储db，即RSM；同时他要扮演一个raft，全称完成raft协议
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)	// 开启raft，将kv的applyCh 传入


	// 开始等待并处理raft的applyCh
	go func() {
		// 死循环
		for {
			msg := <- kv.applyCh // 等待raft的applyCh

			// 使用了snapshot
			if msg.UseSnapshot {

			} else {

				op := msg.Command.(Op) // 将cmd类型转换为Op

				kv.mu.Lock()

				// 判重，同时顺序化

				var result string

				reqId, ok := kv.ack[op.CkId]
				if !(ok && reqId >= op.ReqId){ // 没有重复 执行op
					switch op.Kind {
					case "Get":
						result = kv.db[op.Key]
					case "Put":
						kv.db[op.Key] = op.Value
						result = OK
					case "Append":
						kv.db[op.Key] += op.Value
						result = OK
					}
					kv.ack[op.CkId] = op.ReqId // 更新db 与 ack
					DPrintf("excute, me %d, msg.Index %d, kind %s, ack[ckid] %d, key %s, value %s, result %s\n", me, msg.Index, op.Kind, kv.ack[op.CkId], op.Key, op.Value, kv.db[op.Key])
				} else {
					DPrintf("duplicate, me %d, msg.Index %d, kind %s, ack[ckid] %d, op.ckid %d, op.Reqid %d\n", me, msg.Index, op.Kind, kv.ack[op.CkId], op.CkId, op.ReqId)

					// 重复，只是不执行，但仍然可以获取
					if op.Kind == "Get" {
						result = kv.db[op.Key]
					} else {
						result = OK
					}
				}
				//DPrintf("00 ok %d, me %d, msg.Index %d\n",ok, me, msg.Index)
				_, isLeader := kv.rf.GetState()
				//DPrintf("11 ok %d, me %d, msg.Index %d\n",ok, me, msg.Index)

				if isLeader && op.KvId == kv.me {
					_, ok := kv.logResults[msg.Index]

					DPrintf("0 ok %d, me %d, msg.Index %d\n",ok, me, msg.Index)
					if ok { // 当前kv在之前接受了client的请求，且为logResults[msg.Index] 生成了空间
						kv.logResults[msg.Index] <- result
					}
					// 否则 就是其他的raft结点，不需要对client进行反馈
					DPrintf("1 ok %d, me %d, msg.Index %d\n",ok, me, msg.Index)
				}


				kv.mu.Unlock()
			}



		}
	}()

	// You may need initialization code here.

	return kv
}

/*
客户端请求流程：

client -> kvserver
kvserver -> raft:  Start(op)
raft -> kvserver: applyCh
kvserver : 判重, excute(op)
kvserver -> client
*/


