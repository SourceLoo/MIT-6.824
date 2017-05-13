package raftkv

import "labrpc"
import "crypto/rand"
import (
	"math/big"
	"sync"
)


type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.

	id int64	// client的id
	reqId int  	// 当前clinet的requestId
	mu      sync.Mutex  //对当前client加锁 一个client可能并行发送多个RPC
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.

	ck.id = nrand() //随机生成 因为client会反复上线，不能从0开始增长
	ck.reqId = 0;	//初始化为0
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("RaftKV.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.

	// 构造args
	var args GetArgs

	args.CkId = ck.id
	args.Key = key

	// 更新reqId
	ck.mu.Lock()
	args.ReqId = ck.reqId
	ck.reqId += 1
	ck.mu.Unlock()

	// 不停遍历所有servers，搜索真正的leader
	for {
		for i := 0; i < len(ck.servers); i++{
			var reply GetReply
			ok := ck.servers[i].Call("RaftKV.Get", &args, &reply);

			// 找到 真正的leader 且得到了消息
			if ok && !reply.WrongLeader {
				return reply.Value
			}

			// 否则 就继续查找下去
		}
	}
	//return ""
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("RaftKV.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	// 构造args
	var args PutAppendArgs

	args.CkId = ck.id
	args.Key = key
	args.Value = value
	args.Op = op

	// 更新reqId
	ck.mu.Lock()
	args.ReqId = ck.reqId
	ck.reqId += 1
	ck.mu.Unlock()

	// 不停遍历所有servers，搜索真正的leader
	for {
		for i := 0; i < len(ck.servers); i++{
			var reply PutAppendReply
			ok := ck.servers[i].Call("RaftKV.PutAppend", &args, &reply);

			// 找到 真正的leader 且执行成功
			if ok && !reply.WrongLeader {
				return
			}

			// 否则 就继续查找下去
		}
	}

}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
