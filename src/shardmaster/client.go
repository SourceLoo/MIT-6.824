package shardmaster

//
// Shardmaster clerk.
//

import "labrpc"
import "time"
import "crypto/rand"
import (
	"math/big"
	"sync"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// Your data here.

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
	// Your code here.

	ck.id = nrand() //随机生成 因为client会反复上线，不能从0开始增长
	ck.reqId = 0;	//初始化为0
	return ck
}

func (ck *Clerk) Query(num int) Config {
	args := &QueryArgs{}
	// Your code here.
	args.Num = num // configuration num

	// 构建QueryArgs
	ck.mu.Lock()
	args.CkId = ck.id
	args.ReqId = ck.reqId
	ck.reqId++
	ck.mu.Unlock()

	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply QueryReply
			ok := srv.Call("ShardMaster.Query", args, &reply)
			if ok && reply.WrongLeader == false {
				return reply.Config
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// 将这些 replica group加入
func (ck *Clerk) Join(servers map[int][]string) {
	args := &JoinArgs{}
	// Your code here.
	args.Servers = servers

	// 构建QueryArgs
	ck.mu.Lock()
	args.CkId = ck.id
	args.ReqId = ck.reqId
	ck.reqId++
	ck.mu.Unlock()

	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply JoinReply
			ok := srv.Call("ShardMaster.Join", args, &reply)
			if ok && reply.WrongLeader == false {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// 将这些 replica group 删除
func (ck *Clerk) Leave(gids []int) {
	args := &LeaveArgs{}
	// Your code here.
	args.GIDs = gids

	// 构建QueryArgs
	ck.mu.Lock()
	args.CkId = ck.id
	args.ReqId = ck.reqId
	ck.reqId++
	ck.mu.Unlock()

	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply LeaveReply
			ok := srv.Call("ShardMaster.Leave", args, &reply)
			if ok && reply.WrongLeader == false {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// 当前shard id 分配给 当前gid
func (ck *Clerk) Move(shard int, gid int) {
	args := &MoveArgs{}
	// Your code here.
	args.Shard = shard
	args.GID = gid

	// 构建QueryArgs
	ck.mu.Lock()
	args.CkId = ck.id
	args.ReqId = ck.reqId
	ck.reqId++
	ck.mu.Unlock()

	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply MoveReply
			ok := srv.Call("ShardMaster.Move", args, &reply)
			if ok && reply.WrongLeader == false {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}
