package raftkv

const (
	OK       = "OK"
	Error = "Error"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.


	// 这两个Id将用来完全区分一个entry
	CkId int64 // client 的id
	ReqId int  // 当前clinet的requestId
}

type PutAppendReply struct {
	WrongLeader bool
	Err         Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.

	// 同上
	// 这两个Id将用来完全区分一个entry
	CkId int64 // client 的id
	ReqId int  // 当前clinet的requestId
}

type GetReply struct {
	WrongLeader bool
	Err         Err
	Value       string
}
