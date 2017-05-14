package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import "sync"
import (
	"labrpc"
	"bytes"
	"encoding/gob"
	"time"
	"math/rand"
	"log"
)


//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

//
// A Go object implementing a single Raft peer.
//


/*
raft 节点的结构
mu 锁
peers 所有节点；peers[me]为当前rf
persister 在磁盘存储的内容，持久化
 */


// 三种状态
const (
	Follower = "Follower"
	Candidate = "Candidate"
	Leader = "Leader"
)

// log entry
type Entry struct {
	Cmd interface{}
	Term int
}

type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

				      // Your data here (2A, 2B, 2C).
				      // Look at the paper's Figure 2 for a description of what
				      // state a Raft server must maintain.

	currentTerm int
	votedFor int // 给谁投了票，他必然认定谁是leader
	log []Entry // entry 列表

	commitIndex int
	lastApplied int

	nextIndex []int
	matchIndex []int


				      // 新增
	state string // 状态
	voteCnt int // 获得选票数

	receiveEntryCh chan bool // 通知 收到Leader的Entry
	grantVoteCh chan bool // 通知 投出选票

	findLargerTermCh chan bool // 通信时，rf作为接受者，对方term大，变为Follower
	receiveMajorityVotesCh chan bool // 通信时，rf作为接受者 Candidate，变为Leader

	commitCh chan bool // commitIndex 有更新

}

// 获取 本地最后一条日志的index
func (rf *Raft) getLastLogIndex() (int) {
	return len(rf.log) - 1
}

// 获取 本地最后一条日志的term
func (rf *Raft) getLastLogTerm() (int) {
	return rf.log[rf.getLastLogIndex()].Term
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).

	rf.mu.Lock()
	defer rf.mu.Unlock()

	term = rf.currentTerm
	isleader = (rf.state == Leader) // 从状态中查看 自己是否是leader

	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	w := new(bytes.Buffer)

	e := gob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log) // 将rf的内容写入w中

	data := w.Bytes()
	rf.persister.SaveRaftState(data) // 再将w持久化
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	// Your code here (2C).
	// Example:
	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	d.Decode(&rf.currentTerm)
	d.Decode(&rf.votedFor)
	d.Decode(&rf.log)

	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
}


//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).

	Term int
	CandidateId int
	LastLogIndex int
	LastLogTerm int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).

	Term int
	VoteGranted bool
}

// 添加AppendEntries 结构
type AppendEntriesArgs struct {
	Term int
	LeaderId int

	PrevLogIndex int
	PrevLogTerm int

	Entries []Entry
	LeaderCommit int
}

// 添加AppendEntriesReply 结构
type AppendEntriesReply struct {
	Term int
	Success bool

	NextIndex int
}

// 添加AppendEntries的 handle method
// heartbeat 1s不超过10次，即周期最小为100ms
// 当old leader失联时，要求5s内选出新的leader（可能需要多轮election），故election timeout与heartbeat 要足够短
// 选election timeout为500ms
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {

	// 原子操作 要求rf处理完 这次AppendEntries request
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	DPrintf("term %d, id %d, %s ---> entries term %d, id %d, %s\n", args.Term, args.LeaderId, Leader, rf.currentTerm, rf.me, rf.state)
	DPrintf("len(args.entries) %d, args.PrevLogIndex %d, args.PrevLogTerm %d", len(args.Entries), args.PrevLogIndex, args.PrevLogTerm)

	// 交换 term
	reply.Term = rf.currentTerm
	reply.Success = false

	// 对方小，拒绝
	if args.Term < rf.currentTerm {

		//leader 会变Follower，nextIndex 没必要更新
		return
	}


	// 若本地log 短于 Leader
	if rf.getLastLogIndex() < args.PrevLogIndex{

		// 简单处理 即减一
		reply.NextIndex = rf.getLastLogIndex() + 1


	} else {
		reply.NextIndex = args.PrevLogIndex
		// 本地log 不短于 leader


		// 匹配，先删除，再覆盖本地 args.PrevLogIndex 之后的entries
		if rf.log[args.PrevLogIndex].Term == args.PrevLogTerm {

			// leader nextIndex 为 leader.getLastLogIndex() + 1

			//重点注意 删除 rf 之后的log 只保留到 preLogIndex
			rf.log = rf.log[:args.PrevLogIndex + 1]
			rf.log = append(rf.log, args.Entries...)

			// 注意 先 覆盖，再确定
			reply.NextIndex = rf.getLastLogIndex() + 1
			reply.Success = true

			DPrintf("term %d, id %d, %s +++++ append %d entries, log.index %d, log.term %d\n", rf.currentTerm, rf.me, rf.state, len(args.Entries), rf.getLastLogIndex(), rf.getLastLogTerm())



			// 更新 本地commitIndex
			if args.LeaderCommit > rf.commitIndex {

				if args.LeaderCommit <= rf.getLastLogIndex(){
					rf.commitIndex = args.LeaderCommit
				} else {
					rf.commitIndex = rf.getLastLogIndex()
				}

				DPrintf("term %d, id %d, %s(NotLeader) updateCommitIndex to %d\n", rf.currentTerm, rf.me, rf.state, rf.commitIndex)
				rf.mu.Unlock()
				rf.commitCh <- true
				rf.mu.Lock()
			}

		} else { // 在rf中 从后往前找到一个 log[i].term 不等于rf.log[args.PrevLogIndex].term的

			for i := args.PrevLogIndex - 1; i >= 0; i-- {
				if rf.log[i].Term != rf.log[args.PrevLogIndex].Term {
					reply.NextIndex = i + 1
					break
				}
			}

		}


		/*else {
            //重点注意 删除 rf 之后的log 只保留到 preLogIndex之前
            rf.log = rf.log[:args.PrevLogIndex]
        }*/
	}




	// 上面是 log replication 的操作

	// 下面是 心跳包的状态，reply.success = true
	switch rf.state {
	case Follower:
		if args.Term > rf.currentTerm {
			rf.findLargerTerm(args.Term)

		} else {
			rf.receiveEntry()
		}

	case Candidate:
		if args.Term > rf.currentTerm {
			rf.findLargerTerm(args.Term)

		} else {
			rf.receiveEntry()
		}

	case Leader:
		if args.Term > rf.currentTerm {
			rf.findLargerTerm(args.Term)

		} else { // 同一个Term 不可能出现两个Leader
			log.Fatalf("more than two leaders with the same term")
			//rf.receiveEntry()
		}

	}

}

//
// example RequestVote RPC handler.
// 在type数据类型上定义函数，作用相等于class的method
/*

rf 接受RequestVoteArgs，对RequestVoteReply进行赋值
 这是一个原子操作，不能再细分，全部上锁
 不能对一个投票后，又对另一个投票

 */
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).

	// 原子操作 要求rf处理完 这次 Vote request 操作
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	DPrintf("term %d, id %d, %s ---> requestVote term %d, id %d, %s\n", args.Term, args.CandidateId, Candidate, rf.currentTerm, rf.me, rf.state)

	// 交换 term
	reply.Term = rf.currentTerm
	reply.VoteGranted = false

	// 对方小，拒绝
	if args.Term < rf.currentTerm {

		DPrintf("term %d, id %d, %s is refused requestvote by term %d, id %d, %s, for smaller term\n", args.Term, args.CandidateId, Candidate, rf.currentTerm, rf.me, rf.state)
		return
	}

	grantVote := false
	findLargerTerm := false


	// 首先判断 对方 term是否大，注意更新votedFor
	if args.Term > rf.currentTerm {

		findLargerTerm = true

		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.state = Follower
	}

	// 然后判断 是否应该投票
	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) &&  ((args.LastLogTerm > rf.getLastLogTerm() ) || (args.LastLogTerm == rf.getLastLogTerm() && args.LastLogIndex >= rf.getLastLogIndex())){
		grantVote = true
		reply.VoteGranted = true

		rf.votedFor = args.CandidateId
		rf.state = Follower
	} else {

		DPrintf("term %d, id %d, %s is refused requestvote by term %d, id %d, %s, for less up-to-date\n", args.Term, args.CandidateId, Candidate, rf.currentTerm, rf.me, rf.state)
		return
	}

	if grantVote && findLargerTerm { // 只通知一次 作为发现更大 term 投票 适用于 FCL


		DPrintf("term %d, id %d, %s(FCL) end work, found larger term and granted vote\n", rf.currentTerm, rf.me, rf.state)

		rf.mu.Unlock()
		rf.findLargerTermCh <- true
		rf.mu.Lock()

	} else if grantVote {// 只适用于Follower

		DPrintf("term %d, id %d, %s(Follower) end work, granted vote\n", rf.currentTerm, rf.me, rf.state)
		rf.mu.Unlock()
		rf.grantVoteCh <- true
		rf.mu.Lock()

	} else if findLargerTerm { // 只适用于Follower

		DPrintf("term %d, id %d, %s(Follower) end work, found larger term \n", rf.currentTerm, rf.me, rf.state)
		rf.mu.Unlock()
		rf.findLargerTermCh <- true
		rf.mu.Lock()

	}
}


// 当前rf 向 第server 位 发送AppendEntries 请求
func (rf *Raft)  sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)

	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	// 如果 不再是发送Request时的Leader 这里的回应没有意义了
	if !ok || rf.currentTerm != args.Term || rf.state != Leader{
		return ok
	}

	// 收到response，更新 term 适用L
	if reply.Term > rf.currentTerm { // 对方 term大
		rf.findLargerTerm(reply.Term)

	} else {
		if reply.NextIndex <= 1 {
			reply.NextIndex = 1
		}

		if reply.Success { // server 接受了全部的entries
			DPrintf("term %d, id %d, %s ---> entries term success. id %d, leaderCommit %d, prelogindex %d, prelogterm, %d, reply.NextIndex %d", rf.currentTerm, rf.me, rf.state, server, args.LeaderCommit, args.PrevLogIndex, args.PrevLogTerm, reply.NextIndex)


			// 不能这样，场景：leader发送了AppendEntries，但是过了许久才收到response，这是leader log已经变化。
			rf.nextIndex[server] = reply.NextIndex
			rf.matchIndex[server] = reply.NextIndex - 1


		} else {    // 拒绝了
			// 可能会慢点 但是直观
			DPrintf("term %d, id %d, %s ---> entries term failed. id %d, leaderCommit %d, prelogindex %d, prelogterm, %d, reply.NextIndex %d", rf.currentTerm, rf.me, rf.state, server, args.LeaderCommit, args.PrevLogIndex, args.PrevLogTerm, reply.NextIndex)
			rf.nextIndex[server] = reply.NextIndex
			//rf.nextIndex[server] --
		}
	}
	return ok
}


//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
// 当前rf向第server位的rf发送 RequestVote请求
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)

	// 原子操作 接受response 处理完
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	// 如果 不再是发送Request的Candidate 这里的选票没有意义了
	if !ok ||rf.currentTerm != args.Term || rf.state != Candidate {
		return ok
	}

	// 收到response，// 对方 term大 即 reply.VoteGranted = false
	if reply.Term > rf.currentTerm {
		rf.findLargerTerm(reply.Term)

	} else {

		if reply.VoteGranted {// 收到投票
			rf.voteCnt ++

			// 成为leader
			if rf.voteCnt * 2 > len(rf.peers) {

				rf.state = Leader

				// 初始化
				for i := 0; i < len(rf.peers);  i++{
					rf.nextIndex[i] = rf.getLastLogIndex() + 1
					rf.matchIndex[i] = 0
				}

				rf.getMajorityVotes()
			}
		}
	}

	return ok
}


//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//

/*
leader 收到 client的 一条命令，复制到本地log
 */
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	if rf.state != Leader {
		isLeader = false
		return index, term, isLeader
	}

	index = rf.getLastLogIndex() + 1 // 新来 命令应该存放的下标
	term = rf.currentTerm

	rf.log = append(rf.log, Entry{Term:term, Cmd:command}) // 增加一条log
	DPrintf("term %d, id %d, %s +++++ append one entry, log.index %d, log.term %d\n", rf.currentTerm, rf.me, rf.state, index, term)

	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
}

/*
leader 的 心跳为周期 100ms，频率 10/s
election timeout 选举超时得比这个大很多，600ms + rand(200ms)
    否则：当A成为leader时，A的心跳包还没到Follower的B，B就成为Candidate了。
 */

// 在通信后，收到的Request term小；收到的Response term小； 都变为Follower
func (rf *Raft) findLargerTerm(term int){

	// 更新 currentTerm votedFor
	rf.currentTerm = term
	rf.votedFor = -1

	rf.state = Follower
	//rf.persist()

	DPrintf("term %d, id %d, %s(Follower) end work, found larger term\n", rf.currentTerm, rf.me, rf.state)

	rf.mu.Unlock()
	rf.findLargerTermCh <- true
	rf.mu.Lock()

}


// 以下三个函数 只有3个功能：修改状态、打印信息、修改Channel
func (rf *Raft) receiveEntry(){
	//rf.votedFor = args.LeaderId // 认为对方是leader
	rf.state = Follower
	//rf.persist()

	DPrintf("term %d, id %d, %s(Follower) end work, received Entry\n", rf.currentTerm, rf.me, rf.state)

	rf.mu.Unlock()
	rf.receiveEntryCh <- true
	rf.mu.Lock()
}

/*func (rf *Raft) grantVote(){

    rf.state = Follower
    //rf.persist()

    // 同意投票
    DPrintf("term %d, id %d, %s(Follower) end work, granted Vote\n", rf.currentTerm, rf.me, rf.state)

    rf.mu.Unlock()
    rf.grantVoteCh <- true // 决定投票 通知rf 变为 follower
    rf.mu.Lock()
}*/

func (rf *Raft) getMajorityVotes(){

	// 变为leader
	rf.state = Leader
	//rf.persist()

	DPrintf("term %d, id %d, %s(Leader) end work, get majority votes\n", rf.currentTerm, rf.me, rf.state)

	rf.mu.Unlock()
	rf.receiveMajorityVotesCh <- true
	rf.mu.Lock()
}

func electionTimeout() time.Duration {
	return time.Duration(400 + rand.Intn(200)) * time.Millisecond
	//return time.Duration(800 + rand.Intn(400)) * time.Millisecond
}


// 这3个函数，遵守论文中的rules for server
func (rf *Raft) workAsFollower() {

	// F只接受 request，故只在AppendEntries 与 RequestVote两个函数中接受channel
	select {


	// 接受 request 对方大
	case <- rf.findLargerTermCh:

	// 接受 request 收到 AppendEntries合法通知，继续保持Follower
	case <- rf.receiveEntryCh:

	// 接受 request 投出选票的合法通知，继续保持Follower
	case <- rf.grantVoteCh:

	// 否则 超时 变为Candidate
	case <- time.After(electionTimeout()):
		rf.mu.Lock()
		rf.state = Candidate
		rf.persist()
		DPrintf("term %d, id %d, %s(Candidate) end work, election timeout\n", rf.currentTerm, rf.me, rf.state)
		rf.mu.Unlock()
	}
}

func (rf *Raft) broadcastRequestVote() {
	// 原子操作，要求 分发完所有 Request Vote
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	if rf.state != Candidate {
		return
	}

	// 只有当前是 Candidate，才有资格开始选取


	// 自身更新
	rf.currentTerm ++
	rf.votedFor = rf.me
	rf.voteCnt = 1


	// 构造 RequestVoteArgs
	var args RequestVoteArgs

	args.Term = rf.currentTerm
	args.CandidateId = rf.me
	args.LastLogIndex = rf.getLastLogIndex()
	args.LastLogTerm = rf.getLastLogTerm()

	DPrintf("term %d, id %d, %s broadcast RequestVote\n", rf.currentTerm, rf.me, rf.state)

	for i := 0; i < len(rf.peers); i++{

		// 过滤自己的选票
		if i == rf.me{
			continue
		}

		var reply  RequestVoteReply
		// Candidate goroutine 寻求选票，（此goroutine 与 workAsCandidate 这个主程序要相互通信 ）
		go rf.sendRequestVote(i, &args, &reply)

	}
}

func (rf *Raft) workAsCandidate() {


	rf.broadcastRequestVote()

	// C接受 request
	// C接受 response
	select {

	// 接受 request 对方大
	case <- rf.findLargerTermCh:

	// 接受 request 收到 AppendEntries合法通知，继续变为Follower
	case <- rf.receiveEntryCh:

	// 接受 Response 成为Leader
	case <- rf.receiveMajorityVotesCh:

	// 超时 保持Candidate 开始新的选举
	case <- time.After(electionTimeout()):
		rf.mu.Lock()
		DPrintf("term %d, id %d, %s(Candidate) end work, and will beigin the next election\n", rf.currentTerm, rf.me, rf.state)
		rf.mu.Unlock()
	}

}

func (rf *Raft) updateCommitIndex() { // 论文figure2的最后一条规则

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != Leader {
		return
	}

	N := rf.commitIndex

	// 从后往前
	for N = rf.getLastLogIndex(); N > rf.commitIndex; N-- {

		cnt := 1
		for i := 0; i < len(rf.peers); i++ {
			DPrintf("i %d, matchIndex %d, N %d, log[N].term %d\n", i, rf.matchIndex[i], N, rf.log[N].Term)
			if i != rf.me && rf.matchIndex[i] >= N && rf.log[N].Term == rf.currentTerm {
				cnt ++
			}
		}
		if cnt * 2 > len(rf.peers) {
			break
		}

	}

	if N > rf.commitIndex {
		rf.commitIndex = N

		rf.mu.Unlock()
		DPrintf("term %d, id %d, %s(Leader) updateCommitIndex to %d\n", rf.currentTerm, rf.me, rf.state, rf.commitIndex)
		rf.commitCh <- true
		rf.mu.Lock()
	}
}

func (rf *Raft) broadcastAppendEntries() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != Leader {
		return
	}

	// 原子操作，要求 分发完所有 AppendEntries

	DPrintf("term %d, id %d, %s broadcast AppendEntries\n", rf.currentTerm, rf.me, rf.state)
	for i := 0; i < len(rf.peers); i++ {

		if i == rf.me {
			continue
		}

		// 构造 AppendEntriesArgs
		var args AppendEntriesArgs
		args.Term = rf.currentTerm
		args.LeaderId = rf.me

		// 要复制的是 entries范围 [nextIndex:]
		// 实际上 当 nextIndex[i] > rf.getLastLogIndex()时，nextIndex[i]是等于 rf.getLastLogIndex()+1 的，即entries = nil
		args.PrevLogIndex = rf.nextIndex[i] - 1
		args.PrevLogTerm = rf.log[args.PrevLogIndex].Term

		args.Entries = make([]Entry, rf.getLastLogIndex() - rf.nextIndex[i] + 1)
		copy(args.Entries, rf.log[rf.nextIndex[i]: ]) // 不能直接相等，因为那是引用赋值，args.Entries 改变会影响rf改变

		args.LeaderCommit = rf.commitIndex


		var reply AppendEntriesReply
		go rf.sendAppendEntries(i, &args, &reply)

	}

}

func (rf *Raft) workAsLeader() {

	rf.updateCommitIndex()

	rf.broadcastAppendEntries()

	// C接受 request
	// C接受 response
	select {
	// 接受 request 对方大
	case <- rf.findLargerTermCh:

	// 这是一个循环，下一个循环 Leader再次发送AppendEntries 心跳 周期为100ms 频率10/s
	case <- time.After(time.Duration(100) * time.Millisecond):
		rf.mu.Lock()
		DPrintf("term %d, id %d, %s(Leader) end work, and will begin the next AppendEntries\n", rf.currentTerm, rf.me, rf.state)
		rf.mu.Unlock()
	}
}


func (rf *Raft) init(){

	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = append(rf.log, Entry{Term:0}) // 标志位 方便取得 lastLogTerm，已经 commit 与 apply 了

	rf.commitIndex = 0
	rf.lastApplied = 0

	rf.nextIndex = make([] int, len(rf.peers))
	rf.matchIndex = make([] int, len(rf.peers))

	// 初始是 Follower
	rf.state = Follower

	rf.findLargerTermCh = make(chan bool)
	rf.grantVoteCh = make(chan bool)
	rf.receiveEntryCh = make(chan bool)

	rf.receiveMajorityVotesCh = make(chan bool)
	rf.commitCh = make(chan bool)
}


//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//

/*
make 创建一个raft节点：
 */
func Make(peers []*labrpc.ClientEnd, me int,
persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{} //生成一个rf节点
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.init()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())



	DPrintf("begin work")

	// rf 开始工作
	go func() {
		for  { // 循环查看rf当前状态
			rf.mu.Lock()
			state := rf.state
			rf.mu.Unlock()

			switch state {
			// 注意 判定一个状态后，只有完全执行完一个状态workAs动作 才会进入下一次；
			// 注意 进入workAs方法，有可能state已经改变
			case Follower:
				rf.mu.Lock()
				DPrintf("term %d, id %d, %s begin work\n", rf.currentTerm, rf.me, rf.state)
				rf.mu.Unlock()

				rf.workAsFollower()

			case Candidate:
				rf.mu.Lock()
				DPrintf("term %d, id %d, %s begin work\n", rf.currentTerm, rf.me, rf.state)
				rf.mu.Unlock()

				rf.workAsCandidate()

			case Leader:
				rf.mu.Lock()
				DPrintf("term %d, id %d, %s begin work\n", rf.currentTerm, rf.me, rf.state)
				rf.mu.Unlock()

				rf.workAsLeader()

			}
		}
	}()

	// apply log to RSM
	go func() {
		for {

			<- rf.commitCh
			rf.mu.Lock()

			for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
				msg := ApplyMsg{Index:i, Command:rf.log[i].Cmd}

				DPrintf("term %d, id %d, %s ++++ apply log[%d] log.term %d\n", rf.currentTerm, rf.me, rf.state, i, rf.log[i].Term)

				// 注意这里解锁  通信都考虑解锁
				// 因为不知道buffer有多大，所以建议还是解锁
				rf.mu.Unlock()
				applyCh <- msg
				rf.mu.Lock()

			}
			rf.lastApplied = rf.commitIndex
			rf.mu.Unlock()
		}
	}()


	return rf
}


/* 注意 读写raft成员的时候 都得加锁
// 看日志

// 锁的粒度 太大，在通信时，容易死锁
A 上锁，B上锁，
A sendRequest，等B解锁
B sendRequest，等A解锁
死锁
*/

/*
加锁 的 机制，当时简单的操作时（这之间不能产生goroutine），可以加锁

比如：main()中有 q <- 1 但之前必须有 go func(){<- q}()，channel 生产与消费都是互相阻塞与通知的

rf.findLargerTerm(args.Term) 收到request 或 response的处理，变为Follower 适用于FCL
 */

/*
思考题：若一个选票 过了一段时间重新出现，Candidate已经轮换了2次，Candidate还考虑这个票吗（这个response的term 必然小于本地，直接舍弃）
思考题：若一个Candidate获得足够多了票，称为leader，但是过一段时间又收到一个票是Larger Term，怎么办（不可能出现这个现象）
 */

/*

在一个go routine，不能同时发送两个channel
所以：
AppendEntries中：rf.findLargerTermCh 与 rf.receiveEntryCh 互斥
RequestVote中：rf.findLargerTermCh 与 rf.grantVoteCh 互斥
sendRequestVote中：rf.findLargerTermCh 与 rf.receiveMajorityVotesCh 互斥
 */

/*
死锁样例：
1 是 旧Leade，给Leader0发 AppendEntries，收到response后，变为Follower，正准备进入下一回合state（需上锁）
此时，0 给 1 发 entries，1响应，收到entries，rf.ReceivedEntriesCh <- true，阻塞。未解锁。
总结：mu.lock unlock之间 尽量不要发生Channel操作
 */

/*
2B错误场景再现：
在RequestVote中，rf状态改变后，正准备通知rf.findLargerTerm时，超时了。。
详见2B.old日志 106522行，如何避免？
 */

/*
RequestVote、AppendEntries、sendRequestVote、sendAppendEntries 之间是互斥的
 */