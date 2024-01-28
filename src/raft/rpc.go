package raft

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int // 选举人的term
	CandidateId  int // 选举人发起投票请求要携带自己的ID
	LastLogIndex int // 最新的log的下标
	LastLogTerm  int // 最新的log的term
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  // 返回的follower的term
	VoteGranted bool // 是否给你投票
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int // 节点当前最新的log的idx(闭区间)
	PrevLogTerm  int
	LeaderCommit int
	Entries      []Entry
}
type AppendEntriesReply struct {
	Term          int
	Success       bool
	ConflictIndex int // 期望的term下的最小的index
	ConflictTerm  int // 期望的term
}

func (rf *Raft) genRequestVoteArgs() *RequestVoteArgs {
	args := &RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: rf.getLastLog().Index,
		LastLogTerm:  rf.getLastLog().Term,
	}
	return args
}
func (rf *Raft) genAppendEntriesArgs(index int) *AppendEntriesArgs {
	firstIndex := rf.getFirstLog().Index
	entries := make([]Entry, 0)
	entries = append(entries, rf.logs[index+1-firstIndex:]...)
	return &AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: index,
		PrevLogTerm:  rf.logs[index].Term,
		LeaderCommit: rf.commitIndex,
		Entries:      entries,
	}
}

// sendRequestVote me 向 server 发送请求投票的请求, 然后接收一个回复
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// sendAppendEntries me 向 server 发送请求添加日志的请求, 然后接收一个回复
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}
