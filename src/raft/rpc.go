package raft

import "fmt"

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int // 选举人的term
	CandidateId  int // 选举人发起投票请求要携带自己的ID
	LastLogIndex int // 最新的log的下标
	LastLogTerm  int // 最新的log的term
}

func (request RequestVoteArgs) String() string {
	return fmt.Sprintf("{Term:%v,CandidateId:%v,LastLogIndex:%v,LastLogTerm:%v}", request.Term, request.CandidateId, request.LastLogIndex, request.LastLogTerm)
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  // 返回的follower的term
	VoteGranted bool // 是否给你投票
}

func (response RequestVoteReply) String() string {
	return fmt.Sprintf("{Term:%v,VoteGranted:%v}", response.Term, response.VoteGranted)
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int // 节点当前最新的log的idx(闭区间)
	PrevLogTerm  int
	LeaderCommit int // leader最新提交的日志index
	Entries      []Entry
}

func (request AppendEntriesArgs) String() string {
	return fmt.Sprintf("{Term:%v,LeaderId:%v,PrevLogIndex:%v,PrevLogTerm:%v,LeaderCommit:%v,Entries:%v}", request.Term, request.LeaderId, request.PrevLogIndex, request.PrevLogTerm, request.LeaderCommit, request.Entries)
}

type AppendEntriesReply struct {
	Term          int
	Success       bool
	ConflictIndex int // 期望的term下的最小的index
	ConflictTerm  int // 期望的term
}

func (response AppendEntriesReply) String() string {
	return fmt.Sprintf("{Term:%v,Success:%v,ConflictIndex:%v,ConflictTerm:%v}", response.Term, response.Success, response.ConflictIndex, response.ConflictTerm)
}

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

func (request InstallSnapshotArgs) String() string {
	return fmt.Sprintf("{Term:%v,LeaderId:%v,LastIncludedIndex:%v,LastIncludedTerm:%v,DataSize:%v}", request.Term, request.LeaderId, request.LastIncludedIndex, request.LastIncludedTerm, len(request.Data))
}

type InstallSnapshotReply struct {
	Term int
}

func (response InstallSnapshotReply) String() string {
	return fmt.Sprintf("{Term:%v}", response.Term)
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
	// entries 复制了 index+1 -> 最后的log
	entries := make([]Entry, len(rf.logs[index+1-firstIndex:]))
	copy(entries, rf.logs[index+1-firstIndex:])
	return &AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: index,
		PrevLogTerm:  rf.logs[index-firstIndex].Term,
		LeaderCommit: rf.commitIndex,
		Entries:      entries,
	}
}
func (rf *Raft) genInstallSnapshotArgs() *InstallSnapshotArgs {
	firstLog := rf.getFirstLog()
	return &InstallSnapshotArgs{
		Term:              rf.currentTerm,
		LeaderId:          rf.me,
		LastIncludedIndex: firstLog.Index,
		LastIncludedTerm:  firstLog.Term,
		Data:              rf.persister.ReadSnapshot(),
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

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	return rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
}
