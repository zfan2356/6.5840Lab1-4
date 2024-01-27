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

import (
	"6.5840/labgob"
	"bytes"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
)

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	applyCh        chan ApplyMsg
	applyCond      *sync.Cond
	replicatorCond []*sync.Cond
	state          NodeState

	currentTerm int
	voteFor     int
	logs        []Entry

	commitIndex int   // 当前需要提交到服务器的最大index
	lastApplied int   // 上一次提交至的Index
	nextIndex   []int // 下一次同步的log的Index
	matchIndex  []int // leader当前已经同步的日志进度

	electionTimer  *time.Timer // 心跳计时器, 到时间会广播心跳
	heartbeatTimer *time.Timer // 选举计时器, 如果倒计时过程中没有收到心跳信号就会开始选举
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.state == StateLeader
}
func (rf *Raft) getLastLog() Entry {
	return rf.logs[len(rf.logs)-1]
}
func (rf *Raft) getFirstLog() Entry {
	return rf.logs[0]
}
func (rf Raft) encodeState() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.voteFor)
	e.Encode(rf.logs)
	return w.Bytes()
}

// ifLogUpToDate 判断当前节点的log是否已经过期, 如果过期返回true (其实和判断log这个序列的字典序一样)
func (rf *Raft) ifLogUpToDate(term, index int) bool {
	lastlog := rf.getLastLog()
	return term > lastlog.Term || (term == lastlog.Term && index >= lastlog.Index)
}

// changeState 服务器改变状态
func (rf *Raft) changeState(state NodeState) {
	if rf.state == state {
		return
	}
	DPrintf("{Node %d} changes state from %d to %d in term %d", rf.me, rf.state, state, rf.currentTerm)
	rf.state = state
	switch state {
	case StateFollower:
		rf.heartbeatTimer.Stop()
		rf.electionTimer.Reset(RandomizedElectionTimeout())
	case StateCandidate: // TODO 修改了一下, 按理说变成候选者之后应该再次开始计时, 广播心跳应该停一下
		rf.heartbeatTimer.Stop()
		rf.electionTimer.Reset(RandomizedElectionTimeout())
	case StateLeader:
		lastLog := rf.getLastLog()
		for i := 0; i < len(rf.peers); i++ {
			rf.matchIndex[i], rf.nextIndex[i] = 0, lastLog.Index+1
		}
		rf.electionTimer.Stop()
		rf.heartbeatTimer.Reset(StableHeartBeatTimeout())
	}
}

// RequestVote RPC handler. 请求投票处理函数
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	defer DPrintf("{Node %v}'s state is {state %v,term %v,commitIndex %v,lastApplied %v,firstLog %v,lastLog %v} before processing requestVoteRequest %v and reply requestVoteResponse %v",
		rf.me, rf.state, rf.currentTerm, rf.commitIndex, rf.lastApplied, rf.getFirstLog(), rf.getLastLog(), args, reply)

	// 当候选者的term小于当前节点的term, 直接false, 当候选者的term等于当前的term但是当前节点已经投票给候选者的时候也false
	if args.Term < rf.currentTerm || (args.Term == rf.currentTerm && rf.voteFor != -1 && rf.voteFor != args.CandidateId) {
		reply.Term, reply.VoteGranted = rf.currentTerm, false
		return
	}
	// 当候选者的term大于当前节点的term, 要转化为follower
	if args.Term > rf.currentTerm {
		rf.changeState(StateFollower)
		rf.currentTerm, rf.voteFor = args.Term, -1
	}
	// 如果当前节点的没有过期, 也就是当前节点版本不小于候选人版本, 所以可以返回false
	if !rf.ifLogUpToDate(args.LastLogTerm, args.LastLogIndex) {
		reply.Term, reply.VoteGranted = rf.currentTerm, false
		return
	}
	rf.voteFor = args.CandidateId
	// TODO 去掉了这一段话, 因为候选人投票按理说应该不影响自身计时器
	//rf.electionTimer.Reset(RandomizedElectionTimeout())
	reply.Term, reply.VoteGranted = rf.currentTerm, true

}

// advanceCommitIndexForLeader 根据follower的同步进度, 算出来当前应该提交到服务器的Index
func (rf *Raft) advanceCommitIndexForLeader() {
	n := len(rf.matchIndex)
	srt := make([]int, n)
	copy(srt, rf.matchIndex)
	insertionSort(srt)
	newCommitIndex := srt[n-(n/2+1)] // 找到半数以上
	if newCommitIndex > rf.commitIndex {
		// 切记只能提交当前任期的log
		if rf.matchLog(rf.currentTerm, newCommitIndex) {
			DPrintf("{Node %d} advance commitIndex from %d to %d with matchIndex %v in term %d",
				rf.me, rf.commitIndex, newCommitIndex, rf.matchIndex, rf.currentTerm)
			rf.commitIndex = newCommitIndex
			rf.applyCond.Signal()
		} else {
			DPrintf("{Node %d} can not advance commitIndex from %d because the term of newCommitIndex %d is not equal to currentTerm %d",
				rf.me, rf.commitIndex, newCommitIndex, rf.currentTerm)
		}
	}
}
func (rf *Raft) advanceCommitIndexForFollower(leaderCommit int) {
	newCommitIndex := min(leaderCommit, rf.getLastLog().Index)
	if newCommitIndex > rf.commitIndex {
		DPrintf("{Node %d} advance commitIndex from %d to %d with leaderCommit %d in term %d",
			rf.me, rf.commitIndex, newCommitIndex, leaderCommit, rf.currentTerm)
		rf.commitIndex = newCommitIndex
		rf.applyCond.Signal()
	}
}

func (rf *Raft) StartElection() {
	args := rf.genRequestVoteArgs()
	DPrintf("{Node %v} starts election with RequestVoteRequest %v", rf.me, args)

	grantedVotes := 1 // 来自本身的投票
	rf.voteFor = rf.me
	rf.persist()
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		go func(index int) {
			reply := new(RequestVoteReply)
			if rf.sendRequestVote(index, args, reply) {
				rf.mu.Lock()
				defer rf.mu.Unlock()
				DPrintf("{Node %v} receives RequestVoteResponse %v from {Node %v} "+
					"after sending RequestVoteRequest %v in term %v", rf.me, reply, index, args, rf.currentTerm)

				if rf.currentTerm == args.Term && rf.state == StateCandidate {
					if reply.VoteGranted {
						grantedVotes += 1
						if grantedVotes > len(rf.peers)/2 {
							DPrintf("{Node %v} receives majority votes in term %v", rf.me, rf.currentTerm)
							rf.changeState(StateLeader)
							rf.BroadcastHeartBeat(true)
						}
					} else if reply.Term > rf.currentTerm {
						DPrintf("{Node %v} finds a new leader {Node %v} "+
							"with term %v and steps down in term %v", rf.me, index, reply.Term, rf.currentTerm)
						rf.changeState(StateFollower)
						rf.currentTerm, rf.voteFor = reply.Term, -1
						rf.persist()
					}
				}
			}
		}(i)
	}
}

// BroadcastHeartBeat 广播心跳信号, 然后执行复制功能
func (rf *Raft) BroadcastHeartBeat(isHeartBeat bool) {
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		if isHeartBeat {
			// 广播心跳信号会让跟随者们复制领导者的信息
			go rf.replicateOneRound(i)
		} else {
			// TODO 这里不明白是什么意思, 应该是leader给其他的节点发送信号, 也就是需要更新日志了
			rf.replicatorCond[i].Signal()
		}
	}
}
func (rf *Raft) replicateOneRound(peer int) {
	rf.mu.Lock()
	if rf.state != StateLeader {
		rf.mu.Unlock()
		return
	}
	// 相当于获得leader所认为的这个follower最新的log的index
	prevLogIndex := rf.nextIndex[peer] - 1
	if prevLogIndex < rf.getFirstLog().Index {
		// TODO 已经被快照缓存下
	} else {
		args := rf.genAppendEntriesArgs(prevLogIndex)
		rf.mu.Unlock()
		reply := new(AppendEntriesReply)
		if rf.sendAppendEntries(peer, args, reply) {
			rf.mu.Lock()
			rf.handleAppendEntriesReply(peer, args, reply)
			rf.mu.Unlock()
		}
	}
}

// Start
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != StateLeader {
		return -1, -1, false
	}
	// 这里一定是Leader才会appendNewEntry
	newLog := rf.appendNewEntry(command)
	DPrintf("{Node %v} receives a new command[%v] to replicate in term %v", rf.me, newLog, rf.currentTerm)
	rf.BroadcastHeartBeat(false)
	return newLog.Index, newLog.Term, true
}

func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}
func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// ticker 开启一个协程来监控节点的计时器是否到期
func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here (2A)
		// Check if a leader election should be started.
		select {
		case <-rf.electionTimer.C: // timer的管道, 可以利用管道来监听是否倒计时结束
			rf.mu.Lock()
			rf.changeState(StateCandidate)
			rf.currentTerm += 1
			rf.StartElection()
			rf.electionTimer.Reset(RandomizedElectionTimeout())
			rf.mu.Unlock()
		case <-rf.heartbeatTimer.C:
			rf.mu.Lock()
			if rf.state == StateLeader {
				rf.BroadcastHeartBeat(true)
				rf.heartbeatTimer.Reset(StableHeartBeatTimeout())
			}
			rf.mu.Unlock()
		}
	}
}

// needReplicating 编号为peer的服务器是否需要让其他节点更新日志 (只有Leader才能让其他节点同步自己的日志)
func (rf *Raft) needReplicating(peer int) bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.state == StateLeader && rf.matchIndex[peer] < rf.getLastLog().Index
}

// matchLog 传进来的log是否匹配当前rf对应Index的log
func (rf *Raft) matchLog(term, index int) bool {
	return index <= rf.getLastLog().Index && rf.logs[index-rf.getFirstLog().Index].Term == term
}

// replicator 每个节点又开启n-1个协程, 来监控其他的节点
func (rf *Raft) replicator(peer int) {
	rf.replicatorCond[peer].L.Lock()
	defer rf.replicatorCond[peer].L.Unlock()
	for rf.killed() == false {
		// 如果不需要通知其他节点复制entry, 就等待 所以这里应该是leader的行为
		for !rf.needReplicating(peer) {
			rf.replicatorCond[peer].Wait()
		}
		// 否则就通知一轮复制
		rf.replicateOneRound(peer)
	}
}

// applier 监控日志的提交过程
func (rf *Raft) applier() {
	for rf.killed() == false {
		rf.mu.Lock()
		for rf.lastApplied >= rf.commitIndex {
			rf.applyCond.Wait()
		}
		firstIndex, commitIndex, lastApplied := rf.getFirstLog().Index, rf.commitIndex, rf.lastApplied
		entries := make([]Entry, commitIndex-lastApplied)
		copy(entries, rf.logs[lastApplied+1-firstIndex:commitIndex+1-firstIndex])
		rf.mu.Unlock()

		for _, entry := range entries {
			rf.applyCh <- ApplyMsg{
				CommandValid: true,
				Command:      entry.Command,
				CommandIndex: entry.Index,
				CommandTerm:  entry.Term,
			}
		}
		rf.mu.Lock()
		DPrintf("{Node %v} applies entries %v-%v in term %v",
			rf.me, rf.lastApplied, commitIndex, rf.currentTerm)
		rf.lastApplied = max(rf.lastApplied, commitIndex)
		rf.mu.Unlock()
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	// Your initialization code here (2A, 2B, 2C).
	rf := &Raft{
		mu:             sync.Mutex{},
		peers:          peers,
		persister:      persister,
		me:             me,
		dead:           0,
		applyCh:        applyCh,
		replicatorCond: make([]*sync.Cond, len(peers)),
		state:          StateFollower,
		currentTerm:    0,
		voteFor:        -1,
		logs:           make([]Entry, 1),
		commitIndex:    0,
		lastApplied:    0,
		nextIndex:      make([]int, len(peers)),
		matchIndex:     make([]int, len(peers)),
		electionTimer:  time.NewTimer(RandomizedElectionTimeout()),
		heartbeatTimer: time.NewTimer(StableHeartBeatTimeout()),
	}
	// TODO 新增一句, 因为作为跟随者, 广播应该是useless的
	rf.heartbeatTimer.Stop()
	rf.applyCond = sync.NewCond(&rf.mu)
	lastLog := rf.getLastLog()
	for i := 0; i < len(peers); i++ {
		rf.matchIndex[i], rf.nextIndex[i] = 0, lastLog.Index+1
		if i != rf.me {
			rf.replicatorCond[i] = sync.NewCond(&sync.Mutex{})
			go rf.replicator(i)
		}
	}

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	go rf.applier()

	return rf
}
