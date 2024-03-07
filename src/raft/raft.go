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
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
)

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.RWMutex        // Lock to protect shared access to this peer's state
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

	// 以下三条是raft状态机可持久化的状态
	currentTerm int
	voteFor     int
	logs        []Entry

	commitIndex int   // 当前提交到服务器的最大index
	lastApplied int   // 上一次提交至的Index
	nextIndex   []int // 下一次同步的log的Index
	matchIndex  []int // leader当前已经同步的日志进度

	electionTimer  *time.Timer // 心跳计时器, 到时间会广播心跳
	heartbeatTimer *time.Timer // 选举计时器, 如果倒计时过程中没有收到心跳信号就会开始选举
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.RLock()
	defer rf.mu.RUnlock()
	return rf.currentTerm, rf.state == StateLeader
}
func (rf *Raft) getLastLog() Entry {
	return rf.logs[len(rf.logs)-1]
}
func (rf *Raft) getFirstLog() Entry {
	return rf.logs[0]
}

// needReplicating 编号为peer的节点是否需要更新日志 (只有Leader才能让其他节点同步自己的日志)
func (rf *Raft) needReplicating(peer int) bool {
	rf.mu.RLock()
	defer rf.mu.RUnlock()
	return rf.state == StateLeader && rf.matchIndex[peer] < rf.getLastLog().Index
}

// HasLogInCurrentTerm 判断当前的term中是否已经有日志
func (rf *Raft) HasLogInCurrentTerm() bool {
	rf.mu.RLock()
	defer rf.mu.RUnlock()
	return rf.getLastLog().Term == rf.currentTerm
}

// matchLog 传进来的log是否匹配当前rf对应Index的log
func (rf *Raft) matchLog(term, index int) bool {
	return index <= rf.getLastLog().Index && rf.logs[index-rf.getFirstLog().Index].Term == term
}

// ifLogUpToDate 判断当前节点的log是否已经过期, 如果过期返回true (其实和判断log这个序列的字典序一样)
func (rf *Raft) ifLogUpToDate(term, index int) bool {
	lastlog := rf.getLastLog()
	return term > lastlog.Term || (term == lastlog.Term && index >= lastlog.Index)
}

// changeState 节点改变状态, 非原子, 调用时记得写锁
func (rf *Raft) changeState(state NodeState) {
	if rf.state == state {
		return
	}
	// 这里转换state其实是刷新状态, 也有可能出现follower -> follower的情况, 这个时候应该在函数外更新定时器
	DPrintf("{Node %d} changes state from %d to %d in term %d", rf.me, rf.state, state, rf.currentTerm)
	rf.state = state
	switch state {
	case StateFollower:
		rf.heartbeatTimer.Stop()
		rf.electionTimer.Reset(RandomizedElectionTimeout())
	case StateCandidate:
		// TODO 修改: 变成候选者之后election再次开始计时, 广播心跳停止
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

// BroadcastHeartBeat 广播心跳信号, 然后执行复制功能
func (rf *Raft) BroadcastHeartBeat(isHeartBeat bool) {
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		// 这里如果isHeartBeat == true, 说明当前leader会发送心跳信号, 所以启动n-1
		// 个协程来向n-1个节点发送信号
		// 如果为false, 说明主动唤醒进行日志同步, 向n-1个replicator发送信号, 唤醒replicator进行日志同步
		// 所以本质上都是调用replucateOneRound进行日志同步
		if isHeartBeat {
			go rf.replicateOneRound(i)
		} else {
			rf.replicatorCond[i].Signal()
		}
	}
}

// replicateOneRound 当前Leader给编号为peer的节点发送appendentries请求, 同步日志
func (rf *Raft) replicateOneRound(peer int) {
	rf.mu.RLock()
	if rf.state != StateLeader {
		rf.mu.RUnlock()
		return
	}
	// 相当于获得leader所认为的这个follower最新的log的index
	prevLogIndex := rf.nextIndex[peer] - 1
	if prevLogIndex < rf.getFirstLog().Index {
		// TODO 已经被快照缓存下, 所以可以更新peer节点的状态机, 变成快照状态
		args := rf.genInstallSnapshotArgs()
		rf.mu.RUnlock()
		reply := new(InstallSnapshotReply)
		if rf.sendInstallSnapshot(peer, args, reply) {
			rf.mu.Lock()
			rf.handleInstallSnapshotReply(peer, args, reply)
			rf.mu.Unlock()
		}
	} else {
		args := rf.genAppendEntriesArgs(prevLogIndex)
		rf.mu.RUnlock()
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

// Kill 返回raft算法是否结束
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}
func (rf *Raft) killed() bool {
	return atomic.LoadInt32(&rf.dead) == 1
}
func (rf *Raft) Me() int {
	return rf.me
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

// applier 监控日志的应用过程
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
		nextIndex:      make([]int, len(peers)),
		matchIndex:     make([]int, len(peers)),
		electionTimer:  time.NewTimer(RandomizedElectionTimeout()),
		heartbeatTimer: time.NewTimer(StableHeartBeatTimeout()),
	}
	rf.readPersist(persister.ReadRaftState())
	// TODO 新增一句, 因为作为跟随者, 广播应该是useless的
	rf.heartbeatTimer.Stop()
	rf.applyCond = sync.NewCond(&rf.mu)
	lastLog := rf.getLastLog()
	for i := 0; i < len(peers); i++ {
		rf.matchIndex[i], rf.nextIndex[i] = 0, lastLog.Index+1
		if i != rf.me {
			rf.replicatorCond[i] = sync.NewCond(&sync.Mutex{})
			// start replicator goroutine to replicate entries in batch
			go rf.replicator(i)
		}
	}

	// start ticker goroutine to start elections
	go rf.ticker()
	// start applier goroutine to push committed logs into applyCh exactly once
	go rf.applier()

	return rf
}
