package raft

// RequestVote RPC handler. 请求投票处理函数, 当前为接收者, 接收候选人发来的args, 返回一个reply
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
	// 如果当前节点的日志比候选者日志要新，那么也直接false
	if !rf.ifLogUpToDate(args.LastLogTerm, args.LastLogIndex) {
		reply.Term, reply.VoteGranted = rf.currentTerm, false
		return
	}
	rf.voteFor = args.CandidateId
	// TODO 这样有助于网络不稳定下的选举问题
	rf.electionTimer.Reset(RandomizedElectionTimeout())
	reply.Term, reply.VoteGranted = rf.currentTerm, true

}

// StartElection 开始选举, 对于除自己外的所有节点开启一个协程, 进行并行投票, 然后统计数据
func (rf *Raft) StartElection() {
	args := rf.genRequestVoteArgs()
	DPrintf("{Node %v} starts election with RequestVoteRequest %v", rf.me, args)

	grantedVotes := 1 // 来自本身的投票
	rf.voteFor = rf.me
	rf.persist()
	for i := range rf.peers {
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
