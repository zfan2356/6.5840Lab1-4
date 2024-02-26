package raft

type Entry struct {
	Index   int
	Term    int
	Command interface{}
}

// appendNewEntry leader在自己的日志上新增一条日志
func (rf *Raft) appendNewEntry(command interface{}) Entry {
	lastLog := rf.getLastLog()
	newLog := Entry{
		Index:   lastLog.Index + 1,
		Term:    rf.currentTerm,
		Command: command,
	}
	rf.logs = append(rf.logs, newLog)
	rf.matchIndex[rf.me], rf.nextIndex[rf.me] = newLog.Index, newLog.Index+1
	rf.persist()
	return newLog
}

// AppendEntries RPC handler, 广播心跳处理函数, 同时执行复制功能, 节点接收args, 返回一个reply
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	defer DPrintf("{Node %v}'s state is {state %v,term %v,commitIndex %v,lastApplied %v,firstLog %v,lastLog %v} "+
		"before processing AppendEntriesRequest %v and reply AppendEntriesResponse %v",
		rf.me, rf.state, rf.currentTerm, rf.commitIndex, rf.lastApplied, rf.getFirstLog(), rf.getLastLog(), args, reply)

	// 节点不会接收比自己term小的leader的更新信息
	if args.Term < rf.currentTerm {
		reply.Term, reply.Success = rf.currentTerm, false
		return
	}
	// 广播心跳会更新term信息, 也可以顺便将votefor清空下
	if args.Term > rf.currentTerm {
		rf.currentTerm, rf.voteFor = args.Term, -1
	}
	rf.changeState(StateFollower)
	rf.electionTimer.Reset(RandomizedElectionTimeout())
	if args.PrevLogIndex < rf.getFirstLog().Index {
		// 缓存在快照中, 但是在之前已经被特判过, 所以应该警告一下, 然后返回
		reply.Term, reply.Success = 0, false
		DPrintf("{Node %v} receives unexpected AppendEntriesRequest %v from {Node %v} because prevLogIndex %v < firstLogIndex %v",
			rf.me, args, args.LeaderId, args.PrevLogIndex, rf.getFirstLog().Index)
		return
	}
	// 传入的匹配index有冲突, 补全冲突细节
	if !rf.matchLog(args.PrevLogTerm, args.PrevLogIndex) {
		reply.Term, reply.Success = rf.currentTerm, false
		lastIndex := rf.getLastLog().Index
		if lastIndex < args.PrevLogIndex {
			// 如果广播的要添加的log下标比当前最大的下标要大, 说明之前还有的log没有同步成功
			reply.ConflictTerm, reply.ConflictIndex = -1, lastIndex+1
		} else {
			// 否则就是不匹配（可能是因为网络分区的原因）这个时候我们处理冲突不是从该索引开始，
			// 而是要一直追回到该索引所处的任期的第一个索引
			firstIndex := rf.getFirstLog().Index
			reply.ConflictTerm = rf.logs[args.PrevLogIndex-firstIndex].Term
			index := args.PrevLogIndex - 1
			for index >= firstIndex && rf.logs[index-firstIndex].Term == reply.ConflictTerm {
				index--
			}
			reply.ConflictIndex = index
		}
		return
	}
	// 传来的index没有冲突，但是只限于index之前没有冲突，所以继续检验后面的，如果出现冲突就直接覆盖
	firstIndex := rf.getFirstLog().Index
	for idx, entry := range args.Entries {
		if entry.Index-firstIndex >= len(rf.logs) || rf.logs[entry.Index-firstIndex].Term != entry.Term {
			rf.logs = shrinkEntriesArray(append(rf.logs[:entry.Index-firstIndex], args.Entries[idx:]...))
			break
		}
	}
	rf.advanceCommitIndexForFollower(args.LeaderCommit)
	reply.Term, reply.Success = rf.currentTerm, true

}

// handleAppendEntriesReply leader处理节点appendEntries之后返回的reply, 然后准备提交, 当然也要加锁, 已经加在了replicateOneRound中
func (rf *Raft) handleAppendEntriesReply(peer int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	if rf.state == StateLeader && rf.currentTerm == args.Term {
		if reply.Success {
			// 成功append了日志, 然后计算提交的日志下标, 直接提交即可
			rf.matchIndex[peer] = args.PrevLogIndex + len(args.Entries)
			rf.nextIndex[peer] = rf.matchIndex[peer] + 1
			rf.advanceCommitIndexForLeader()
		} else {
			if reply.Term > rf.currentTerm {
				rf.changeState(StateFollower)
				rf.currentTerm, rf.voteFor = reply.Term, -1
				rf.persist()
			} else if rf.currentTerm == reply.Term {
				// 既然不是term的问题, 说明就是存在conflict
				// 如果返回的Term等于-1, 说明之前有日志没有同步
				rf.nextIndex[peer] = reply.ConflictIndex
				if reply.ConflictTerm != -1 {
					// 退回寻找leader对应冲突term的index
					firstIndex := rf.getFirstLog().Index
					for i := args.PrevLogIndex; i >= firstIndex; i-- {
						if rf.logs[i-firstIndex].Term == reply.ConflictTerm {
							rf.nextIndex[peer] = i + 1
							break
						}
					}
				}
			}
		}
	}
	DPrintf("{Node %v}'s state is {state %v,term %v,commitIndex %v,lastApplied %v,firstLog %v,lastLog %v} after handling AppendEntriesResponse %v for AppendEntriesRequest %v",
		rf.me, rf.state, rf.currentTerm, rf.commitIndex, rf.lastApplied, rf.getFirstLog(), rf.getLastLog(), reply, args)
}
