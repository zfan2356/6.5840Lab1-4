package raft

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	// 这里表示快照已经处理到从一开始到index(包括index)的所有指令, 这些指令都存储到了snapshot中
	rf.mu.Lock()
	defer rf.mu.Unlock()
	snapshotIndex := rf.getFirstLog().Index
	if index <= snapshotIndex {
		DPrintf("{Node %v} rejects replacing log with snapshotIndex %v as current snapshotIndex %v is larger in term %v",
			rf.me, index, snapshotIndex, rf.currentTerm)
		return
	}
	rf.logs = shrinkEntriesArray(rf.logs[index-snapshotIndex:])
	rf.logs[0].Command = nil
	rf.persister.Save(rf.encodeState(), snapshot)
	DPrintf("{Node %v}'s state is {state %v,term %v,commitIndex %v,lastApplied %v,firstLog %v,lastLog %v} after replacing log with snapshotIndex %v as old snapshotIndex %v is smaller",
		rf.me, rf.state, rf.currentTerm, rf.commitIndex, rf.lastApplied, rf.getFirstLog(), rf.getLastLog(), index, snapshotIndex)
}

// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("{Node %v} service calls CondInstallSnapshot with lastIncludedTerm %v and lastIncludedIndex %v to check whether snapshot is still valid in term %v",
		rf.me, lastIncludedTerm, lastIncludedIndex, rf.currentTerm)
	if lastIncludedIndex <= rf.commitIndex {
		DPrintf("{Node %v} rejects the snapshot which lastIncludedIndex is %v because commitIndex %v is larger",
			rf.me, lastIncludedIndex, rf.commitIndex)
		return false
	}
	if lastIncludedIndex > rf.getLastLog().Index {
		rf.logs = make([]Entry, 1)
	} else {
		firstIndex := rf.getFirstLog().Index
		rf.logs = shrinkEntriesArray(rf.logs[lastIncludedIndex-firstIndex:])
		rf.logs[0].Command = nil
	}
	rf.logs[0].Term, rf.logs[0].Index = lastIncludedTerm, lastIncludedIndex
	rf.lastApplied, rf.commitIndex = lastIncludedIndex, lastIncludedIndex

	rf.persister.Save(rf.encodeState(), snapshot)
	return true
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer DPrintf("{Node %v}'s state is {state %v,term %v,commitIndex %v,lastApplied %v,firstLog %v,lastLog %v} before processing InstallSnapshotRequest %v and reply InstallSnapshotResponse %v",
		rf.me, rf.state, rf.currentTerm, rf.commitIndex, rf.lastApplied, rf.getFirstLog(), rf.getLastLog(), args, reply)

	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		return
	}
	if args.Term > rf.currentTerm {
		rf.currentTerm, rf.voteFor = args.Term, -1
		rf.persist()
	}

	rf.changeState(StateFollower)
	rf.electionTimer.Reset(RandomizedElectionTimeout())

	if args.LastIncludedIndex <= rf.commitIndex {
		return
	}

	go func() {
		rf.applyCh <- ApplyMsg{
			SnapshotValid: true,
			Snapshot:      args.Data,
			SnapshotTerm:  args.LastIncludedTerm,
			SnapshotIndex: args.LastIncludedIndex,
		}
	}()
}

func (rf *Raft) handleInstallSnapshotReply(peer int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	if rf.state == StateLeader && rf.currentTerm == args.Term {
		if reply.Term > rf.currentTerm {
			rf.changeState(StateFollower)
			rf.currentTerm, rf.voteFor = reply.Term, -1
			rf.persist()
		} else {
			rf.matchIndex[peer], rf.nextIndex[peer] = args.LastIncludedIndex, args.LastIncludedIndex+1
		}
	}
	DPrintf("{Node %v}'s state is {state %v,term %v,commitIndex %v,lastApplied %v,firstLog %v,lastLog %v} after handling InstallSnapshotResponse %v for InstallSnapshotRequest %v",
		rf.me, rf.state, rf.currentTerm, rf.commitIndex, rf.lastApplied, rf.getFirstLog(), rf.getLastLog(), args, reply)
}
