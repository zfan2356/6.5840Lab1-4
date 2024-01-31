package raft

import (
	"fmt"
	"log"
	"math/rand"
	"os"
	"sync"
	"time"
)

// Debugging
const Debug = true

func DPrintf(format string, a ...interface{}) {
	if Debug {
		logFile, _ := os.OpenFile("./debug.log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
		log.SetOutput(logFile)
		log.Printf(format, a...)
	}
}

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
	CommandTerm  int
	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

func (msg ApplyMsg) String() string {
	if msg.CommandValid {
		return fmt.Sprintf("{Command:%v,CommandTerm:%v,CommandIndex:%v}", msg.Command, msg.CommandTerm, msg.CommandIndex)
	} else if msg.SnapshotValid {
		return fmt.Sprintf("{Snapshot:%v,SnapshotTerm:%v,SnapshotIndex:%v}", msg.Snapshot, msg.SnapshotTerm, msg.SnapshotIndex)
	} else {
		panic(fmt.Sprintf("unexpected ApplyMsg{CommandValid:%v,CommandTerm:%v,CommandIndex:%v,SnapshotValid:%v,SnapshotTerm:%v,SnapshotIndex:%v}", msg.CommandValid, msg.CommandTerm, msg.CommandIndex, msg.SnapshotValid, msg.SnapshotTerm, msg.SnapshotIndex))
	}
}

type NodeState uint8

const (
	StateFollower NodeState = iota
	StateCandidate
	StateLeader
)

func (state NodeState) String() string {
	switch state {
	case StateFollower:
		return "Follower"
	case StateCandidate:
		return "Candidate"
	case StateLeader:
		return "Leader"
	}
	panic(fmt.Sprintf("unexpected NodeState %d", state))
}

type lockRand struct {
	mu   sync.Mutex
	rand *rand.Rand
}

func (r *lockRand) Intn(n int) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.rand.Intn(n)
}

var globalRand = &lockRand{
	mu:   sync.Mutex{},
	rand: rand.New(rand.NewSource(time.Now().UnixNano())),
}

const (
	HeartBeatTimeout = 125
	ElectionTimeout  = 1000
)

func StableHeartBeatTimeout() time.Duration {
	return time.Duration(HeartBeatTimeout) * time.Millisecond
}
func RandomizedElectionTimeout() time.Duration {
	return time.Duration(ElectionTimeout+globalRand.Intn(ElectionTimeout)) * time.Millisecond
}

// shrinkEntriesArray discards the underlying array used by the entries slice
// if most of it isn't being used. This avoids holding references to a bunch of
// potentially large entries that aren't needed anymore. Simply clearing the
// entries wouldn't be safe because clients might still be using them.
func shrinkEntriesArray(entries []Entry) []Entry {
	// We replace the array if we're using less than half of the space in
	// it. This number is fairly arbitrary, chosen as an attempt to balance
	// memory usage vs number of allocations. It could probably be improved
	// with some focused tuning.
	const lenMultiple = 2
	if len(entries)*lenMultiple < cap(entries) {
		newEntries := make([]Entry, len(entries))
		copy(newEntries, entries)
		return newEntries
	}
	return entries
}

func insertionSort(s []int) {
	a, b := 0, len(s)
	for i := a + 1; i < b; i++ {
		for j := i; j > a && s[j] < s[j-1]; j-- {
			s[j], s[j-1] = s[j-1], s[j]
		}
	}
}
