package kvraft

import (
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

type KVServer struct {
	mu      sync.RWMutex
	me      int
	rf      *raft.Raft // 内嵌raft节点
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big
	lastApplied  int

	stateMachine   KVStateMachine
	lastOperations map[int64]OperationContext
	notifyChans    map[int]chan *CommandReply
}

// isDuplicateArgs 判断当前的指令编号是否是过期的指令
func (kv *KVServer) isDuplicateArgs(clientID, commandID int64) bool {
	opcontext, ok := kv.lastOperations[clientID]
	return ok && opcontext.MaxAppliedCommandID >= commandID
}

// Command Handler函数, KVServer -> raft 然后等待返回
func (kv *KVServer) Command(args *CommandArgs, reply *CommandReply) {
	defer DPrintf("{Node %v} processes CommandRequest %v with CommandResponse %v", kv.rf.Me(), args, reply)
	kv.mu.RLock()
	if args.Op != OpGet && kv.isDuplicateArgs(args.ClientID, args.CommandID) {
		lastReply := kv.lastOperations[args.ClientID].LastReply
		reply.Value, reply.Err = lastReply.Value, lastReply.Err
		kv.mu.RUnlock()
		return
	}
	kv.mu.RUnlock()

	index, _, isLeader := kv.rf.Start(Command{args})
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	kv.mu.Lock()
	ch := kv.getNotifyChan(index)
	kv.mu.Unlock()
	// 等待回复reply, 需要做超时处理, 超时就代表raft那边没有应用该指令
	select {
	case res := <-ch:
		reply.Value, reply.Err = res.Value, res.Err
	case <-time.After(ExecuteTimeout):
		reply.Err = ErrTimeout
	}

	go func() {
		kv.mu.Lock()
		kv.removeOutdateChan(index)
		kv.mu.Unlock()
	}()
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	DPrintf("{Node %v} has been killed", kv.rf.Me())
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// applyLogToStateMachine 将command应用到状态机中, 返回一个reply
func (kv *KVServer) applyLogToStateMachine(command Command) *CommandReply {
	var value string
	var err Err
	switch command.Op {
	case OpPut:
		err = kv.stateMachine.Put(command.Key, command.Value)
	case OpAppend:
		err = kv.stateMachine.Append(command.Key, command.Value)
	case OpGet:
		value, err = kv.stateMachine.Get(command.Key)
	}
	return &CommandReply{
		Value: value,
		Err:   err,
	}
}

// applier 守护线程, 用于监控applyCh中的提交的日志, 然后应用到状态机中, raft通过notifyCh将成功执行的指令返回给server
func (kv *KVServer) applier() {
	for kv.killed() == false {
		select {
		case msg := <-kv.applyCh:
			DPrintf("{Node %v} tries to apply message %v", kv.rf.Me(), msg)
			if msg.CommandValid {
				// 当前是一个提交的操作
				kv.mu.Lock()
				if msg.CommandIndex <= kv.lastApplied {
					DPrintf("{Node %v} discards outdated message %v because a newer snapshot which lastApplied is %v has been restored",
						kv.rf.Me(), msg, kv.lastApplied)
					kv.mu.Unlock()
					continue
				}
				kv.lastApplied = msg.CommandIndex

				reply := new(CommandReply)
				command := msg.Command.(Command)

				// 查询操作是幂等的, 如果是修改操作, 且当前client的这次操作是过时操作, 就应该使用上一次操作的返回
				// TODO 但是这个在判断lastApplied时候应该已经continue掉了
				if command.Op != OpGet && kv.isDuplicateArgs(command.ClientID, command.CommandID) {
					DPrintf("{Node %v} doesn't apply duplicated message %v to stateMachine because maxAppliedCommandId is %v for client %v",
						kv.rf.Me(), msg, kv.lastOperations[command.ClientID], command.ClientID)
					lastReply := kv.lastOperations[command.ClientID].LastReply
					reply.Value, reply.Err = lastReply.Value, lastReply.Err
				} else {
					reply = kv.applyLogToStateMachine(command)
					if command.Op != OpGet {
						kv.lastOperations[command.ClientID] = OperationContext{
							MaxAppliedCommandID: command.CommandID,
							LastReply:           reply,
						}
					}
				}
				if currentTerm, isLeader := kv.rf.GetState(); isLeader && msg.CommandTerm == currentTerm {
					ch := kv.getNotifyChan(msg.CommandIndex)
					ch <- reply
				}
				if kv.needSnapshot() {
					kv.takeSnapshot(msg.CommandIndex)
				}
				kv.mu.Unlock()
			} else if msg.SnapshotValid {
				// 当前是一个应用快照的操作
				kv.mu.Lock()
				if kv.rf.CondInstallSnapshot(msg.SnapshotTerm, msg.SnapshotIndex, msg.Snapshot) {
					kv.restoreSnapshot(msg.Snapshot)
					kv.lastApplied = msg.SnapshotIndex
				}
				kv.mu.Unlock()
			} else {
				panic(fmt.Sprintf("unexpected message %v", msg))
			}
		}
	}
}

func (kv *KVServer) getNotifyChan(idx int) chan *CommandReply {
	if _, ok := kv.notifyChans[idx]; !ok {
		kv.notifyChans[idx] = make(chan *CommandReply, 1)
	}
	return kv.notifyChans[idx]
}

func (kv *KVServer) removeOutdateChan(idx int) {
	delete(kv.notifyChans, idx)
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Command{})

	applyCh := make(chan raft.ApplyMsg) // raft节点和KVServer节点共用一个applyCh
	kv := &KVServer{
		mu:             sync.RWMutex{},
		me:             me,
		rf:             raft.Make(servers, me, persister, applyCh),
		applyCh:        applyCh,
		dead:           0,
		maxraftstate:   maxraftstate,
		lastApplied:    0,
		stateMachine:   NewMemoryKV(),
		lastOperations: make(map[int64]OperationContext),
		notifyChans:    make(map[int]chan *CommandReply),
	}

	// You may need initialization code here.
	kv.restoreSnapshot(persister.ReadSnapshot())
	// TODO snapshot
	go kv.applier()
	DPrintf("{Node %v} has started", kv.rf.Me())

	return kv
}
