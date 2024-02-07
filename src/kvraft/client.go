package kvraft

import "6.5840/labrpc"
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	leaderID  int64
	clientID  int64
	commandID int64
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	return &Clerk{
		servers:   servers,
		leaderID:  0,
		clientID:  nrand(),
		commandID: 0,
	}
}

func (ck *Clerk) Command(args *CommandArgs) string {
	for {
		reply := new(CommandReply)
		if !ck.servers[ck.leaderID].Call("KVServer.Command", args, reply) || reply.Err == ErrWrongLeader || reply.Err == ErrTimeout {
			ck.leaderID = (ck.leaderID + 1) % int64(len(ck.servers))
			continue
		}
		ck.commandID++
		return reply.Value
	}
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.

func (ck *Clerk) Get(key string) string {
	// You will have to modify this function.
	return ck.Command(&CommandArgs{
		Key:       key,
		Value:     "",
		Op:        OpGet,
		ClientID:  ck.clientID,
		CommandID: ck.commandID,
	})
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op OperationOP) {
	// You will have to modify this function.
	ck.Command(&CommandArgs{
		Key:       key,
		Value:     value,
		Op:        op,
		ClientID:  ck.clientID,
		CommandID: ck.commandID,
	})
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, OpPut)
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, OpAppend)
}
