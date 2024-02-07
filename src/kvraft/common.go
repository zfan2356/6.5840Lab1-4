package kvraft

import (
	"fmt"
	"log"
	"os"
	"time"
)

const Debug = true

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		logFile, _ := os.OpenFile("./debug.log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
		log.SetOutput(logFile)
		log.Printf(format, a...)
	}
	return
}

type Err uint8

const ExecuteTimeout = 500 * time.Millisecond
const (
	OK Err = iota
	ErrNoKey
	ErrWrongLeader
	ErrTimeout
)

func (err Err) String() string {
	switch err {
	case OK:
		return "OK"
	case ErrNoKey:
		return "ErrNoKey"
	case ErrWrongLeader:
		return "ErrWrongLeader"
	case ErrTimeout:
		return "ErrTimeout"
	}
	panic(fmt.Sprintf("unexpected Err %d", err))
}

type CommandArgs struct {
	Key   string
	Value string
	Op    OperationOP
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClientID  int64
	CommandID int64
}

type CommandReply struct {
	Value string
	Err   Err
}

type OperationOP uint8

const (
	OpPut OperationOP = iota
	OpAppend
	OpGet
)

func (o OperationOP) String() string {
	switch o {
	case OpPut:
		return "OpPut"
	case OpAppend:
		return "OpAppend"
	case OpGet:
		return "OpGet"
	}
	panic(fmt.Sprintf("unexpected OperationOP %d", o))
}

type Command struct {
	*CommandArgs
}

type OperationContext struct {
	MaxAppliedCommandID int64
	LastReply           *CommandReply
}
