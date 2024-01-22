package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.

// ReportRequest 报告完成的任务ID以及当前完成的是什么状态的任务(map or reduce)
type ReportRequest struct {
	Id    int
	Phase SchedulePhase
}
type ReportResponse struct {
}

// HeartbeatResponse Coordinator返回的响应, 包括分配给这个worker的task的详细信息
type HeartbeatResponse struct {
	TaskType TaskType
	NMap     int
	NReduce  int
	Id       int
	FileName string
}
type HeartbeatRequest struct {
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
