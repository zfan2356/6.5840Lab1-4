package mr

import "fmt"

type SchedulePhase uint8

const (
	MapPhase SchedulePhase = iota
	ReducePhase
	CompletePhase
)

func (phash SchedulePhase) String() string {
	switch phash {
	case MapPhase:
		return "MapPhase"
	case ReducePhase:
		return "ReducePhase"
	case CompletePhase:
		return "CompletePhase"
	}
	panic(fmt.Sprintf("unexist SchedulePhase %d", phash))
}

type TaskType uint8

const (
	MapTask TaskType = iota
	ReduceTask
	WaitTask
	CompleteTask
)

func (t TaskType) String() string {
	switch t {
	case MapTask:
		return "MapTask"
	case ReduceTask:
		return "ReduceTask"
	case CompleteTask:
		return "CompleteTask"
	case WaitTask:
		return "WaitTask"
	}
	panic(fmt.Sprintf("unexist TaskType %d \n", t))
}

type TaskStatus uint8

const (
	Idle = iota
	Working
	Finished
)
