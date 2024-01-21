package mr

import (
	"log"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

const (
	MaxTaskRunInterval = time.Second * 10
)

type Task struct {
	filename  string
	id        int
	startTime time.Time
	status    int
}

type Coordinator struct {
	// Your definitions here.
	files       []string
	nReduce     int
	nMap        int
	phase       SchedulePhase
	tasks       []Task
	heartbeatCh chan heartbeatMsg
	reportCh    chan reportMsg
	doneCh      chan struct{} // 仅用于传递信息, 表示已经完成
}

type heartbeatMsg struct {
	response *HeartbeatResponse
	ok       chan struct{}
}

type reportMsg struct {
	request *ReportRequest
	ok      chan struct{}
}

// Your code here -- RPC handlers for the worker to call.

func (c *Coordinator) HeartBeat(request *HeartbeatRequest, response *HeartbeatResponse) error {
	msg := heartbeatMsg{response, make(chan struct{})}
	c.heartbeatCh <- msg
	<-msg.ok
	return nil
}

func (c *Coordinator) Report(request *ReportRequest, response *ReportResponse) error {
	msg := reportMsg{request, make(chan struct{})}
	c.reportCh <- msg
	<-msg.ok
	return nil
}

func (c *Coordinator) initMapPhase() {
	c.phase = MapPhase
	c.tasks = make([]Task, len(c.files))
	for i, file := range c.files {
		c.tasks[i] = Task{
			filename: file,
			id:       i,
			status:   Idle,
		}
	}
}

func (c *Coordinator) initReducePhase() {
	c.phase = ReducePhase
	c.tasks = make([]Task, c.nReduce)
	for i := 0; i < c.nReduce; i++ {
		c.tasks[i] = Task{
			id:     i,
			status: Idle,
		}
	}
}

func (c *Coordinator) initCompletePhase() {
	c.phase = CompletePhase
	c.doneCh <- struct{}{}
}

func (c *Coordinator) selectTask(response *HeartbeatResponse) bool {
	allfinished := true
	ok := false
	for i, task := range c.tasks {
		switch task.status {
		case Idle:
			allfinished = false
			ok = true
			c.tasks[i].status, c.tasks[i].startTime = Working, time.Now()
			response.NReduce, response.NMap, response.Id = c.nReduce, c.nMap, i
			if c.phase == MapPhase {
				response.TaskType = MapTask
				response.FileName = c.files[i]
			} else {
				response.TaskType = ReduceTask
			}
		case Working:
			allfinished = false
			if time.Now().Sub(task.startTime) > MaxTaskRunInterval {
				ok = true
				c.tasks[i].startTime = time.Now()
				response.NReduce, response.NMap, response.Id = c.nReduce, c.nMap, i
				if c.phase == MapPhase {
					response.TaskType = MapTask
					response.FileName = c.files[i]
				} else {
					response.TaskType = ReduceTask
				}
			}
		case Finished:

		}
		if ok {
			break
		}
	}
	if !ok {
		response.TaskType = WaitTask
	}
	return allfinished
}

func (c *Coordinator) schedule() {
	c.initMapPhase()
	for {
		select {
		case msg := <-c.heartbeatCh:
			if c.phase == CompletePhase {
				msg.response.TaskType = CompleteTask
			} else if c.selectTask(msg.response) {
				switch c.phase {
				case MapPhase:
					log.Printf("Coordinator: %v finished, start %v \n", MapPhase, ReducePhase)
					c.initReducePhase()
					c.selectTask(msg.response)
				case ReducePhase:
					log.Printf("Coordinator: %v finished, end!", ReducePhase)
				}
			}
			log.Printf("Coordinator: assigned a task %v to worker \n", msg.response)
			msg.ok <- struct{}{}
		case msg := <-c.reportCh:
			if msg.request.Phase == c.phase {
				log.Printf("Coordinator: Worker has executed task %v \n", msg.request)
				c.tasks[msg.request.Id].status = Finished
			}
			msg.ok <- struct{}{}
		}
	}
}

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	<-c.doneCh
	return true
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		files:       files,
		nReduce:     nReduce,
		nMap:        len(files),
		heartbeatCh: make(chan heartbeatMsg),
		reportCh:    make(chan reportMsg),
		doneCh:      make(chan struct{}),
	}
	c.server()
	go c.schedule()
	return &c
}
