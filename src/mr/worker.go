package mr

import (
	"fmt"
    "io"
    "io/ioutil"
	"os"
    "sync"
    "time"
)
import "log"
import "net/rpc"
import "hash/fnv"

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// 请求task的方法
func doHeartBeat() *HeartbeatResponse {
	response := HeartbeatResponse{}
	call("Coordinator.HeartBeat", &HeartbeatRequest{}, &response)
	return &response
}

// 汇报task的完成情况
func doReport(id int, phase SchedulePhase) {
	call("Coordinator.Report", &ReportRequest{id, phase}, &ReportResponse{})
}

func doMapTask(mapf func(string, string) []KeyValue, response *HeartbeatResponse) {
	file, err := os.Open(response.FileName)
	defer file.Close()
	if err != nil {
		log.Fatalf("cannot open file %v \n", response.FileName)
		return
	}
	content, err := io.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read file %v\n", response.FileName)
		return
	}

	kva := mapf(response.FileName, string(content))
    intermediates := make([][]KeyValue, response.NReduce)
    for _, kv := range kva {
        idx := ihash(kv.Key) % response.NReduce
        intermediates[idx] = append(intermediates[idx], kv)
    }
    var wg sync.WaitGroup
    for i, intermediate := range intermediates {
        wg.Add(1)
        go func(idx int, intermediate []KeyValue) {
            defer wg.Done()
            interFilePath := 
        }(i, intermediate)
    }
    wg.Wait()
    doReport(response.Id, MapPhase)
}

func doReduceTask(reducef func(string, []string) string, response *HeartbeatResponse) {
	var kva []KeyValue
	for i := 0; i < response.NReduce; i++ {

	}
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

	for {
		response := doHeartBeat()
		log.Printf("Worker: receive coordinator's heartbeat %v \n", response)
		switch response.TaskType {
		case MapTask:
			doMapTask(mapf, response)
		case ReduceTask:
			doReduceTask(reducef, response)
		case WaitTask:
			time.Sleep(time.Second)
		case CompleteTask:
			return
		default:
			panic(fmt.Sprintf("unexist task type %v \n", response.TaskType))
		}
	}

}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
