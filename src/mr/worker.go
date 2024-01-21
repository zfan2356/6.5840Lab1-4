package mr

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
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
	response := &HeartbeatResponse{}
	call("Coordinator.HeartBeat", &HeartbeatRequest{}, response)
	return response
}

// 汇报task的完成情况
func doReport(id int, phase SchedulePhase) {
	call("Coordinator.Report", &ReportRequest{id, phase}, &ReportResponse{})
}

func doMapTask(mapf func(string, string) []KeyValue, response *HeartbeatResponse) {
	filename := response.FileName
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open file %v \n", filename)
	}
	content, err := io.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read file %v\n", filename)
	}
	file.Close()
	kva := mapf(filename, string(content))
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
			interFilePath := generateMapResultFileName(response.Id, idx)
			var buf bytes.Buffer
			enc := json.NewEncoder(&buf)
			for _, kv := range intermediate {
				err := enc.Encode(&kv)
				if err != nil {
					log.Fatalf("cannot encode json %v\n", kv)
				}
			}
			err := atomicWriteFile(interFilePath, &buf)
			if err != nil {
				log.Fatalf("map write file failed, err: %v \n", err)
			}
		}(i, intermediate)
	}
	wg.Wait()
	doReport(response.Id, MapPhase)
}

func doReduceTask(reducef func(string, []string) string, response *HeartbeatResponse) {
	var kva []KeyValue
	for i := 0; i < response.NMap; i++ {
		filePath := generateMapResultFileName(i, response.Id)
		file, err := os.Open(filePath)
		if err != nil {
			log.Fatalf("cannot open file %v \n", filePath)
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
		file.Close()
	}
	results := make(map[string][]string)
	for _, kv := range kva {
		results[kv.Key] = append(results[kv.Key], kv.Value)
	}
	var buf bytes.Buffer
	for k, v := range results {
		output := reducef(k, v)
		fmt.Fprintf(&buf, "%v %v\n", k, output)
	}
	atomicWriteFile(generateReduceResultFileName(response.Id), &buf)
	doReport(response.Id, ReducePhase)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	for {
		response := doHeartBeat()
		//log.Printf("Worker: receive coordinator's heartbeat %v \n", response)
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
