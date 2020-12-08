package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// Task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}


//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	for {
		reply := GetTask()
		//task := reply.Task
		//n := reply.NReduce
		switch reply.Task.GetTaskType() {
		case MapTask:
			doMap(reply, mapf)
		case ReduceTask:
			doReduce(reply, reducef)
		case WaitTask:
			doWait()
		case EndTask:
			return
		default:
			panic("Worker received wrong Task type")
		}
	}
}

func doReduce(reply *GetTaskReply, reducef func(string, []string) string) {

	task := reply.Task
	reduceFiles := task.GetReduceFiles()
	var intermediate []KeyValue
	readReduceFiles(reduceFiles, &intermediate)
	sort.Sort(ByKey(intermediate))
	fileName := generateReduceOutputFileName(task.GetReduceTaskId())
	file, err := os.Create(fileName)
	if err != nil {
		panic("Error when creating reduce output file")
	}
	reduceAndWrite(file, &intermediate, reducef)
	NotifyTaskDone(task)
}

func reduceAndWrite(file *os.File, intermediate *[]KeyValue, reducef func(string, []string) string) {

	i := 0
	for i < len(*intermediate) {
		j := i + 1
		for j < len(*intermediate) && (*intermediate)[j].Key == (*intermediate)[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, (*intermediate)[k].Value)
		}
		output := reducef((*intermediate)[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(file, "%v %v\n", (*intermediate)[i].Key, output)

		fmt.Printf( "%v %v\n", (*intermediate)[i].Key, output)
		i = j
	}

	file.Close()
}

func generateReduceOutputFileName(reduceTaskId int) string {

	return fmt.Sprintf("mr-out-%d", reduceTaskId)
}

func readReduceFiles(files *[]string, intermediate *[]KeyValue) {

	for i := 0; i < len(*files); i++ {

		file, err := os.Open((*files)[i])
		if err != nil {
			panic("Error opening reduce file")
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			//fmt.Println(kv)
			*intermediate = append(*intermediate, kv)
		}
	}
}

func doMap(reply *GetTaskReply, mapf func(string, string) []KeyValue){

	var intermediate []KeyValue
	task := reply.Task
	filename := task.GetFileName()
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	kva := mapf(filename, string(content))
	intermediate = append(intermediate, kva...)
	tmpFileNameSlice := writeIntermediateFile(intermediate, task.GetMapTaskId(), reply.NReduce)
	task.ReduceFiles = tmpFileNameSlice
	NotifyTaskDone(task)
}

func NotifyTaskDone(task *Task) {

	args := TaskDoneArgs{Task: task}

	reply := EmptyArgs{}

	call("Master.NotifyTaskDone", &args, &reply)
}

func writeIntermediateFile(intermediate []KeyValue, mapTaskId int, nReduce int) *[]string{

	encoderSlice := make([]*json.Encoder, nReduce)
	tmpFileNameSlice := make([]string, nReduce)
	for i := 0; i < nReduce; i++ {
		// file, err := os.Create(generateIntermediateFileName(mapTaskId, i))
		file, err := ioutil.TempFile("./", "mr-tmp-*")
		if err != nil {
			panic("Error when create file")
		}
		encoderSlice[i] = json.NewEncoder(file)
		tmpFileNameSlice[i] = file.Name()
	}

	for _, kv := range intermediate {

		encoder := encoderSlice[getReduceTaskId(kv.Key, nReduce)]
		err := encoder.Encode(&kv)
		if err != nil {
			panic("Error encoding kv")
		}
	}

	return &tmpFileNameSlice
}

func getReduceTaskId(key string, nReduce int) int{

	return ihash(key) % nReduce
}

func generateIntermediateFileName(mapTaskId int, reduceId int) string {

	result := fmt.Sprintf("mr-%d-%d", mapTaskId, reduceId)
	return result
}


func doWait() {
	time.Sleep(time.Second)
}

func GetTask() *GetTaskReply{

	args := EmptyArgs{}

	// declare a reply structure.
	reply := &GetTaskReply{}

	// send the RPC request, wait for the reply.
	call("Master.GetTask", &args, reply)

	return reply
}

//
// example function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Master.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
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
