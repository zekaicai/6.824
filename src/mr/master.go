package mr

import (
	"log"
	"sync"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Node struct {

	task *Task
	next *Node
	prev *Node
}

// <-> anchor Back <-> Front
type DLList struct {

	anchor *Node
	size int
	mutex sync.Mutex
}

func NewDDList() *DLList {

	list := &DLList{}
	anchor := &Node{task: nil,
		next: nil, prev: nil}
	list.anchor = anchor
	list.size = 0
	return list
}

func (list *DLList)GetAnchor() *Node{

	return list.anchor
}

func (list *DLList)lock() {

	list.mutex.Lock()
}

func (list *DLList)unlock() {

	list.mutex.Unlock()
}

func (list *DLList)addToBack(node *Node)  {

	list.lock()
	anchor := list.GetAnchor()
	next := anchor.next
	anchor.next = node
	node.prev = anchor
	node.next = next
	next.prev = node
	list.size++
	list.unlock()
}

func (list *DLList)GetSize() int {

	return list.size
}

func (list *DLList)IsEmpty() bool {

	return list.GetSize() == 0
}

func (list *DLList)removeFromFront() *Node{

	list.lock()
	if list.IsEmpty() {
		return nil
	}
	anchor := list.GetAnchor()
	nodeToRemove := anchor.prev
	prev := nodeToRemove.prev
	prev.next = anchor
	anchor.prev = prev
	list.size--
	list.unlock()
	nodeToRemove.prev = nil
	nodeToRemove.next = nil
	return nodeToRemove
}

type SyncQueue struct {

	list *DLList
}

func NewSyncQueue() *SyncQueue {

	q := &SyncQueue{}
	q.list = NewDDList()
	return q
}

func (q *SyncQueue)push(task *Task) {

	node := &Node{task: task, next: nil, prev: nil}
	q.list.addToBack(node)
}

func (q *SyncQueue)pop() *Task {

	node := q.list.removeFromFront()

	if node == nil {
		return nil
	}

	return node.task
}

type Task struct {

}

type Master struct {
	// Your definitions here.
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}


//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	ret := false

	// Your code here.


	return ret
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}

	// Your code here.
	m.init()
	m.addInitMapTasks(files)

	m.server()
	return &m
}

func (m *Master)init()  {

	m.IdleMapTaskQ = make(chan MapTask, BufferedChannelSize)
	m.InProgressMapTaskQ = make(chan MapTask, BufferedChannelSize)
	m.CompletedMapTaskQ = make(chan MapTask, BufferedChannelSize)
	m.IdleReduceTaskQ = make(chan ReduceTask, BufferedChannelSize)
	m.InProgressReduceTaskQ = make(chan ReduceTask, BufferedChannelSize)
	m.CompletedReduceTaskQ = make(chan ReduceTask, BufferedChannelSize)
}

func (m *Master) addInitMapTasks(files []string) {


}
