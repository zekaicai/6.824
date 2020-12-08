package mr

import (
	"fmt"
	"log"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

const (

	Idle int = 0
	InProgress int = 1
	Completed int = 2
	MapTask int = 0
	ReduceTask int = 1
	WaitTask int = 2
	EndTask int = 3
	TimeLimitInSec = 10
)

type Node struct {

	task *Task
	next *Node
	prev *Node
}

func NodeEquals(node1 *Node, node2 *Node) bool{

	return TaskEquals(node1.task, node2.task)
}

// <-> anchor Back <-> Front
type DLList struct {

	anchor *Node
	size int
	mutex sync.Mutex
}

func NewDDList() *DLList {

	list := &DLList{}
	anchor := &Node{task: nil}
	anchor.next = anchor
	anchor.prev = anchor
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

func (list *DLList)findNode(task *Task) *Node {

	list.lock()

	if list.IsEmpty() {
		list.unlock()
		return nil
	}

	anchor := list.GetAnchor()
	node := anchor.next

	for node != anchor {

		if TaskEquals(node.task, task) {

			list.unlock()
			return node
		}
		node = node.next
	}

	list.unlock()
	return nil
}

func (list *DLList)removeNode(task *Task) *Node{

	nodeToRemove := list.findNode(task)
	if nodeToRemove == nil {
		return nil
	}

	list.lock()
	prev := nodeToRemove.prev
	next := nodeToRemove.next
	prev.next = next
	next.prev = prev
	list.size--
	list.unlock()
	return nodeToRemove
}

func (list *DLList)findTimeOutTask(sec float64) *Task{
	list.lock()

	if list.IsEmpty() {
		list.unlock()
		return nil
	}

	anchor := list.GetAnchor()
	node := anchor.next

	for node != anchor {

		if node.task.TaskOutOfTime(sec) {

			list.unlock()
			return node.task
		}
		node = node.next
	}

	list.unlock()
	return nil
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
		list.unlock()
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

func (q *SyncQueue)popAnTimeOutTask(sec float64) *Task{

	return q.list.findTimeOutTask(sec)
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

func (q *SyncQueue)isEmpty() bool {

	return q.list.IsEmpty()
}

func (q *SyncQueue)remove(task *Task) *Task {

	list := q.list
	removedNode := list.removeNode(task)
	if removedNode == nil {
		return nil
	}
	return removedNode.task
}

type Task struct {

	TaskType      int    // MapTask/ReduceTask/WaitTask/EndTask
	FileName      string // name of file to read from for map Task
	MapTaskIdx    int
	ReduceTaskIdx int
	ReduceFiles   *[]string
	assignedTime time.Time
}

func (task *Task)TaskOutOfTime(sec float64) bool {

	now := time.Now()
	diff := now.Sub(task.assignedTime)
	if diff.Seconds() > sec {
		return true
	}
	return false
}

func TaskEquals(task1 *Task, task2 *Task) bool{

	if task1.TaskType != task2.TaskType {
		return false
	}

	if task1.TaskType == MapTask {
		return task1.MapTaskIdx == task2.MapTaskIdx
	}

	if task1.TaskType == ReduceTask {
		return task1.ReduceTaskIdx == task2.ReduceTaskIdx
	}

	return false
}

func (task *Task)GetReduceFiles() *[]string {

	return task.ReduceFiles
}

func (task *Task)GetTaskType() int {
	return task.TaskType
}

func (task *Task)GetFileName() string {
	return task.FileName
}

func (task *Task)GetMapTaskId() int {
	return task.MapTaskIdx
}
func (task *Task)GetReduceTaskId() int {
	return task.ReduceTaskIdx
}

type Master struct {
	// Your definitions here.
	nReduce int
	nMap int
	mapTaskQueue [3]*SyncQueue
	reduceTaskQueue [3]*SyncQueue
	mapTasksDone bool
	reduceTasksDone bool
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



func (m *Master) GetTask(args *EmptyArgs, reply *GetTaskReply) error {

	if !m.mapTasksDone {

		task := m.mapTaskQueue[Idle].pop()
		if task == nil { // If map tasks are not done but no idle map tasks left, means all map tasks are in progress, return an wait taskToAssign

			inProgressMapTaskQueue := m.mapTaskQueue[InProgress]
			taskToAssign := inProgressMapTaskQueue.popAnTimeOutTask(TimeLimitInSec)
			if taskToAssign != nil {
				taskToAssign.assignedTime = time.Now()
				reply.Task = taskToAssign
				reply.NReduce = m.nReduce
				return nil
			}
			taskToAssign = NewWaitTask()
			reply.Task = taskToAssign
			return nil
		}
		task.assignedTime = time.Now()
		m.mapTaskQueue[InProgress].push(task)
		reply.Task = task
		reply.NReduce = m.nReduce
		return nil
	}

	if !m.reduceTasksDone {

		task := m.reduceTaskQueue[Idle].pop()
		if task == nil { // If map tasks are not done but no idle map tasks left, means all map tasks are in progress, return an wait taskToAssign

			inProgressReduceTaskQueue := m.reduceTaskQueue[InProgress]
			taskToAssign := inProgressReduceTaskQueue.popAnTimeOutTask(TimeLimitInSec)
			if taskToAssign != nil {
				taskToAssign.assignedTime = time.Now()
				reply.Task = taskToAssign
				return nil
			}
			taskToAssign = NewWaitTask()
			reply.Task = taskToAssign
			return nil
		}
		task.assignedTime = time.Now()
		m.reduceTaskQueue[InProgress].push(task)
		reply.Task = task
		return nil
	}

	if m.allTasksDone() {
		task := NewEndTask()
		reply.Task = task
		return nil
	}

	return nil
}

func NewWaitTask() *Task {
	
	return &Task{
		TaskType: WaitTask,
	}
}

func  NewEndTask() *Task {
	return &Task{
		TaskType: EndTask,
	}
}


func (m *Master) NotifyTaskDone(args *TaskDoneArgs, reply *EmptyArgs) error {

	task := args.Task
	switch task.GetTaskType() {
	case MapTask:
		m.handleMapTaskDone(task)
	case ReduceTask:
		m.handleReduceTaskDone(task)
	default:
		panic("Master received task done notification, but wrong task type")
	}
	return nil
}

func (m *Master)handleReduceTaskDone(task *Task) {

	reduceTaskQueue := m.reduceTaskQueue
	reduceTaskInProgressQueue := reduceTaskQueue[InProgress]
	if reduceTaskInProgressQueue.remove(task) == nil {
		return
	}
	reduceTaskQueue[Completed].push(task)
	reduceTaskIdleQueue := reduceTaskQueue[Idle]
	if reduceTaskIdleQueue.isEmpty() && reduceTaskInProgressQueue.isEmpty() {
		m.reduceTasksDone = true
	}
}

func (m *Master)handleMapTaskDone(task *Task) {

	mapTaskQueue := m.mapTaskQueue
	mapTaskInProgressQueue := mapTaskQueue[InProgress]
	if mapTaskInProgressQueue.remove(task) == nil {
		return
	}
	renameReduceFiles(task)
	mapTaskQueue[Completed].push(task)
	mapTaskIdleQueue := mapTaskQueue[Idle]
	if mapTaskIdleQueue.isEmpty() && mapTaskInProgressQueue.isEmpty() {
		m.mapTasksDone = true
		m.createInitReduceTasks()
	}
}

func (m *Master)createInitReduceTasks() {

	for i := 0; i < m.nReduce; i++ {

		reduceFiles := make([]string, m.nMap)
		for j := 0; j < m.nMap; j++ {
			reduceFiles[j] = generateIntermediateFileName(j, i)
		}
		task := Task{
			TaskType:      ReduceTask,
			ReduceTaskIdx: i,
			ReduceFiles:   &reduceFiles,
		}
		m.reduceTaskQueue[Idle].push(&task)
	}
}

func renameReduceFiles(task *Task) {

	reduceFiles := task.ReduceFiles
	mapTaskId := task.MapTaskIdx

	for idx, oldName := range *reduceFiles {

		newName := generateReduceFileName(mapTaskId, idx)
		os.Rename(oldName, newName)
		(*reduceFiles)[idx] = newName
	}
}

func generateReduceFileName(mapTaskId int, reduceTaskId int) string {

	return fmt.Sprintf("mr-%d-%d", mapTaskId, reduceTaskId)
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
	if m.allTasksDone() {
		ret = true
	}

	return ret
}

//
// create a Master.
// main/mrmaster.go calls this function.
// NReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}

	// Your code here.
	m.init(nReduce)
	m.addInitMapTasks(files)
	m.nMap = len(files)
	m.server()
	return &m
}

func (m *Master)init(nReduce int)  {

	m.nReduce = nReduce
	for i := 0; i < 3; i++ {
		m.mapTaskQueue[i] = NewSyncQueue()
		m.reduceTaskQueue[i] = NewSyncQueue()
	}
}

func (m *Master) addInitMapTasks(files []string) {

	idleQ := m.mapTaskQueue[Idle]
	for i := 0; i < len(files); i++ {

		task := Task{
			TaskType:   MapTask,
			FileName:   files[i],
			MapTaskIdx: i,
		}
		idleQ.push(&task)
	}
}

func (m *Master) allTasksDone() bool {

	return m.mapTasksDone && m.reduceTasksDone
}
