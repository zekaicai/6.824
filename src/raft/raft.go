package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(Command interface{}) (index, Term, isleader)
//   start agreement on a new log entry
// rf.GetState() (Term, isLeader)
//   ask a Raft for its current Term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"fmt"
	"sync"
	"time"
)
import "sync/atomic"
import "../labrpc"

// import "bytes"
// import "../labgob"

const (

	// Election related constants
	MinElectionTimeOut         int = 500 // in Millisecond
	MaxElectionTimeOut         int = 800 // in Millisecond
	ElectionTimerCheckInterval int = 10  // Interval for the check go routine to check if it should start an election
	HeartBeatInterval          int = 100 // The tester requires that the leader send heartbeat RPCs no more than ten times per second.

	// Raft instance state
	Follower  int = 0
	Candidate int = 1
	Leader    int = 2
)

//
// as each Raft peer becomes aware that successive log Entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

type LogEntry struct {
	Command interface{}
	Term    int
}

func (entry *LogEntry) getTerm() int {

	if entry == nil {
		return -1
	}
	return entry.Term
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	state                  int
	currentTerm            int
	votedFor               int
	electionTimerStartTime time.Time

	// set to false when starting an election, if timer times out
	// and the raft is in candidate state, set it to true and broadcast on cond
	// to wake former raft waiting on cond for election result and end last round
	// election, the timer will trigger a new election
	waitElectionTimeOut bool
	randTimeOutPeriod   time.Duration
	cond                *sync.Cond // condition variable for raft instance to wait on when waiting result for an election

	log         []LogEntry
	nextIndex   []int
	matchIndex  []int
	commitIndex int
	lastApplied int
	applyCh     chan ApplyMsg
}

func (rf *Raft) raftDPrintf(str string, a ...interface{}) {

	prefix := fmt.Sprintf("[Server%d][Term%d] ", rf.me, rf.currentTerm)
	DPrintf(prefix+str, a...)
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	term = rf.currentTerm
	isleader = rf.state == Leader
	rf.mu.Unlock()
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next Command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// Command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the Command will appear at
// if it's ever committed. the second return value is the current
// Term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
	rf.mu.Lock()
	isLeader = rf.state == Leader
	if !isLeader {
		rf.mu.Unlock()
		rf.raftDPrintf("Start() return false")
		return index, term, isLeader
	}

	rf.log = append(rf.log, LogEntry{
		Command: command,
		Term:    rf.currentTerm,
	})
	index = len(rf.log) - 1
	term = rf.currentTerm
	rf.raftDPrintf("Start() return true, new log entry is at index of %d", index)
	rf.mu.Unlock()
	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
	rf.cond.Broadcast()
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) initNextIndex() {

	rf.nextIndex = make([]int, len(rf.peers))

	for i := 0; i < len(rf.nextIndex); i++ {

		if rf.me == i {
			rf.nextIndex[i] = -1
			continue
		}
		rf.nextIndex[i] = len(rf.log) // last log index + 1
	}
}

func (rf *Raft) hasLogEntriesToSendTo(follower int) bool {

	return len(rf.log)-1 >= rf.nextIndex[follower]
}

func (rf *Raft) containsMatchingLogEntry(index int, term int) bool {

	if !rf.containsLogEntryAtIndex(index) {
		return false
	}

	return rf.log[index].Term == term
}

func (rf *Raft) containsLogEntryAtIndex(idx int) bool {

	return len(rf.log)-1 >= idx
}

func (rf *Raft) initMatchIndex() {

	rf.matchIndex = make([]int, len(rf.peers))

	for i := 0; i < len(rf.matchIndex); i++ {

		if rf.me == i {
			rf.matchIndex[i] = -1
			continue
		}
		rf.matchIndex[i] = 0 // last log index + 1
	}
}

func (rf *Raft) majorityAgreesOnIndex(index int) bool {

	count := 1
	for i := 0; i < len(rf.matchIndex); i++ {

		if rf.me == i {
			continue
		}
		if rf.matchIndex[i] >= index {
			count++
		}
	}

	return count > len(rf.peers)/2
}

func (rf *Raft) isMoreUpToDate(args *RequestVoteArgs) bool {

	myLastEntryIndex := len(rf.log) - 1
	myLastEntryTerm := rf.log[myLastEntryIndex].Term

	if myLastEntryTerm != args.LastLogTerm {
		if myLastEntryTerm > args.LastLogTerm {
			return true
		} else {
			return false
		}
	}

	if myLastEntryIndex > args.LastLogIndex {
		return true
	}

	return false
}

func (rf *Raft) printRaftMessage() {

	DPrintf("[Server%d] Just won an election.\n"+
		"Its current term is %d", rf.me, rf.currentTerm)
	DPrintf("NextIndex")
	for i := 0; i < len(rf.nextIndex); i++ {
		DPrintf("Follower%d, nextIndex is %d", i, rf.nextIndex[i])
	}
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	DPrintf("[Server%d] Make()", me)
	rf.state = Follower
	rf.mu = sync.Mutex{}
	rf.cond = sync.NewCond(&rf.mu)
	rf.applyCh = applyCh

	rf.mu.Lock()
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.resetElectionTimer()
	rf.waitElectionTimeOut = false
	rf.log = make([]LogEntry, 1) // first index is 1, at index 0 is a {nil, 0} entry
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.initNextIndex()
	rf.initMatchIndex()
	rf.mu.Unlock()
	go rf.timerStart() // start a long running goroutine timely check if times up

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}
