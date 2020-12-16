package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, Term, isleader)
//   start agreement on a new log entry
// rf.GetState() (Term, isLeader)
//   ask a Raft for its current Term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"math/rand"
	"sync"
	"time"
)
import "sync/atomic"
import "../labrpc"

// import "bytes"
// import "../labgob"

const (
	MinElectionTimeOut int = 150 // in Millisecond
	MaxElectionTimeOut int = 300 // in Millisecond
	ElectionTimerCheckInterval int = 10 // Interval for the check go routine to check if it should start an election
	WaitVoteTimeOut = 50
	HeartBeatInterval = 10
	Follower int = 0
	Candidate int = 1
	Leader int = 2
)

func (rf *Raft) setState(state int) {

	rf.state = state
}

//
// as each Raft peer becomes aware that successive log entries are
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
	state int
	currentTerm int
	votedFor int
	electionTimerStartTime time.Time
	randTimeOutPeriod time.Duration
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
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term        int // candidate's Term
	CandidateId int // candidate requesting vote
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  // current Term, for candidate to update itself
	VoteGranted bool // true means candidate received vote
}

type AppendEntriesArgs struct {

	Term int // leader's term
	LeaderId int // so follower can redirect clients
}

type AppendEntriesReply struct {

	Term int // current term, for leader to update itself
}
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {

	rf.mu.Lock()
	if args.Term > rf.currentTerm {
		DPrintf("[Server%d] received append entries with higher term from %d, " +
			"enter a new term and become follower" ,rf.me, args.LeaderId)
		//rf.startNewTerm(args.Term)
		rf.setState(Follower)
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.resetElectionTimer()
		reply.Term = rf.currentTerm
		rf.mu.Unlock()
		return
	}
	if args.Term < rf.currentTerm {
		DPrintf("[Server%d] received append entries with lower term form Server%d", rf.me, args.LeaderId)
		reply.Term = rf.currentTerm
		rf.mu.Unlock()
		return
	}
	if args.Term == rf.currentTerm {
		DPrintf("[Server%d] received append entries with equal term form Server%d", rf.me, args.LeaderId)
		rf.resetElectionTimer()
		DPrintf("[Server%d] reset it's timer to %s milliseconds after %s",
			rf.me, rf.randTimeOutPeriod.String(), rf.electionTimerStartTime.String())
		rf.state = Follower
		reply.Term = rf.currentTerm
		rf.mu.Unlock()
		return
	}
}
//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	DPrintf("[Server%d] Received vote request from Server%d", rf.me, args.CandidateId)
	// reply false if Term < currentTerm
	if args.Term < rf.currentTerm {

		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		DPrintf("[Server%d] vote false for request from Server%d, " +
			"because its term %d is greater than that in request(term %d)", rf.me, args.CandidateId,
			rf.currentTerm, args.Term)
		// reply false if Term < currentTerm
		rf.mu.Unlock()
		return
	}
	// if this is the first time this server sees the new term,
	// it (1) updates its term
	// (2) set votedFor in this new term to -1
	if args.Term > rf.currentTerm {

		//rf.startNewTerm(args.Term)
		rf.setState(Follower)
		rf.currentTerm = args.Term
		rf.votedFor = -1
	}
	// if voted for is null or candidateID
	// TODO: and candidate's log is at least as up-to-date as receiver's log, grant vote
	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {

		reply.Term = rf.currentTerm
		reply.VoteGranted = true
		DPrintf("[Server%d] vote true for request from Server%d", rf.me, args.CandidateId)
		rf.resetElectionTimer()
		rf.mu.Unlock()
		return
	}

	reply.Term = rf.currentTerm
	reply.VoteGranted = false
	DPrintf("[Server%d] vote false for request from Server%d," +
		" because it has already voted for another server in this term", rf.me, args.CandidateId)
	rf.mu.Unlock()
	return
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// Term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).


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
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}
//
// reset the election timer
//
func (rf *Raft) resetElectionTimer() {

	rf.electionTimerStartTime = time.Now()
	rf.randTimeOutPeriod = time.Millisecond *
		time.Duration(randomIntInRange(MinElectionTimeOut, MaxElectionTimeOut))
}

func (rf *Raft) startElection() {

	rf.mu.Lock()
	rf.currentTerm++
	rf.setState(Candidate)
	DPrintf("[Server%d]Start an election, it's currentTerm is %d", rf.me, rf.currentTerm)
	count := 0
	finished := 0
	numServers := len(rf.peers)
	count++ // vote for itself
	rf.votedFor = rf.me
	finished++
	rf.resetElectionTimer()
	rf.mu.Unlock()
	cond := sync.NewCond(&rf.mu)
	for i := 0; i < numServers; i++ {

		rf.mu.Lock()
		if i == rf.me {
			rf.mu.Unlock()
			continue
		}
		rf.mu.Unlock()
		go func(x int) {

			args := &RequestVoteArgs{}
			rf.mu.Lock()
			args.CandidateId = rf.me
			args.Term = rf.currentTerm
			rf.mu.Unlock()
			reply := &RequestVoteReply{}
			DPrintf("[Server%d] Send vote request to Server%d", rf.me, x)
			ok := rf.sendRequestVote(x, args, reply)
			if !ok {
				DPrintf("[Server%d]Error when sending vote request to server %d", rf.me, x)
				return
			}
			rf.mu.Lock() // TODO: the lock() is to protect count and finished local variables, not sure if should use another small lock
			defer rf.mu.Unlock()
			if reply.Term > rf.currentTerm {
				DPrintf("[Server%d] Received response for a vote request, but find" +
					"its term is smaller than that in the reply, so it set its term to" +
					"the new term and enter follower state(from candidate state)", rf.me)
				//rf.startNewTerm(reply.Term)
				rf.setState(Follower)
				rf.currentTerm = reply.Term
				rf.votedFor = -1
				rf.resetElectionTimer()
			}
			if reply.VoteGranted {
				count++
			}
			finished++
			cond.Broadcast()
		}(i)
	}

	rf.mu.Lock()

	voteTimeOut := false
	go func() {
		time.Sleep(time.Millisecond * WaitVoteTimeOut)
		voteTimeOut = true
		cond.Broadcast()
	}()
	for count <= numServers / 2 && finished != numServers && rf.state == Candidate && !voteTimeOut {
		cond.Wait()
	}

	if rf.state == Candidate {

		if voteTimeOut {

			DPrintf("[Server%d] Wait votes result for %d milliseconds and time out" +
				"start a new election", rf.me, WaitVoteTimeOut)
			rf.mu.Unlock()
			rf.startElection()
			return
		}
		if count > numServers / 2 {
			DPrintf("Server %d received %d votes, win an election", rf.me, count)
			rf.setState(Leader)
			go func() {

				for {

					if rf.killed() {
						return
					}
					rf.mu.Lock()
					if rf.state != Leader {

						DPrintf("[Server%d]Stop sending heartbeat because it is no longer leader", rf.me)
						rf.resetElectionTimer()
						rf.mu.Unlock()
						return
					}
					rf.mu.Unlock()
					for i := 0; i < numServers; i++ {

						rf.mu.Lock()
						if rf.me == i {
							rf.mu.Unlock()
							continue
						}
						rf.mu.Unlock()
						go func(x int) {

							rf.mu.Lock()
							args := &AppendEntriesArgs{
								Term:     rf.currentTerm,
								LeaderId: rf.me,
							}
							rf.mu.Unlock()
							reply := &AppendEntriesReply{}
							DPrintf("[Server%d] Sent heartbeat to Server%d", args.LeaderId, x)
							ok := rf.sendAppendEntries(x, args, reply)

							if !ok {

								DPrintf("[Server%d]Error when sending heartbeat to Server%d", args.LeaderId, x)
								return
							}

							rf.mu.Lock()
							defer rf.mu.Unlock()
							if reply.Term > rf.currentTerm {
								DPrintf("[Server%d] Received response for a hearbeat, but find" +
									"its term is smaller than that in the reply, so it set its term to" +
									"the new term and enter follower state(from leader state)", rf.me)
								//rf.startNewTerm(reply.Term)
								rf.setState(Follower)
								rf.currentTerm = reply.Term
								rf.votedFor = -1
								rf.resetElectionTimer()
							}
						}(i)
					}
					time.Sleep(time.Millisecond * HeartBeatInterval)
				}
			}()
		} else {
			DPrintf("Server %d received %d votes, lose an election", rf.me, count)
			rf.setState(Follower)
			rf.resetElectionTimer()
		}
	} else {

		DPrintf("[Server%d]End the election not in candidate state", rf.me)
		rf.resetElectionTimer()
	}

	rf.mu.Unlock()
}

//func voteTimeOut(electionStartTime time.Time) bool {
//
//	now := time.Now()
//	return now.After(electionStartTime.Add(time.Millisecond * WaitVoteTimeOut))
//}

func (rf *Raft) electionTimeUp() bool {

	now := time.Now()
	//DPrintf("[Server%d] here", rf.me)
	//fmt.Println("[Server%d]" + now.String(), rf.me)
	//fmt.Println("[Server%d]" +rf.electionTimerStartTime.Add(rf.randTimeOutPeriod).String(),  rf.me)
	if now.After(rf.electionTimerStartTime.Add(rf.randTimeOutPeriod)) {
		DPrintf("[Server%d] Start an election, it last received heartbeat from leader at %s," +
			"and its random timer is %s", rf.me, rf.electionTimerStartTime, rf.randTimeOutPeriod)
		return true
	}
	return false
}

func randomIntInRange(min int, max int) int {

	return rand.Intn(max - min) + min
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
	rf.mu.Lock()
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.resetElectionTimer()
	rf.mu.Unlock()
	go func() {
		for {
			if rf.killed() {
				return
			}
			rf.mu.Lock()
			if rf.electionTimeUp() && rf.state != Leader{
				rf.mu.Unlock()
				rf.startElection()
			} else {
				rf.mu.Unlock()
			}
			time.Sleep(time.Millisecond * time.Duration(ElectionTimerCheckInterval))
		}
	}()
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())


	return rf
}
