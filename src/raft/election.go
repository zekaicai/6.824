package raft

import (
	"math/rand"
	"time"
)

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
		DPrintf("[Server%d] Vote false for request from Server%d, "+
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
		DPrintf("[Server%d] Vote true for request from Server%d", rf.me, args.CandidateId)
		rf.resetElectionTimer()
		rf.mu.Unlock()
		return
	}

	reply.Term = rf.currentTerm
	reply.VoteGranted = false
	DPrintf("[Server%d] Vote false for request from Server%d,"+
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
	DPrintf("[Server%d] Start an election, it's currentTerm is %d", rf.me, rf.currentTerm)
	count := 0
	finished := 0
	numServers := len(rf.peers)
	count++ // vote for itself
	rf.votedFor = rf.me
	finished++
	rf.resetElectionTimer()
	rf.waitElectionTimeOut = false
	rf.mu.Unlock()

	for i := 0; i < numServers; i++ {

		if rf.killed() {
			return
		}

		rf.mu.Lock()
		if i == rf.me {
			rf.mu.Unlock()
			continue
		}
		rf.mu.Unlock()

		go rf.requestVoteAndHandleReply(i, &count, &finished)
	}

	rf.waitForVoteResult(&count, &finished)
}

func (rf *Raft) waitForVoteResult(count *int, finished *int) {

	rf.mu.Lock()
	numServers := len(rf.peers)

	for *count <= numServers/2 && *finished != numServers && rf.state == Candidate && !rf.waitElectionTimeOut && !rf.killed() {
		rf.cond.Wait()
	}

	if rf.killed() {
		return
	}

	if rf.waitElectionTimeOut {
		DPrintf("[Server%d] Times out when waiting for result of the election"+
			", so it ends the current election thread, the timer will start a new election thread", rf.me)
		rf.mu.Unlock()
		return
	}

	if rf.state == Candidate {

		if *count > numServers/2 {
			DPrintf("[Server%d] Received %d votes, win an election", rf.me, *count)
			rf.setState(Leader)
			go rf.sendHeartbeat()
		} else {
			DPrintf("[Server%d] Received %d votes, lose an election", rf.me, *count)
			rf.setState(Follower)
			rf.resetElectionTimer()
		}
	} else {

		DPrintf("[Server%d] End the election not in candidate state", rf.me)
		rf.resetElectionTimer()
	}

	rf.mu.Unlock()
}

func (rf *Raft) sendHeartbeat() {

	for {

		if rf.killed() {
			return
		}
		rf.mu.Lock()
		numServers := len(rf.peers)
		if rf.state != Leader {

			DPrintf("[Server%d] Stop sending heartbeat because it is no longer leader", rf.me)
			rf.resetElectionTimer()
			rf.mu.Unlock()
			return
		}
		rf.mu.Unlock()
		for i := 0; i < numServers; i++ {

			if rf.killed() {
				return
			}
			rf.mu.Lock()
			if rf.me == i {
				rf.mu.Unlock()
				continue
			}
			rf.mu.Unlock()
			go rf.sendHeartbeatAndHandleReply(i)
		}
		time.Sleep(time.Millisecond * time.Duration(HeartBeatInterval))
	}
}

func (rf *Raft) sendHeartbeatAndHandleReply(follower int) {

	rf.mu.Lock()
	args := &AppendEntriesArgs{
		Term:     rf.currentTerm,
		LeaderId: rf.me,
	}
	rf.mu.Unlock()
	reply := &AppendEntriesReply{}
	DPrintf("[Server%d] Sent heartbeat to Server%d", args.LeaderId, follower)
	ok := rf.sendAppendEntries(follower, args, reply)

	if !ok {

		DPrintf("[Server%d] Error when sending heartbeat to Server%d", args.LeaderId, follower)
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()
	if reply.Term > rf.currentTerm {
		DPrintf("[Server%d] Received response for a hearbeat, but find"+
			"its term is smaller than that in the reply, so it set its term to"+
			"the new term and enter follower state(from leader state)", rf.me)
		rf.setState(Follower)
		rf.currentTerm = reply.Term
		rf.votedFor = -1
		rf.resetElectionTimer()
	}
}

func (rf *Raft) requestVoteAndHandleReply(voter int, count, finished *int) {

	args := &RequestVoteArgs{}
	rf.mu.Lock()
	args.CandidateId = rf.me
	args.Term = rf.currentTerm
	rf.mu.Unlock()
	reply := &RequestVoteReply{}
	DPrintf("[Server%d] Send vote request to Server%d", rf.me, voter)
	ok := rf.sendRequestVote(voter, args, reply)
	if !ok {
		DPrintf("[Server%d] Error when sending vote request to server %d", rf.me, voter)
		return
	}
	rf.mu.Lock() // TODO: the lock() is to protect count and finished local variables, not sure if should use another small lock
	defer rf.mu.Unlock()
	if reply.Term > rf.currentTerm {
		DPrintf("[Server%d] Received response for a vote request, but find"+
			"its term is smaller than that in the reply, so it set its term to"+
			"the new term and enter follower state(from candidate state)", rf.me)
		rf.setState(Follower)
		rf.currentTerm = reply.Term
		rf.votedFor = -1
		rf.resetElectionTimer()
	}
	if reply.VoteGranted {
		*count++
	}
	*finished++
	rf.cond.Broadcast()
}

func (rf *Raft) electionTimeUp() bool {

	now := time.Now()
	if now.After(rf.electionTimerStartTime.Add(rf.randTimeOutPeriod)) {
		return true
	}
	return false
}

func randomIntInRange(min int, max int) int {

	return rand.Intn(max-min) + min
}

func (rf *Raft) timerStart() {
	for {
		if rf.killed() {
			return
		}
		rf.mu.Lock()
		if rf.electionTimeUp() && rf.state != Leader {
			if rf.state == Candidate {
				rf.waitElectionTimeOut = true
				rf.cond.Broadcast()
			}
			go rf.startElection()
		}
		rf.mu.Unlock()
		time.Sleep(time.Millisecond * time.Duration(ElectionTimerCheckInterval))
	}
}

func (rf *Raft) setState(state int) {

	rf.state = state
}
