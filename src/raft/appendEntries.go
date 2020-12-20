package raft

import (
	"time"
)

type AppendEntriesArgs struct {
	Term         int        // leader's Term
	LeaderId     int        // so follower can redirect clients
	PrevLogIndex int        // index of log entry immediately preceding new ones
	PrevLogTerm  int        // Term of PrevLogIndex entry
	Entries      []LogEntry // log Entries to store (empty for heartbeat; may send more than one for efficiency)
	LeaderCommit int        // leader’s commitIndex
}

func (args *AppendEntriesArgs) isHeartBeat() bool {
	return args.Entries == nil || len(args.Entries) == 0
}

type AppendEntriesReply struct {
	Term    int  // current Term, for leader to update itself
	Success bool // true if follower contained entry matching PrevLogIndex and PrevLogTerm
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {

	rf.mu.Lock()
	if args.Term < rf.currentTerm {
		DPrintf("[Server%d] Received append Entries with lower Term form Server%d", rf.me, args.LeaderId)
		reply.Term = rf.currentTerm
		reply.Success = false
		rf.mu.Unlock()
		return
	}

	if args.Term > rf.currentTerm {
		DPrintf("[Server%d] Received append Entries with higher Term from %d, "+
			"enter a new Term and become follower", rf.me, args.LeaderId)
		rf.setState(Follower)
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.resetElectionTimer()
	}

	if args.Term == rf.currentTerm {
		DPrintf("[Server%d] Received append Entries with equal Term form Server%d", rf.me, args.LeaderId)
		rf.resetElectionTimer()
		DPrintf("[Server%d] Reset it's timer to %s milliseconds after %s",
			rf.me, rf.randTimeOutPeriod.String(), rf.electionTimerStartTime.String())
		rf.state = Follower
	}

	// here, the args.Term >= rf.currentTerm
	// both heartbeat and normal append entries contains prevLogIndex, should check and return Success accordingly
	// Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm
	if !rf.containsMatchingLogEntry(args.PrevLogIndex, args.PrevLogTerm) {
		reply.Term = rf.currentTerm
		reply.Success = false
		DPrintf("[Server%d] Does not contain an entry at prevLogIndex "+
			"whose term matches prevLogTerm, return false", rf.me)
		rf.mu.Unlock()
		return
	} else {
		reply.Term = rf.currentTerm
		reply.Success = true
		DPrintf("[Server%d] Contains an entry at prevLogIndex "+
			"whose term matches prevLogTerm, return true", rf.me)
	}

	// here, the RPC args.Term >= rf.currentTerm
	// and reply is set correctly, it should also check if it is a heartbeat
	// if not, it should check its entry and overwrite with that in the args
	if !args.isHeartBeat() {

		// If an existing entry conflicts with a new one (same index
		// but different terms), delete the existing entry and all that
		// follow it
		startIndex := args.PrevLogIndex + 1
		for i, entry := range args.Entries {

			idx := i + startIndex
			if rf.containsLogEntryAtIndex(idx) {

				if !rf.containsMatchingLogEntry(idx, entry.Term) {
					rf.log = rf.log[:idx]
				} else {
					continue
				}
			}
			rf.log = append(rf.log, entry)
		}
	}

	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, len(rf.log)-1)
		go rf.applyLogEntries()
		DPrintf("[Server%d] commit index is now %d", rf.me, rf.commitIndex)
	}

	rf.mu.Unlock()
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) startAppendEntries() {

	for {

		if rf.killed() {
			return
		}
		rf.mu.Lock()
		numServers := len(rf.peers)
		if rf.state != Leader {

			DPrintf("[Server%d] Stop sending heartbeat because it is no longer leader", rf.me)
			//rf.resetElectionTimer()
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
			go rf.sendAppendEntriesAndHandleReply(i)
		}
		time.Sleep(time.Millisecond * time.Duration(HeartBeatInterval))
	}
}

//func (rf *Raft) sendHeartbeatAndHandleReply(follower int) {
//
//	rf.mu.Lock()
//
//	args := &AppendEntriesArgs{
//		Term:         rf.currentTerm,
//		LeaderId:     rf.me,
//		LeaderCommit: rf.commitIndex,
//	}
//	rf.mu.Unlock()
//	reply := &AppendEntriesReply{}
//	DPrintf("[Server%d] Sent heartbeat to Server%d", args.LeaderId, follower)
//	ok := rf.sendAppendEntries(follower, args, reply)
//
//	if !ok {
//
//		// TODO: seems sending don't need to retry if sending heartbeat fails
//		DPrintf("[Server%d] Error when sending heartbeat to Server%d", args.LeaderId, follower)
//		return
//	}
//
//	rf.mu.Lock()
//	defer rf.mu.Unlock()
//	if reply.Term > rf.currentTerm {
//		DPrintf("[Server%d] Received response for a hearbeat, but find"+
//			"its Term is smaller than that in the reply, so it set its Term to"+
//			"the new Term and enter follower state(from leader state)", rf.me)
//		rf.setState(Follower)
//		rf.currentTerm = reply.Term
//		rf.votedFor = -1
//		rf.resetElectionTimer()
//	}
//}

func (rf *Raft) sendAppendEntriesAndHandleReply(follower int) {

	rf.mu.Lock()

	nextIndex := rf.nextIndex[follower]
	prevLogIndex := nextIndex - 1
	DPrintf("[Server%d] PrevLogIndex to send is %d", rf.me, prevLogIndex)
	prevLogEntry := rf.log[prevLogIndex]
	prevLogTerm := prevLogEntry.Term
	// because leader sent entries to the end,
	// so the next next index is it's length, record it now because length could be modified later
	lastIndex := len(rf.log) - 1
	sentCurrentTerm := rf.currentTerm
	args := &AppendEntriesArgs{
		Term:         sentCurrentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      rf.log[nextIndex:],
		LeaderCommit: rf.commitIndex,
	}

	rf.mu.Unlock()

	reply := &AppendEntriesReply{}
	DPrintf("[Server%d] Sent append entry RPC to Server%d", args.LeaderId, follower)

	for {
		if rf.killed() {
			return
		}
		ok := rf.sendAppendEntries(follower, args, reply)
		if ok {
			break
		}
		DPrintf("[Server%d] Error when sending append entry RPC to server %d, retry", rf.me, follower)
	}

	rf.mu.Lock()
	if reply.Term > rf.currentTerm {
		DPrintf("[Server%d] Received response for an append entry RPC , but find"+
			"its Term is smaller than that in the reply, so it set its Term to"+
			"the new Term and enter follower state(from leader state)", rf.me)
		rf.setState(Follower)
		rf.currentTerm = reply.Term
		rf.votedFor = -1
		rf.resetElectionTimer()
		rf.mu.Unlock()
		return
	}
	rf.mu.Unlock()

	if reply.Success {
		rf.mu.Lock()
		// If successful: update nextIndex and matchIndex for follower (§5.3)
		rf.nextIndex[follower] = lastIndex + 1
		rf.matchIndex[follower] = lastIndex
		DPrintf("[Server%d] Updated server%d's nextIndex to %d because it received "+
			"success reply of append entry RPC up to %d", rf.me, follower, lastIndex+1, lastIndex)
		// TODO: here it should not conclude the commit index should update, If there exists an N such that N > commitIndex, a majority of matchIndex[i] ≥ N, and log[N].term == currentTerm: set commitIndex = N (§5.3, §5.4)
		// maybe should use another goroutine to check when commit index should be updated when matchIndex changes, which is here, maybe use a cond vatiable should use another goroutine to check when commit index should be updated when matchIndex changes, which is here, maybe use a cond vatiable
		newCommitIndex := lastIndex
		//DPrintf("newCommitIndex = %d", newCommitIndex)
		//DPrintf("rf.commitIndex = %d", rf.commitIndex)
		for ; newCommitIndex > rf.commitIndex; newCommitIndex-- {
			//DPrintf("newCommitIndex = %d", newCommitIndex)
			if rf.majorityAgreesOnIndex(newCommitIndex) && rf.log[newCommitIndex].Term == rf.currentTerm {
				break
			}
		}
		rf.commitIndex = newCommitIndex
		DPrintf("[Server%d] Commit index is now %d", rf.me, rf.commitIndex)
		go rf.applyLogEntries()
		rf.mu.Unlock()
		return
	}

	rf.mu.Lock()
	// Server return false if term < currentTerm
	// in this case the Success argument in reply
	// does not contain information about nextIndex
	// should return here
	if reply.Term > sentCurrentTerm {

		rf.mu.Unlock()
		return
	}
	//If AppendEntries fails because of log inconsistency: decrement nextIndex and retry(retry after next heartbeat interval
	rf.nextIndex[follower]--
	DPrintf("[Server%d] Append entries of PrevLogIndex %d,"+
		" PrevLogTerm %d fails, decrement nextIndex "+
		"for follower %d", rf.me, prevLogIndex, prevLogTerm, follower)
	rf.mu.Unlock()
}

func (rf *Raft) trySendingAppendEntriesTo(follower int, args *AppendEntriesArgs, reply *AppendEntriesReply) {

	for {
		if rf.killed() {
			return
		}
		ok := rf.sendAppendEntries(follower, args, reply)
		if ok {
			break
		}
		DPrintf("[Server%d] Error when sending append entry RPC to server %d, retry", rf.me, follower)
	}
}

func (rf *Raft) applyLogEntries() {

	// If commitIndex > lastApplied: increment lastApplied, apply
	// log[lastApplied] to state machine (§5.3)
	rf.mu.Lock()
	if rf.commitIndex > rf.lastApplied {

		go func() {

			rf.mu.Lock()
			for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {

				entry := rf.log[i]
				msg := ApplyMsg{
					CommandValid: true,
					Command:      entry.Command,
					CommandIndex: i,
				}
				DPrintf("[Server%d] Sending apply message of log at index %d", rf.me, i)
				rf.applyCh <- msg
				rf.lastApplied = i
			}
			rf.mu.Unlock()
		}()

	}
	rf.mu.Unlock()
}
