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
		rf.raftDPrintf("Received append Entries with lower Term(%d) form Server%d, return false",
			args.Term, args.LeaderId)
		reply.Term = rf.currentTerm
		reply.Success = false
		rf.mu.Unlock()
		return
	}

	if args.Term > rf.currentTerm {
		rf.raftDPrintf("Received append Entries with higher Term(%d) from Server%d, "+
			"enter a new Term and become follower, continue process of this RPC", args.Term, args.LeaderId)
		rf.setState(Follower)
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.resetElectionTimer()
	}

	if args.Term == rf.currentTerm {
		rf.raftDPrintf("Received append Entries with equal Term%d form Server%d, "+
			"continue process of this RPC", args.Term, args.LeaderId)
		rf.resetElectionTimer()
		//DPrintf("[Server%d] Reset it's timer to %s milliseconds after %s",
		//	rf.me, rf.randTimeOutPeriod.String(), rf.electionTimerStartTime.String())
		rf.state = Follower
	}

	// here, the args.Term >= rf.currentTerm
	// both heartbeat and normal append entries contains prevLogIndex, should check and return Success accordingly
	// Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm
	if !rf.containsMatchingLogEntry(args.PrevLogIndex, args.PrevLogTerm) {
		reply.Term = rf.currentTerm
		reply.Success = false
		rf.raftDPrintf("Does not contain an entry at prevLogIndex %d"+
			" whose term matches prevLogTerm %d, return false", args.PrevLogIndex, args.PrevLogTerm)
		rf.mu.Unlock()
		return
	} else {
		reply.Term = rf.currentTerm
		reply.Success = true
		rf.raftDPrintf("Contains an entry at prevLogIndex %d"+
			"whose term matches prevLogTerm %d, return true", args.PrevLogIndex, args.PrevLogTerm)
	}

	// here, the RPC args.Term >= rf.currentTerm
	// and reply is set correctly, it should also check if it is a heartbeat
	// if not, it should check its entry and overwrite with that in the args
	lastNewEntryIndex := args.PrevLogIndex
	if !args.isHeartBeat() {

		// If an existing entry conflicts with a new one (same index
		// but different terms), delete the existing entry and all that
		// follow it
		startIndex := args.PrevLogIndex + 1
		for i, entry := range args.Entries {

			idx := i + startIndex
			if rf.containsLogEntryAtIndex(idx) {

				if !rf.containsMatchingLogEntry(idx, entry.Term) {
					rf.raftDPrintf("Does not contain log entry at index of %d "+
						"whose term is %d, truncate log starting from index of %d",
						idx, entry.Term, idx)
					rf.log = rf.log[:idx]
				} else {
					continue
				}
			}
			rf.log = append(rf.log, entry)
			rf.raftDPrintf("Append entry of term %d at index of %d", entry.Term, idx)
			lastNewEntryIndex = idx
		}
	}

	// The min in the final step (#5) of AppendEntries is necessary,
	// and it needs to be computed with the index of the last new entry.
	// It is not sufficient to simply have the function that applies things
	// from your log between lastApplied and commitIndex stop when it reaches the end of your log.
	// This is because you may have entries in your log that differ
	// from the leader’s log after the entries that the leader sent you
	// (which all match the ones in your log). Because #3 dictates that
	// you only truncate your log if you have conflicting entries, those won’t be removed,
	// and if leaderCommit is beyond the entries the leader sent you, you may apply incorrect entries.
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, lastNewEntryIndex)
		rf.raftDPrintf("After processing append entry RPC from Server%d, "+
			"commit index is now %d", args.LeaderId, rf.commitIndex)
		go rf.applyLogEntries()
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

			rf.raftDPrintf("Stop sending heartbeat because it is no longer leader")
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
	// this goroutine was started by sending append entries goroutine
	// should include check for leader state and prepare args in on critical segment
	// otherwise could send append entries request while not in leader state
	// which is dangerous, because non-leader's append entry RPC could overwrite
	// committed entries in other servers
	if rf.state != Leader {
		rf.raftDPrintf("Find itself no longer leader when preparing args for an " +
			"append entry RPC, stop sending")
		rf.raftDPrintf("De 了一天的 bug 原来在这里")
		rf.mu.Unlock()
		return
	}
	nextIndex := rf.nextIndex[follower]
	prevLogIndex := nextIndex - 1
	//DPrintf("[Server%d] PrevLogIndex to send is %d", rf.me, prevLogIndex)
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

	reply := &AppendEntriesReply{}
	rf.raftDPrintf("Sent append entry RPC to Server%d, "+
		"prevLogIndex to send is %d, prevLogTerm is %d, sent entries range is[%d, %d]",
		follower, prevLogIndex, prevLogTerm, nextIndex, lastIndex)
	rf.mu.Unlock()
	//for {
	//	if rf.killed() {
	//		return
	//	}
	//	ok := rf.sendAppendEntries(follower, args, reply)
	//	if ok {
	//		break
	//	}
	//	DPrintf("[Server%d] Error when sending append entry RPC to server %d, retry", rf.me, follower)
	//}
	ok := rf.sendAppendEntries(follower, args, reply)
	if !ok {
		//DPrintf("[Server%d] Error when sending append entries to Server%d", args.LeaderId, follower)
		return
	}

	//From experience, we have found that by far the simplest thing to do is
	//to first record the term in the reply (it may be higher than your current term),
	//and then to compare the current term with the term you sent in your original RPC.
	//	If the two are different, drop the reply and return. Only if the two terms are
	//the same should you continue processing the reply.
	rf.mu.Lock()
	if rf.currentTerm != sentCurrentTerm {

		rf.raftDPrintf("Received response for an append entry request, "+
			"but find its current term does not equal to the term when sending the request(term %d)"+
			", discard response", sentCurrentTerm)
		rf.mu.Unlock()
		return
	}

	if reply.Term > rf.currentTerm {
		rf.raftDPrintf("Received response for an append entry RPC , but find "+
			"its Term is smaller than that in the reply(Term %d), so it set its Term to "+
			"the new Term and enter follower state(from leader state), does not process the response", reply.Term)
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
		rf.raftDPrintf("Updated server%d's nextIndex to %d because it received "+
			"success reply of append entry RPC up to %d", follower, lastIndex+1, lastIndex)
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
		rf.raftDPrintf("Majority agrees on %d, set commit index to %d", rf.commitIndex, rf.commitIndex)
		go rf.applyLogEntries()
		rf.mu.Unlock()
		return
	}

	rf.mu.Lock()

	//// Server return false if term < currentTerm
	//// in this case the Success argument in reply
	//// does not contain information about nextIndex
	//// should return here
	//if reply.Term > sentCurrentTerm {
	//
	//	rf.mu.Unlock()
	//	return
	//}

	//If AppendEntries fails because of log inconsistency: decrement nextIndex and retry(retry after next heartbeat interval
	rf.nextIndex[follower]--
	rf.raftDPrintf("Append entries of PrevLogIndex %d,"+
		" PrevLogTerm %d fails, decrement nextIndex "+
		"for follower %d", prevLogIndex, prevLogTerm, follower)
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
				rf.raftDPrintf("Sending apply message of log at index %d", i)
				rf.applyCh <- msg
				rf.lastApplied = i
			}
			rf.mu.Unlock()
		}()

	}
	rf.mu.Unlock()
}
