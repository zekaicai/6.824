package raft

type AppendEntriesArgs struct {
	Term     int // leader's term
	LeaderId int // so follower can redirect clients
}

type AppendEntriesReply struct {
	Term int // current term, for leader to update itself
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {

	rf.mu.Lock()
	if args.Term > rf.currentTerm {
		DPrintf("[Server%d] Received append entries with higher term from %d, "+
			"enter a new term and become follower", rf.me, args.LeaderId)
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
		DPrintf("[Server%d] Received append entries with lower term form Server%d", rf.me, args.LeaderId)
		reply.Term = rf.currentTerm
		rf.mu.Unlock()
		return
	}
	if args.Term == rf.currentTerm {
		DPrintf("[Server%d] Received append entries with equal term form Server%d", rf.me, args.LeaderId)
		rf.resetElectionTimer()
		DPrintf("[Server%d] Reset it's timer to %s milliseconds after %s",
			rf.me, rf.randTimeOutPeriod.String(), rf.electionTimerStartTime.String())
		rf.state = Follower
		reply.Term = rf.currentTerm
		rf.mu.Unlock()
		return
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}
