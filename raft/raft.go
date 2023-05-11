package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"3db/networking"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}
type LogEntry struct {
	Term    int
	Command interface{}
}

const (
	FOLLOWER  = 0
	CANDIDATE = 1
	LEADER    = 2
)

// A Go object implementing a single Raft peer.
type Raft struct {
	mu              sync.Mutex              // Lock to protect shared access to this peer's state
	peers           []*networking.ClientEnd // RPC end points of all peers
	persister       *Persister              // Object to hold this peer's persisted state
	me              int                     // this peer's index into peers[]
	dead            int32                   // set by Kill()
	state           int                     // NUM to represent follower, candidate, and leader states
	electionTimeout time.Time
	applyCh         chan ApplyMsg // channel to send logs to commit on
	currentTerm     int
	votedFor        int
	log             []LogEntry
	commitIndex     int
	lastApplied     int
	nextIndex       []int //if leader, keep index of new nog entry the leader will send to follower
	matchIndex      []int
}

type RequestVoteArgs struct {
	CandidateId  int
	Term         int
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) GetState() (int, bool) { // currentTerm, isLeader
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.state == LEADER
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	//an old leader is outdated — let them know so they can update
	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		reply.Term = args.Term
		return
	}

	//the server is outdated — return to follower state
	if args.Term > rf.currentTerm {
		rf.state = FOLLOWER
		rf.currentTerm = args.Term
		rf.votedFor = args.CandidateId

		reply.Term = rf.currentTerm
		reply.VoteGranted = true
		return
	}

	log := rf.log
	//if the candidate's log is not more up to date than the server, do not grant vote
	if len(log) <= args.LastLogIndex || log[args.LastLogIndex].Term != args.LastLogTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	reply.VoteGranted = true
	reply.Term = rf.currentTerm

}

// this will use context from previous nextIndex to
func (rf *Raft) buildLogEntries(logEntries []LogEntry) {}

func (rf *Raft) sendLogEntries(term int) {}

// modify appendentry handler - if logentries is empty, interpret as heartbeat
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term > rf.currentTerm {
		rf.becomeFollower(args.Term)
		return
	}
	reply.Success = false
	if args.Term == rf.currentTerm {
		if rf.state != FOLLOWER {
			rf.becomeFollower(args.Term)
		}
		rf.electionTimeout = time.Now()
		reply.Success = true
	}
	reply.Term = rf.currentTerm
}

func (rf *Raft) Start(command interface{}) (int, int, bool) {

	rf.mu.Lock()
	defer rf.mu.Unlock()
	isLeader := rf.state == LEADER
	term := rf.currentTerm

	if !isLeader {
		return rf.nextIndex[rf.me], term, false
	}

	rf.matchIndex[rf.me] = rf.nextIndex[rf.me]
	rf.nextIndex[rf.me]++

	rf.log = append(rf.log, LogEntry{Term: term, Command: command})

	return rf.nextIndex[rf.me], term, true
}

func (rf *Raft) getElectionTimeout() time.Duration {
	electionTimeout := rand.Intn(250) + 250
	return time.Duration(electionTimeout) * time.Millisecond
}

func (rf *Raft) becomeFollower(term int) {
	//expects existing lock state
	rf.currentTerm = term
	rf.state = FOLLOWER
	rf.votedFor = -1
	rf.electionTimeout = time.Now()
	go rf.runElectionTimer()

}

func (rf *Raft) sendHeartbeats() {

	rf.mu.Lock()
	if rf.state != LEADER {
		rf.mu.Unlock()
		return
	}

	savedCurrentTerm := rf.currentTerm
	rf.mu.Unlock()

	for idx, _ := range rf.peers {
		if idx == rf.me {
			continue
		}
		args := AppendEntriesArgs{}
		args.LeaderId = rf.me
		args.Term = savedCurrentTerm

		go func(idx int) {
			reply := AppendEntriesReply{}

			ok := rf.sendAppendEntries(idx, &args, &reply)

			rf.mu.Lock()
			defer rf.mu.Unlock()
			if ok {
				if rf.state != LEADER {
					rf.becomeFollower(reply.Term)
					return
				}
			}

		}(idx)
	}
}

// rf is already locked when called
func (rf *Raft) becomeLeader() {
	rf.state = LEADER

	go func() {
		for {
			rf.sendHeartbeats()
			time.Sleep(100 * time.Millisecond)
			rf.mu.Lock()
			if rf.state != LEADER {
				rf.mu.Unlock()
				return
			}
			rf.mu.Unlock()
		}

	}()

}

// locked when function is called
func (rf *Raft) startElection() {
	rf.state = CANDIDATE
	rf.currentTerm++
	rf.votedFor = rf.me
	savedCurrentTerm := rf.currentTerm

	rf.electionTimeout = time.Now()

	totalVotes := 1

	for idx := range rf.peers {
		if idx == rf.me {
			continue
		}
		go func(peerId int) {
			args := RequestVoteArgs{}
			args.CandidateId = rf.me
			args.Term = savedCurrentTerm

			reply := RequestVoteReply{}

			ok := rf.sendRequestVote(peerId, &args, &reply)

			if ok {
				rf.mu.Lock()
				defer rf.mu.Unlock()

				if rf.state != FOLLOWER {
					return
				}

				if reply.Term > savedCurrentTerm {
					rf.becomeFollower(reply.Term)
					return
				}

				if reply.Term == savedCurrentTerm {
					if reply.VoteGranted {
						totalVotes += 1
						if totalVotes > len(rf.peers)/2 {
							rf.becomeLeader()
							return
						}
					}
				}

			}

		}(idx)
	}

	go rf.runElectionTimer()

}

func (rf *Raft) runElectionTimer() {
	timeoutDuration := rf.getElectionTimeout()
	rf.mu.Lock()
	currentTerm := rf.currentTerm
	rf.mu.Unlock()

	for {
		time.Sleep(10 * time.Millisecond)
		rf.mu.Lock()
		if rf.state != CANDIDATE && rf.state != FOLLOWER {
			rf.mu.Unlock()
			break
		}

		//received a heartbeat that updated us
		if currentTerm != rf.currentTerm {
			rf.mu.Unlock()
			break
		}

		elapsed := time.Since(rf.electionTimeout)

		if elapsed >= timeoutDuration {
			rf.startElection()
			rf.mu.Unlock()
			break
		}
		rf.mu.Unlock()

	}
}

// The ticker go routine starts a new election if this peer hasn't received heartsbeats recently.
func (rf *Raft) ticker() {
	for !rf.killed() {

		rf.mu.Lock()
		rf.electionTimeout = time.Now()
		rf.mu.Unlock()
		rf.runElectionTimer()
	}
}

// Seperate, long-running gourtine that sends committed log entries in order on the applyCh.
func (rf *Raft) appendLogEntries() {
	for !rf.killed() {
		select {
		case <-rf.applyCh:
			//do something
		}
	}
}

func Make(peers []*networking.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	rf.currentTerm = 0
	rf.votedFor = -1
	rf.state = 0
	rf.applyCh = applyCh
	rf.commitIndex = 0

	rf.readPersist(persister.ReadRaftState())

	//long running goroutines
	go rf.ticker()
	go rf.appendLogEntries()

	return rf
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 {
		return
	}
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
func (rf *Raft) persist() {}

// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {
	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {}
