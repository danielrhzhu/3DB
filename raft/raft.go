package raft

import (
	"3db/networking"
	"math"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

// RPC defintions
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

// send to client channel
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

// raft-level types/enums
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
	//volatile on all servers
	mu              sync.Mutex              // Lock to protect shared access to this peer's state
	peers           []*networking.ClientEnd // RPC end points of all peers
	persister       *Persister              // Object to hold this peer's persisted state
	me              int                     // this peer's index into peers[]
	dead            int32                   // set by Kill()
	state           int                     // NUM to represent follower, candidate, and leader states
	applyCh         chan ApplyMsg           // channel to send logs to commit on
	commitCh        chan struct{}
	electionTimeout time.Time
	commitIndex     int
	lastApplied     int

	//persistent on all servers
	currentTerm int
	votedFor    int
	log         []LogEntry

	//volatile on leaders
	nextIndex  []int // index of highest log entry known to be replicated on server[i]
	matchIndex []int // index of the next log entry to be sent to the server
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

// // Seperate, long-running gourtine that sends committed log entries in order on the applyCh.
func (rf *Raft) commitLogEntries() {
	for !rf.killed() {
		for range rf.commitCh {
			rf.mu.Lock()
			rf.applyCh <- ApplyMsg{}
			rf.mu.Unlock()
		}
	}
}

func (rf *Raft) GetState() (int, bool) { // currentTerm, isLeader
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.state == LEADER
}

func Make(peers []*networking.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.state = FOLLOWER
	rf.currentTerm = 0
	rf.votedFor = -1

	rf.applyCh = applyCh
	rf.commitIndex = -1
	rf.lastApplied = -1

	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	rf.readPersist(persister.ReadRaftState())

	//listens for heartbeats
	go rf.ticker()

	//listens for events to apply to applyCh
	go rf.commitLogEntries()

	return rf
}

// API for user to send commands to
func (rf *Raft) Start(command interface{}) (int, int, bool) {

	rf.mu.Lock()
	isLeader := rf.state == LEADER
	term := rf.currentTerm

	if !isLeader {
		rf.mu.Unlock()
		return rf.nextIndex[rf.me], term, false
	}

	rf.log = append(rf.log, LogEntry{Term: term, Command: command})
	rf.mu.Unlock()
	rf.sendHeartbeats()

	return rf.nextIndex[rf.me], term, true
}

// expects lock
func (rf *Raft) getLastLogIdxAndTerm() (int, int) {
	if len(rf.log) > 0 {
		idx := len(rf.log) - 1
		return idx, rf.log[idx].Term
	}
	return -1, -1
}

// returns whether (idx1,term1) is more up to date than (term1, term2)
func isLogMoreUpToDate(idx1 int, term1 int, idx2 int, term2 int) bool {
	if term1 == term2 {
		return idx1 > idx2
	}
	return term1 > term2
}

// RV RPC handler
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
		rf.becomeFollower(args.Term)
		reply.Term = rf.currentTerm
		reply.VoteGranted = true
		return
	}
	//if the candidate's log is not more up to date than the server, do not grant vote
	lastLogIdx, term := rf.getLastLogIdxAndTerm()
	if rf.votedFor != -1 || !isLogMoreUpToDate(args.LastLogIndex, args.Term, lastLogIdx, term) {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	rf.electionTimeout = time.Now()
	rf.votedFor = args.CandidateId

	reply.VoteGranted = true
	reply.Term = rf.currentTerm
}

// AE RPC handler
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term > rf.currentTerm {
		rf.becomeFollower(args.Term)
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	if args.Term < rf.currentTerm {
		reply.Success = false
		reply.Term = rf.currentTerm
		return
	}

	if rf.state != FOLLOWER {
		rf.becomeFollower(args.Term)
	}

	rf.electionTimeout = time.Now()

	log := rf.log
	// lastLogIdx, term := rf.getLastLogIdxAndTerm()
	//prevlogIndex == -1 if there have been no entries commited yet
	//if log doesnt contain an entry at prevlogindex or it does not match leader's
	if !(args.PrevLogIndex == -1 ||
		(args.PrevLogIndex < len(rf.log) && args.PrevLogTerm == rf.log[args.PrevLogIndex].Term)) {
		reply.Success = false
		reply.Term = rf.currentTerm
		return

	}

	// if args.PrevLogIndex > 0 && !isLogMoreUpToDate(args.PrevLogIndex, args.PrevLogTerm) {
	// }

	//if an existing entry conflicts with a new one (same index but diff terms)
	// delete the existing entry and all that follow
	matchIdx := args.PrevLogIndex + 1

	for i := matchIdx; i < len(args.Entries); i++ {
		//point where leader has more entries or point where logs diverge
		if i >= len(log) || log[i].Term != args.Entries[i].Term {
			//append any new entries not in log,
			//note: follower can have more entries that leader, which is fine
			rf.log = rf.log[:i]
			rf.log = append(rf.log, args.Entries[i:]...)
			break
		}
	}

	//if the leader's commit index is greater than the current follower's commit index
	//update the current node's commit index
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = int(math.Min(float64(args.LeaderCommit), float64(len(log))))
		rf.commitCh <- struct{}{}
	}

	reply.Term = rf.currentTerm
	reply.Success = false
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

// tells Start() whether operations are replicated to a majority of servers
// or opeartions were  (this is )
func (rf *Raft) sendHeartbeats() {

	rf.mu.Lock()
	if rf.state != LEADER {
		rf.mu.Unlock()
		return
	}

	savedCurrentTerm := rf.currentTerm
	rf.mu.Unlock()
	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}

		go func(peer int) {
			rf.mu.Lock()
			peerNextIndex := rf.nextIndex[peer]

			entriesToSend := rf.log[peerNextIndex:]
			if peerNextIndex >= 0 {
				entriesToSend = rf.log[peerNextIndex:]
			}

			prevLogTerm := -1
			prevLogIndex := peerNextIndex - 1
			//this means that there will be a prev log term

			args := AppendEntriesArgs{
				Term:         savedCurrentTerm,
				LeaderId:     rf.me,
				PrevLogIndex: prevLogIndex,
				PrevLogTerm:  prevLogTerm,
				Entries:      entriesToSend,
				LeaderCommit: rf.commitIndex,
			}
			rf.mu.Unlock()

			reply := AppendEntriesReply{}
			ok := rf.sendAppendEntries(peer, &args, &reply)

			rf.mu.Lock()
			defer rf.mu.Unlock()
			if ok {
				if reply.Term > savedCurrentTerm {
					rf.becomeFollower(reply.Term)
					return
				}

				if rf.state != LEADER {
					return
				}

				if savedCurrentTerm != reply.Term {
					return
				}

				if !reply.Success {
					rf.nextIndex[peer] = peerNextIndex - 1
					return
				}

				nextIdx := peerNextIndex + len(entriesToSend)
				rf.nextIndex[peer] = nextIdx
				rf.matchIndex[peer] = nextIdx - 1

				savedCommitIdx := rf.commitIndex

				//if there exists an N such that N > commitIndex,
				//AND a majority of matchindex[i] >= N
				//And log[N].term == currentTerm
				for i := savedCommitIdx + 1; i < len(rf.log); i++ {
					if rf.log[i].Term == rf.currentTerm {

						cnt := 1

						for j := range rf.peers {
							if rf.matchIndex[j] >= i {
								cnt++
							}
							//if we reach a quorom, change the commit index
							if cnt > len(rf.peers)/2 {
								rf.commitIndex = i
							}
						}
					}
				}
				//if we reach a quorom, send message to commitCH to commit the current Index
				if savedCommitIdx != rf.commitIndex {
					rf.commitCh <- struct{}{}
				}

			}

		}(peer)
	}
}

// rf is already locked when called
func (rf *Raft) becomeLeader() {
	rf.state = LEADER

	for peer := range rf.peers {
		rf.nextIndex[peer] = len(rf.log)
		rf.matchIndex[peer] = -1
	}

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

	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}

		go func(peer int) {
			rf.mu.Lock()
			args := RequestVoteArgs{}
			args.CandidateId = rf.me
			args.Term = savedCurrentTerm
			args.LastLogIndex = len(rf.log) - 1
			if len(rf.log) > 0 {
				args.LastLogTerm = rf.log[len(rf.log)-1].Term
			} else {
				args.LastLogTerm = -1
			}

			rf.mu.Unlock()
			reply := RequestVoteReply{}
			ok := rf.sendRequestVote(peer, &args, &reply)

			if ok {
				rf.mu.Lock()
				defer rf.mu.Unlock()

				if rf.state != CANDIDATE {
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

		}(peer)
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
		if rf.state != FOLLOWER && rf.state != LEADER {
			rf.mu.Unlock()
			return
		}

		//received a heartbeat that updated us
		if currentTerm != rf.currentTerm {
			rf.mu.Unlock()
			return
		}

		elapsed := time.Since(rf.electionTimeout)

		if elapsed >= timeoutDuration {
			rf.startElection()
			rf.mu.Unlock()
			return
		}
		rf.mu.Unlock()

	}
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
