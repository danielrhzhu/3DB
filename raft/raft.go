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
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"danieldb/networking"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex              // Lock to protect shared access to this peer's state
	peers     []*networking.ClientEnd // RPC end points of all peers
	persister *Persister              // Object to hold this peer's persisted state
	me        int                     // this peer's index into peers[]
	dead      int32                   // set by Kill()

	votedFor        int
	state           int
	log             []LogEntry
	currentTerm     int
	electionTimeout time.Time
	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.state == 2
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
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

// restore previously persisted state.
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

// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

type RequestVoteArgs struct {
	CandidateId int
	Term        int
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

type LogEntry struct {
}

type AppendEntriesArgs struct {
	Term     int
	LeaderId int
	Entries  []LogEntry
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	//the leader is outdated — let them know so they can update
	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		reply.Term = args.Term
		return
	}

	//the server is outdated — return to follower state
	if args.Term > rf.currentTerm {
		rf.state = 0
		rf.currentTerm = args.Term
		rf.votedFor = args.CandidateId

		reply.Term = rf.currentTerm
		reply.VoteGranted = true
		return
	}
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term > rf.currentTerm {
		rf.becomeFollower(args.Term)
		return
	}
	reply.Success = false
	if args.Term == rf.currentTerm {
		if rf.state != 0 {
			rf.becomeFollower(args.Term)
		}
		rf.electionTimeout = time.Now()
		reply.Success = true
	}
	reply.Term = rf.currentTerm
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The networking package simulates a lossy network, in which servers
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
// look at the comments in ../networking/networking.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

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
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
}

func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) getElectionTimeout() time.Duration {
	electionTimeout := rand.Intn(250) + 250
	return time.Duration(electionTimeout) * time.Millisecond
}

func (rf *Raft) becomeFollower(term int) {
	//expects existing lock state
	// fmt.Printf("S%d has become a follower with term %d\n", rf.me, term)
	rf.currentTerm = term
	rf.state = 0
	rf.votedFor = -1
	rf.electionTimeout = time.Now()
	go rf.runElectionTimer()

}

func (rf *Raft) sendHeartbeats() {

	rf.mu.Lock()
	if rf.state != 2 {
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
				if rf.state != 2 {
					rf.becomeFollower(reply.Term)
					return
				}
			}

		}(idx)
	}
}

func (rf *Raft) startLeader() {
	//expects locking state
	rf.state = 2
	// fmt.Printf("S%d: Become leader for term %d", rf.me, rf.currentTerm)
	//send heartbeats while leader
	go func() {
		for {
			rf.sendHeartbeats()
			time.Sleep(100 * time.Millisecond)
			rf.mu.Lock()
			if rf.state != 2 {
				rf.mu.Unlock()
				return
			}
			rf.mu.Unlock()
		}

	}()

}

func (rf *Raft) startElection() {
	//locked when function is called
	rf.state = 1
	rf.currentTerm += 1
	rf.votedFor = rf.me
	savedCurrentTerm := rf.currentTerm

	rf.electionTimeout = time.Now()

	// fmt.Printf("S%d: Become candidate for term %d", rf.me, savedCurrentTerm)

	totalVotes := 1

	for idx, _ := range rf.peers {
		if idx == rf.me {
			continue
		}
		go func(peerId int) {
			args := RequestVoteArgs{}
			args.CandidateId = rf.me
			args.Term = savedCurrentTerm

			reply := RequestVoteReply{}
			// fmt.Printf("S%d: Requesting vote from S%d", rf.me, peerId)

			ok := rf.sendRequestVote(peerId, &args, &reply)

			if ok {
				rf.mu.Lock()
				defer rf.mu.Unlock()
				// fmt.Printf("S%d: Received reply from S%d", rf.me, peerId)

				if rf.state != 1 {
					// fmt.Printf("S%d left candidate state while waiting for vote", rf.me)
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
							rf.startLeader()
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

	// fmt.Printf("S%d: Started election timer", rf.me)

	for {
		time.Sleep(10 * time.Millisecond)
		rf.mu.Lock()
		if rf.state != 1 && rf.state != 0 {
			// fmt.Printf("S%d: Aborting election", rf.me)
			rf.mu.Unlock()
			break
		}

		//received a heartbeat that updated us
		if currentTerm != rf.currentTerm {
			// fmt.Printf("S%d: Election term changed from %d to %d", rf.me, currentTerm, rf.currentTerm)
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

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.

func (rf *Raft) ticker() {
	for !rf.killed() {

		rf.mu.Lock()
		rf.electionTimeout = time.Now()
		rf.mu.Unlock()
		rf.runElectionTimer()
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*networking.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.state = 0

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
