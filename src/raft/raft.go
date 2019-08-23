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

import "sync"
import (
	"labrpc"
	"time"
	"math/rand"
	"fmt"
)

// import "bytes"
// import "labgob"


// Possible values (states) for StateType
const (
	StateFollower StateType = iota
	StateCandidate
	StateLeader
)

type StateType uint64

var stmap = [...]string{
	"StateFollower",
	"StateCandidate",
	"StateLeader",
}

func (st StateType) String() string {
	return stmap[uint64(st)]
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

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.


	lastHeard time.Time

	// How often to send heartbeat
	heartbeatTick 	 time.Duration
	randomizedElectionTimeout time.Duration

	electionTimer *time.Timer
	heartbeatTimer *time.Timer

	// Persistent State
	Term int
	Vote int

	votes chan int
	votesGranted int
	sentVotesThisTerm bool


	log     []interface{}
	lead 	int

	// Volatile state on all servers
	commitIndex int
	lastApplied int

	// Volatile state on leaders
	nextIndex  map[int]int
	matchIndex map[int]int

	// Leader state
	state       StateType
	stateAction	func(*Raft)
}

func (rf *Raft) resetRandomizedElectionTimeout() {
	randsource := rand.New(rand.NewSource(time.Now().UnixNano()))
	rf.randomizedElectionTimeout = 300 + time.Duration(randsource.Intn(1000)) * time.Millisecond
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	var term = rf.Term
	var isleader = rf.state == StateLeader

	// Your code here (2A).
	return term, isleader
}

func (rf *Raft) transitionToFollower(term int, lead int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	DPrintf(fmt.Sprintln(rf.me, "Transitioning from:", rf.state, "to follower"))
	rf.state = StateFollower
	rf.Term = term

	rf.resetRandomizedElectionTimeout()
	rf.electionTimer.Stop()
	rf.electionTimer.Reset(rf.randomizedElectionTimeout)

	rf.stateAction = FollowerState

	rf.lead = lead
	rf.Vote = -1
}

func (rf *Raft) transitionToCandidate() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state == StateLeader {
		panic("Invalid transition [leader -> candidate]")
	}

	rf.Term++

	rf.Vote = rf.me
	rf.lead = -1

	rf.resetRandomizedElectionTimeout()
	rf.electionTimer.Stop()
	rf.electionTimer.Reset(rf.randomizedElectionTimeout)
	rf.votes = make(chan int)
	rf.votesGranted = 1
	rf.sentVotesThisTerm = false

	rf.state = StateCandidate
	rf.stateAction = CandidateState

	DPrintf(fmt.Sprintln(rf.me, "Transitioning from:", rf.state, "to candidate", "on term", rf.Term))
}

func (rf *Raft) transitionToLeader() {
	rf.mu.Lock()
	if rf.state == StateFollower {
		panic("Invalid transition [follower -> leader]")
	}
	DPrintf(fmt.Sprintln(rf.me, "Transitioning from:", rf.state, "to leader"))

	rf.state = StateLeader
	rf.stateAction = LeaderState
	rf.lead = rf.me
	rf.Vote = rf.me

	rf.heartbeatTimer = time.NewTimer(100 * time.Millisecond)
	rf.mu.Unlock()
	for peer, _ := range rf.peers {
		if peer == rf.me {
			continue
		}
		rf.sendHeartBeat(peer)
	}
}


func FollowerState(rf *Raft) {
	select {
	case <-rf.electionTimer.C:
		rf.transitionToCandidate()
		break
	}
}

func CandidateState(rf *Raft) {
	select {
	case <-rf.votes:
		rf.mu.Lock()
		rf.votesGranted++

		if rf.votesGranted > len(rf.peers)/2 {
			rf.mu.Unlock()
			rf.transitionToLeader()
			break
		} else {
			rf.mu.Unlock()
		}

	default:
		if !rf.sentVotesThisTerm {
			fmt.Println("SENDING VOTES")
			for i, _ := range rf.peers {
				if i == rf.me {
					continue
				}
				go func(peer int) {
					ok := rf.sendRequestVote(peer)
					if ok {
						rf.votes <- 1
					}
				}(i)
			}
			rf.mu.Lock()
			rf.sentVotesThisTerm = true
			rf.mu.Unlock()
		}
	}
}

func LeaderState(rf *Raft) {
	select {
		case <-rf.heartbeatTimer.C:
			for peer, _ := range rf.peers {
				if peer == rf.me {
					continue
				}
				rf.sendHeartBeat(peer)
			}
	}
}

func (rf *Raft) sendHeartBeat(peer int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	args := &AppendEntriesArgs{
		Term: rf.Term,
		LeaderId: rf.me,
		PrevLogIndex: 0,
		PrevLogTerm: 0,
		Entries: make([]interface{}, 0),
		LeaderCommit: 0,
	}
	reply := &AppendEntriesReply{}
	ok := rf.peers[peer].Call("Raft.AppendEntries", args, reply)
	fmt.Println(ok)
}

func (rf *Raft) sendRequestVote(peer int) (ok bool) {
	rf.mu.Lock()
	args := &RequestVoteArgs{
		rf.Term,
		rf.me,
		0,
		0,
	}
	rf.mu.Unlock()
	reply := &RequestVoteReply{}

	rf.peers[peer].Call("Raft.RequestVote", args, reply)

	rf.checkRequest(reply.Term, peer)

	if reply.VoteGranted {
		fmt.Println("Got vote")
		rf.votes <- 1
		ok = true
	} else {
		fmt.Println("Didn't get vote")
		ok = false
	}
	return false
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
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

//
// example AppendEntries RPC reply structure.
// field names must start with capital letters!
//
type AppendEntriesArgs struct {
	// Your data here (2A).
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries		 []interface{}
	LeaderCommit int
}

type AppendEntriesReply struct {
	// Your data here (2A).
	Term    int
	Success	bool
}

func (rf *Raft) checkRequest(term int, from int) bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if term > rf.Term {
		return true
	} else {
		return false
	}
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).

	if rf.checkRequest(args.Term, args.CandidateId) {
		rf.transitionToFollower(args.Term, args.CandidateId)
	}

	rf.mu.Lock()
	rf.electionTimer.Reset(rf.randomizedElectionTimeout)
	canVote := rf.Vote == args.CandidateId ||
		(rf.Vote == -1)
	reply.Term = args.Term

	if canVote {
		reply.VoteGranted = true
		rf.Vote = args.CandidateId
	} else {
		reply.VoteGranted = false
	}
	rf.mu.Unlock()
}

//
// example AppendEntries RPC handler.
//
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	if args.Term < rf.Term {
		reply.Term = rf.Term
		reply.Success = false
		rf.mu.Unlock()
	} else if args.Term >= rf.Term {
		rf.mu.Unlock()
		if rf.checkRequest(args.Term, args.LeaderId) {
			rf.transitionToFollower(args.Term, args.LeaderId)
		}
		fmt.Println("Resetting my timer")
		rf.electionTimer.Reset(rf.randomizedElectionTimeout)
		reply.Term = args.Term
		reply.Success = true
	}
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
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := false

	// Your code here (2B).

	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
	fmt.Println(rf.me, " was killed")
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
	rf.state = StateFollower
	rf.stateAction = FollowerState

	rf.resetRandomizedElectionTimeout()
	rf.electionTimer = time.NewTimer(rf.randomizedElectionTimeout)

	go func() {
		for {
			rf.stateAction(rf)
		}
	}()

	return rf
}
