package raft

import (
	"labrpc"
	"math/rand"
	"sync"
	"time"
)

type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool
	Snapshot    []byte
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex
	peers     []*labrpc.ClientEnd
	persister *Persister
	me        int // index into peers[]

	currTerm      int
	votedFor      int
	state         string
	numVotes      int
	lastHeartbeat time.Time
}

func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	var term int = rf.currTerm
	var isleader bool
	if rf.state == "Leader" {
		isleader = true
	} else {
		isleader = false
	}
	rf.mu.Unlock()
	return term, isleader
}

func (rf *Raft) persist() {
}

func (rf *Raft) readPersist(data []byte) {
}

type RequestVoteArgs struct {
	Term        int
	CandidateId int
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	if args.Term < rf.currTerm {
		reply.VoteGranted = false

	} else if args.Term > rf.currTerm {
		rf.state = "Follower"
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId // vote
		rf.currTerm = args.Term        // update term
		rf.lastHeartbeat = time.Now()  // reset election timer

	} else if rf.state == "Follower" {
		// rf.term == args.Term
		if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
			reply.VoteGranted = true
			rf.votedFor = args.CandidateId
			rf.lastHeartbeat = time.Now()
		} else {
			reply.VoteGranted = false
		}
	}
	reply.Term = rf.currTerm
	rf.mu.Unlock()
}

func (rf *Raft) sendRequestVote(server int, args RequestVoteArgs) bool {
	reply := &RequestVoteReply{}
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	if ok {
		rf.mu.Lock()
		if reply.Term > rf.currTerm {
			rf.currTerm = reply.Term
			rf.state = "Follower"
			rf.votedFor = -1
		} else if reply.VoteGranted {
			rf.numVotes += 1
			if rf.numVotes > len(rf.peers)/2 && rf.state == "Candidate" {
				rf.state = "Leader"
				go rf.heartbeating()
			}
		}
		rf.mu.Unlock()
	}
	return ok
}

type AppendEntriesArgs struct {
	Term     int
	LeaderId int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	if args.Term < rf.currTerm {
		reply.Success = false
	} else {
		rf.lastHeartbeat = time.Now() // reset election timer
		rf.state = "Follower"
		rf.votedFor = -1
		rf.currTerm = args.Term
		reply.Success = true
	}
	reply.Term = rf.currTerm
	rf.mu.Unlock()
}

func (rf *Raft) sendAppendEntries(server int, args AppendEntriesArgs) bool {
	reply := &AppendEntriesReply{}
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	if ok {
		rf.mu.Lock()
		if reply.Term > rf.currTerm {
			rf.currTerm = reply.Term
			rf.state = "Follower"
			rf.votedFor = -1
		}
		rf.mu.Unlock()
	}
	return ok
}

// goroutine that sends out periodic heartbeats
func (rf *Raft) heartbeating() {
	for {
		time.Sleep(time.Millisecond * 200)
		rf.mu.Lock()
		if rf.state != "Leader" {
			rf.mu.Unlock()
			return
		}
		args := AppendEntriesArgs{Term: rf.currTerm, LeaderId: rf.me}
		for i := 0; i < len(rf.peers); i++ {
			if i != rf.me {
				go rf.sendAppendEntries(i, args)
			}
		}
		rf.mu.Unlock()
	}
}

func randomTimer() time.Duration {
	rand.Seed(time.Now().UnixNano())
	timer := time.Duration(rand.Intn(300)+300) * time.Millisecond
	return timer
}

// goroutine that detects failures and starts elections
func (rf *Raft) election() {
	electionTimeout := randomTimer() // random election timeout
	for {
		time.Sleep(time.Millisecond * 50) // check for timeout every 50 milliseconds
		rf.mu.Lock()
		now := time.Now()
		electionTime := rf.lastHeartbeat.Add(electionTimeout)
		if now.After(electionTime) && (rf.state != "Leader") {
			// timeout
			rf.state = "Candidate"
			rf.lastHeartbeat = time.Now() // reset timer
			rf.currTerm += 1              // increment term
			rf.numVotes = 1               // vote for self
			rf.votedFor = rf.me

			// request votes
			args := RequestVoteArgs{Term: rf.currTerm, CandidateId: rf.me}
			for i := 0; i < len(rf.peers); i++ {
				if i != rf.me {
					go rf.sendRequestVote(i, args)
				}
			}
			electionTimeout = randomTimer()
		}
		rf.mu.Unlock()
	}
}

func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true
	return index, term, isLeader
}

func (rf *Raft) Kill() {
}

func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// initialization
	rf.currTerm = 0
	rf.votedFor = -1
	rf.state = "Follower"
	rf.lastHeartbeat = time.Now()

	// make goroutines
	go rf.election()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}
