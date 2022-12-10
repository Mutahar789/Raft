package raft

import (
	"bytes"
	"encoding/gob"
	"labrpc"
	"math/rand"
	"sync"
	"time"
)

// return the minimum of two integers
func minimum(a, b int) int {
	if a > b {
		return b
	} else {
		return a
	}
}

// returns the maximum of two integers
func maximum(a, b int) int {
	if a > b {
		return a
	} else {
		return b
	}
}

type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool
	Snapshot    []byte
}

type LogEntry struct {
	Index   int
	Term    int
	Command interface{}
}

// A Go object implementing a single Raft peer
type Raft struct {
	mu        sync.Mutex
	peers     []*labrpc.ClientEnd
	persister *Persister
	me        int // index into peers[]
	applyCh   chan ApplyMsg

	// persistent state on all servers
	currTerm int
	votedFor int
	log      []LogEntry

	// volatile state on all servers
	commitIndex int
	lastApplied int

	state         string
	numVotes      int
	lastHeartbeat time.Time

	// volatile state on leaders
	// re-initialized on every election
	nextIndex  []int
	matchIndex []int
}

// return currentTerm and whether this server
// believes it is the leader.
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

// persist state
func (rf *Raft) persist() {
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	rf.mu.Lock()
	// persistent state
	e.Encode(rf.currTerm)
	e.Encode(rf.log)
	e.Encode(rf.votedFor)
	rf.mu.Unlock()
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

// restore previously persisted state
func (rf *Raft) readPersist(data []byte) {
	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	rf.mu.Lock()
	d.Decode(&rf.currTerm)
	d.Decode(&rf.log)
	d.Decode(&rf.votedFor)
	rf.mu.Unlock()
}

// RequestVote RPC arguments structure
type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// RequestVote RPC reply structure
type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

// RequestVote RPC handler
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	// candidate's log is up to date if:
	// case 1: my log is empty
	// case 2: candidate's lastLogTerm > my lastLogTerm
	// case 3: candidate's lastLogTerm == my lastLogTerm && candidate's lastLogIndex >= my lastLogIndex
	logUpToDate := len(rf.log) == 0 || (args.LastLogTerm > rf.log[len(rf.log)-1].Term) ||
		((args.LastLogTerm == rf.log[len(rf.log)-1].Term) && (args.LastLogIndex >= len(rf.log)))

	if args.Term < rf.currTerm {
		reply.VoteGranted = false

	} else if args.Term > rf.currTerm {
		rf.state = "Follower"
		rf.currTerm = args.Term // update term
		rf.votedFor = -1        // have not voted in this term
		rf.numVotes = 0

		if logUpToDate {
			reply.VoteGranted = true
			rf.votedFor = args.CandidateId // vote
			rf.lastHeartbeat = time.Now()  // reset election timer
		}

	} else if rf.state == "Follower" {
		// rf.currTerm == args.Term
		if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && logUpToDate {
			reply.VoteGranted = true
			rf.votedFor = args.CandidateId
			rf.lastHeartbeat = time.Now()
		} else {
			reply.VoteGranted = false
		}

	} else {
		reply.VoteGranted = false
	}

	reply.Term = rf.currTerm
	rf.mu.Unlock()
	rf.persist()
}

func (rf *Raft) sendRequestVote(server int, args RequestVoteArgs) bool {
	rf.mu.Lock()
	if rf.state != "Candidate" || rf.state == "Leader" {
		// only candidates and leaders can request vote
		// no assumptions taken around release and re-aquire of lock
		rf.mu.Unlock()
		return false
	}
	rf.mu.Unlock()
	reply := &RequestVoteReply{}
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	if ok {
		rf.mu.Lock()
		if reply.Term > rf.currTerm {
			// found a server with higher term
			// revert to follower state
			rf.currTerm = reply.Term
			rf.state = "Follower"
			rf.votedFor = -1
			rf.numVotes = 0

		} else if reply.VoteGranted {
			rf.numVotes += 1
			if rf.numVotes > len(rf.peers)/2 && rf.state == "Candidate" {
				if rf.state != "Leader" {
					rf.state = "Leader"
					for i := 0; i < len(rf.peers); i++ {
						rf.nextIndex[i] = len(rf.log) + 1
						rf.matchIndex[i] = 0
					}
					go rf.heartbeating()
				}
			}
		}
		rf.mu.Unlock()
		rf.persist()
	}
	return ok
}

// AppendEntries RPC arguments structure
type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

// AppendEntries RPC reply structure
type AppendEntriesReply struct {
	Term      int
	Success   bool
	NextIndex int
}

// creates and returns args for AppendEntries RPC
func (rf *Raft) createAppendEntriesArgs(server int) AppendEntriesArgs {
	args := AppendEntriesArgs{
		Term:         rf.currTerm,
		LeaderId:     rf.me,
		PrevLogIndex: rf.nextIndex[server] - 1,
		LeaderCommit: rf.commitIndex,
		Entries:      rf.log[rf.nextIndex[server]-1 : len(rf.log)],
	}
	if args.PrevLogIndex != 0 {
		args.PrevLogTerm = rf.log[rf.nextIndex[server]-2].Term
	}
	return args
}

// AppendEntries RPC handler
func (rf *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	if args.Term < rf.currTerm {
		reply.Success = false
	} else {
		if rf.state != "Follower" || args.Term > rf.currTerm {
			rf.state = "Follower"
			rf.currTerm = args.Term // update term
			rf.votedFor = -1        // have not voted for anyone in this term
			rf.numVotes = 0
		}
		rf.lastHeartbeat = time.Now() // reset election timer

		if args.PrevLogIndex == 0 {
			reply.Success = true
			rf.log = args.Entries

		} else if len(rf.log) >= args.PrevLogIndex && rf.log[args.PrevLogIndex-1].Term == args.PrevLogTerm {
			// append entries consistency check passed
			// overwrite extraneous entries if any
			reply.Success = true
			rf.log = rf.log[:args.PrevLogIndex]
			rf.log = append(rf.log, args.Entries...)

		} else {
			// append entries consistency check failed
			reply.Success = false
		}

		// TRY REMOVING SECOND CONDITION
		if reply.Success && args.LeaderCommit > rf.commitIndex {
			rf.commitIndex = minimum(len(rf.log), args.LeaderCommit)
			for i := rf.lastApplied; i < rf.commitIndex; i++ {
				rf.applyCh <- ApplyMsg{
					Index:   rf.log[i].Index,
					Command: rf.log[i].Command,
				}
			}
			rf.lastApplied = rf.commitIndex
		}
	}
	reply.NextIndex = len(rf.log) + 1
	reply.Term = rf.currTerm
	rf.mu.Unlock()
	rf.persist()
}

func (rf *Raft) sendAppendEntries(server int, args AppendEntriesArgs) bool {
	rf.mu.Lock()
	if rf.state != "Leader" {
		// only leaders can send AppendEntriesRPC's
		// no assumptions taken around release and re-aquire of lock
		rf.mu.Unlock()
		return false
	}
	rf.mu.Unlock()

	reply := &AppendEntriesReply{}
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	if ok {
		rf.mu.Lock()
		if reply.Term > rf.currTerm {
			// found a server with higher term
			// revert to follower state
			rf.currTerm = reply.Term
			rf.state = "Follower"
			rf.votedFor = -1
			rf.numVotes = 0

		} else {
			if reply.Success {
				rf.nextIndex[server] = reply.NextIndex
				rf.matchIndex[server] = rf.nextIndex[server] - 1
				rf.updateCommitIndex()

			} else {
				// decrement nextIndex
				rf.nextIndex[server] = maximum(rf.matchIndex[server]+1, 1)
				// retry
				newArgs := rf.createAppendEntriesArgs(server)
				go rf.sendAppendEntries(server, newArgs)
			}
		}
		rf.mu.Unlock()
		rf.persist()
	}
	return ok
}

// updates commitIndex and applies applies committed updates to state machine
func (rf *Raft) updateCommitIndex() {
	// update commit index
	for i := len(rf.log); i > rf.commitIndex; i-- {
		count := 0
		for j := 0; j < len(rf.peers); j++ {
			if j == rf.me {
				count += 1
				continue
			}
			if rf.matchIndex[j] >= i {
				count += 1
			}
		}
		if count > len(rf.peers)/2 && rf.log[i-1].Term == rf.currTerm {
			rf.commitIndex = i
			break
		}
	}
	// apply
	for k := rf.lastApplied; k < rf.commitIndex; k++ {
		rf.applyCh <- ApplyMsg{
			Index:   rf.log[k].Index,
			Command: rf.log[k].Command,
		}
	}
	rf.lastApplied = rf.commitIndex
}

// goroutine that sends out periodic heartbeats
func (rf *Raft) heartbeating() {
	for {
		rf.mu.Lock()
		if rf.state != "Leader" {
			rf.mu.Unlock()
			return
		}
		for i := 0; i < len(rf.peers); i++ {
			if i != rf.me {
				go func(i int) {
					rf.mu.Lock()
					args := rf.createAppendEntriesArgs(i)
					go rf.sendAppendEntries(i, args)
					rf.mu.Unlock()
				}(i)
			}
		}
		rf.mu.Unlock()
		// send heartbeat every 100 milliseconds
		time.Sleep(time.Millisecond * 100)
	}
}

// return a random timer between 750 and 1500 milliseconds
func randomTimer() time.Duration {
	rand.Seed(time.Now().UnixNano())
	timer := time.Duration(rand.Intn(750)+750) * time.Millisecond
	return timer
}

// goroutine that detects failures and starts elections
func (rf *Raft) election() {
	electionTimeout := randomTimer()
	for {
		time.Sleep(time.Millisecond * 100) // check for timeout every 100 milliseconds
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
			args := RequestVoteArgs{
				Term:         rf.currTerm,
				CandidateId:  rf.me,
				LastLogIndex: len(rf.log),
			}
			if args.LastLogIndex != 0 {
				// if atleast one entry in log
				args.LastLogTerm = rf.log[len(rf.log)-1].Term
			}
			for i := 0; i < len(rf.peers); i++ {
				if i != rf.me {
					go rf.sendRequestVote(i, args)
				}
			}
			electionTimeout = randomTimer()
		}
		rf.mu.Unlock()
		rf.persist()
	}
}

// starts agreement on a new command issued by the client
func (rf *Raft) Start(command interface{}) (int, int, bool) {

	rf.mu.Lock()

	var index int = len(rf.log) + 1
	var term int = rf.currTerm
	var isleader bool
	if rf.state == "Leader" {
		isleader = true
		// create entry
		entry := LogEntry{
			Index:   index,
			Term:    term,
			Command: command,
		}
		// append entry to log
		rf.log = append(rf.log, entry)
	} else {
		isleader = false
	}
	rf.mu.Unlock()
	rf.persist()
	return index, term, isleader
}

func (rf *Raft) Kill() {
}

// creates and returns a raft peer
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.applyCh = applyCh

	// initialization
	rf.currTerm = 0
	rf.votedFor = -1
	rf.numVotes = 0
	rf.state = "Follower"
	rf.lastHeartbeat = time.Now()

	rf.log = make([]LogEntry, 0)

	rf.commitIndex = 0
	rf.lastApplied = 0

	for i := 0; i < len(peers); i++ {
		rf.nextIndex = append(rf.nextIndex, 1)
		rf.matchIndex = append(rf.matchIndex, 0)
	}
	// make goroutines
	go rf.election()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}
