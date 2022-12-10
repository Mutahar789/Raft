package raftkv

import (
	"encoding/gob"
	"labrpc"
	"log"
	"raft"
	"sync"
	"time"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type RaftKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	maxraftstate int               // snapshot if log grows this big
	kvstore      map[string]string // key value store
	clientState  map[int64]int     // stores id of last applied request of each client
}

type Op struct {
	Type      string
	Key       string
	Value     string
	Id        int         // id of server that the client sent the request to
	Inform    chan string // to inform the Get and PutAppend handlers that a command is applied
	ClientID  int64
	RequestID int
}

// returns the current value of a key.
// returns "" if key is not found.
func (kv *RaftKV) get(key string) string {
	value, ok := kv.kvstore[key]
	if ok {
		return value
	}
	return ""
}

// puts (or appends to) the value for a key
func (kv *RaftKV) putAppend(key string, value string, op string, clientID int64, requestID int) {
	lastApplied, ok := kv.clientState[clientID]
	if !(ok && lastApplied >= requestID) {
		if op == "Put" {
			kv.kvstore[key] = value

		} else if op == "Append" {
			v := kv.get(key)
			kv.kvstore[key] = v + value
		}
		// update last applied client request
		kv.clientState[clientID] = requestID
	}
}

// keeps reading applyCh to apply newly committed commands
func (kv *RaftKV) apply() {
	for {
		select {
		case applyMsg := <-kv.applyCh:
			command := applyMsg.Command.(Op)
			if command.Type == "Get" {
				value := kv.get(command.Key)
				if command.Id == kv.me {
					command.Inform <- value
				}
			} else {
				kv.putAppend(
					command.Key,
					command.Value,
					command.Type,
					command.ClientID,
					command.RequestID,
				)
				if command.Id == kv.me {
					command.Inform <- ""
				}

			}
		}

	}
}

// informs when there is a leader change
func (kv *RaftKV) checkIfLeaderChange(notLeaderAnymore chan int, reqResolved chan int) {
	for {
		select {
		case <-reqResolved:
			return
		default:
			time.Sleep(time.Millisecond * 50)
			_, isLeader := kv.rf.GetState()
			if !isLeader {
				notLeaderAnymore <- 1
				return
			}
		}
	}
}

// Get RPC handler
func (kv *RaftKV) Get(args *GetArgs, reply *GetReply) {
	op := Op{
		Type:   "Get",
		Key:    args.Key,
		Id:     kv.me,
		Inform: make(chan string),
	}
	_, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.WrongLeader = true
		return
	}
	// wait for commitment
	notLeaderAnymore := make(chan int)
	reqResolved := make(chan int)
	go kv.checkIfLeaderChange(notLeaderAnymore, reqResolved)
	for {
		select {
		case value := <-op.Inform:
			// command applied
			reply.WrongLeader = false
			// stop the leader change go routine
			reqResolved <- 1
			reply.Value = value
			return
		case <-notLeaderAnymore:
			reply.WrongLeader = true
			return
		}
	}
}

// PutAppend RPC handler
func (kv *RaftKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	op := Op{
		Type:      args.Op,
		Key:       args.Key,
		Value:     args.Value,
		Id:        kv.me,
		Inform:    make(chan string),
		ClientID:  args.ClientID,
		RequestID: args.RequestID,
	}
	_, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.WrongLeader = true
		return
	}
	// wait for commitment
	notLeaderAnymore := make(chan int)
	reqResolved := make(chan int)
	go kv.checkIfLeaderChange(notLeaderAnymore, reqResolved)
	for {
		select {
		case <-op.Inform:
			// command applied
			reply.WrongLeader = false
			// stop the leader change go routine
			reqResolved <- 1
			return

		case <-notLeaderAnymore:
			reply.WrongLeader = true
			return
		}
	}
}

func (kv *RaftKV) Kill() {
	kv.rf.Kill()
}

// create and return a kv server.
// initialize server state.
// starts goroutines for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *RaftKV {

	gob.Register(Op{})

	kv := new(RaftKV)
	kv.maxraftstate = maxraftstate

	kv.me = me
	kv.kvstore = make(map[string]string)
	kv.clientState = make(map[int64]int)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	go kv.apply()

	return kv
}
