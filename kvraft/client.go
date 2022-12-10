package raftkv

import (
	"crypto/rand"
	"labrpc"
	"math/big"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	leader  int
	me      int64 // unique client identifier
	reqID   int   // for unique identification of each PutAppend request
}

// returns a random int64 identifier
func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

// creates and returns a clerk
func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	ck.me = nrand()
	ck.reqID = 0
	ck.leader = 0
	return ck
}

// fetches the current value for a key.
// returns "" if key is not found.
func (ck *Clerk) Get(key string) string {
	for {
		args := GetArgs{Key: key}
		reply := GetReply{}
		ok := ck.servers[ck.leader].Call("RaftKV.Get", &args, &reply)
		if ok {
			if reply.WrongLeader {
				// try another server
				ck.leader = (ck.leader + 1) % len(ck.servers)
				continue
			}
			return reply.Value
		}
	}
}

// puts (or appends to) the value for a key
func (ck *Clerk) PutAppend(key string, value string, op string) {
	for {
		args := PutAppendArgs{
			Key:       key,
			Value:     value,
			Op:        op,
			ClientID:  ck.me,
			RequestID: ck.reqID,
		}
		reply := PutAppendReply{}
		ok := ck.servers[ck.leader].Call("RaftKV.PutAppend", &args, &reply)
		if ok {
			if reply.WrongLeader {
				// try another server
				ck.leader = (ck.leader + 1) % len(ck.servers)
				continue

			} else {
				// incremenet request ID
				ck.reqID += 1
				return
			}
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
