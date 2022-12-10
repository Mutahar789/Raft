package raftkv

// Put or Append
type PutAppendArgs struct {
	Key       string
	Value     string
	Op        string // "Put" or "Append"
	ClientID  int64
	RequestID int
}

type PutAppendReply struct {
	WrongLeader bool
}

type GetArgs struct {
	Key string
}

type GetReply struct {
	WrongLeader bool
	Value       string
}
