package raftkv

const (
	OK       = "OK"
	ErrNoKey = "ErrNoKey"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Id int64
	ReqId int
}

type PutAppendReply struct {
	IsSuccess bool
	Err         Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	Id int64
	ReqId int
}

type GetReply struct {
	IsSuccess   bool
	Err         Err
	Value       string
}
