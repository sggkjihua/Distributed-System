package shardkv

//
// Sharded key/value server.
// Lots of replica groups, each running op-at-a-time paxos.
// Shardmaster decides which group serves each shard.
// Shardmaster may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongGroup  = "ErrWrongGroup"
	ErrWrongLeader = "ErrWrongLeader"
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
	Cid int64
	Seq int
}

type PutAppendReply struct {
	Err Err
	IsLeader bool
	Processed bool
	Seq int

}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	Cid int64
	Seq int
}

type GetReply struct {
	Err   Err
	Value string
	IsLeader bool
}

type TransferArgs struct{
	Num int
	Shard Shard
}

type TransferReply struct{
	Err Err
}


type FetchArgs struct{
	Num int
	ShardsNeeded map[int]bool
	From int
}

type FetchReply struct{
	Err Err
	Shards []Shard
	Num int
}


type DeleteArgs struct{
	Num int
	ShardsConfirmed map[int]bool
	From int
}

type DeleteReply struct{
	Err Err
	Num int
}
