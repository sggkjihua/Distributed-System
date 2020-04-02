package kvraft

import (
	"time"
	"fmt"
	"../labgob"
	"../labrpc"
	"log"
	"../raft"
	"sync"
	"sync/atomic"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}


type Op struct{
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Operation string
	Key string
	Val string
	Seq int
	Cid int64
}


type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.

	kvMap map[string] string

	processed map[int] bool
	processedFromClient map[int] bool


	seqOfClient map[int64]int
	logEntryIndex map[int] Op
	finished chan bool

	seq int
}


func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.

	command := Op{Operation:"Get" , Key:args.Key, Seq: args.Seq, Cid:args.Cid}
	/*
	_, isLeader := kv.rf.GetState()
	if isLeader{
		reply.IsLeader = true
		reply.Value = kv.kvMap[args.Key]
	}else{
		reply.IsLeader = false
	}
	return
	*/
	
	
	_, _, isLeader := kv.rf.Start(command)
	// start 递交上去的command不应该有重复的sequence
	if ! isLeader {
		reply.IsLeader = false
		reply.Err = "Not leader, try other server"
		return
	}
	select{
		// leader should wait until it get the result
		// should we set a timer here?
	case  <- kv.finished:
		reply.IsLeader = true
		reply.Value = kv.kvMap[args.Key]
		return
	case <- time.After(time.Duration(600)*time.Millisecond):
		reply.IsLeader = false
		return
	}
}


func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// 这个方法也有缺陷，如果是中间有个leader直接成功了，那么其他的server并不会有这个op在map里面
	// 因此在真正implement的时候，会忽略这个问题

	// Your code here.
	op := Op{Operation:args.Op , Key:args.Key, Val:args.Value, Seq: args.Seq, Cid:args.Cid}
	index, _, isLeader := kv.rf.Start(op)
	// start 递交上去的command不应该有重复的sequence
	if ! isLeader {
		reply.IsLeader = false
		reply.Err = "Not leader, try other server"
		return
	}
	kv.logEntryIndex[index] = op
	// index, term, isLeader := kv.rf.Start(command)
	//kv.rf.Start(command)

	select{
		// leader should wait until it get the result
		// should we set a timer here?
	case  <- kv.finished:
		reply.IsLeader = true
		return
	case <- time.After(time.Duration(600)*time.Millisecond):
		reply.IsLeader = false
		return
	}

	//fmt.Printf("Index: %v Term: %v IsLeader: %v\n", index, term, isLeader)
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}


func (kv *KVServer) handleCommitment(commit raft.ApplyMsg){
	kv.mu.Lock()
	defer kv.mu.Unlock()
	command := commit.Command
	index := commit.CommandIndex

	op, ok := command.(Op)
	if !ok {
		// transformation failed, normally will not have this issue
		return
	}

	// get the Seq and Cid from the commitment
	Seq := op.Seq
	Cid := op.Cid
	val, exists := kv.seqOfClient[Cid]

	// if commitment has not been applied before
	// or the command for this client and seq is not the same
	//fmt.Printf("%v ------ %v\n", op, kv.commandMap[Cid][Seq])
	less := !exists || val<Seq
	match := false
	_, contains := kv.logEntryIndex[index]
	if contains {
		match = kv.logEntryIndex[index] == op
	}
	if less || !match {
		Operation := op.Operation
		Key := op.Key
		Value := op.Val
		if Operation == "Put" {
			kv.kvMap[Key] = Value
		}else if Operation == "Append" {
			var original string
			if val, ok := kv.kvMap[Key]; ok {
				original = val
			}
			kv.kvMap[Key] = original+Value
		}else if Operation == "Get" {
			fmt.Printf("Get command %v has been processed\n", command)
		}
		// update the maxSeq for this client ID after all has been done
		kv.seqOfClient[Cid] = Seq
	}else{
		fmt.Printf("Ignore operation %v\n", command)
	}

	if _, isLeader := kv.rf.GetState(); isLeader{
		kv.finished <- true
	}
}


func (kv *KVServer) listenForCommitment() {
	for commit := range kv.applyCh {
		fmt.Printf("%v receive a commitment %v\n", kv.me, commit)
		kv.handleCommitment(commit)
	}
}


//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	kv.mu =  sync.Mutex{}
	// You may need initialization code here.

	kv.kvMap = make(map[string]string)
	kv.seqOfClient = make(map[int64]int)
	kv.processed = make(map[int]bool)
	kv.processedFromClient = make(map[int]bool)
	kv.finished = make(chan bool, 1)
	kv.logEntryIndex = make(map[int]Op)
	go kv.listenForCommitment()

	return kv
}
