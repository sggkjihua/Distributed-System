package kvraft

import (
	//"time"
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
	Id int
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

	finished chan bool
}


func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	key := args.Key
	v, ok := kv.kvMap[key]
	if ok {
		reply.Value = v
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	_, isLeader := kv.rf.GetState()
	Id := args.Id
	val, ok := kv.processed[Id]
	if ok && val {
		// request as been processed, no need to do that again
		// so simply return
		return
	}
	// is currently processing, but not yet finished
	kv.processed[Id] = false
	if ! isLeader {
		reply.Err = "Not leader, try other server"
		return
	}

	// generate the command and publish to the leader

	command := Op{Operation:args.Op , Key:args.Key, Val:args.Value, Id: args.Id}

	kv.rf.Start(command)
	// once the leader has started processing this log
	kv.processed[Id] = true
	//timer := time.NewTimer(time.Duration(5*time.Second))
	for{
		select{
		/*case <- timer.C:
			reply.Err = "Time out, resend command"
			delete(kv.processed, Id)
			timer.Stop()
			return
			*/
		case  <- kv.finished:
			//timer.Stop()
			//fmt.Printf("%v receive a commitment %v Leader status: %v\n",kv.me, commitment, isLeader)
			//kv.handleCommitment(commitment.Command)
			return

		}
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


func (kv *KVServer) handleCommitment(command interface{}){
	op, ok := command.(Op)
	if !ok {
		return
	}
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
	}
	fmt.Printf("%v map[%v]=%v\n", kv.me, Key, kv.kvMap[Key])
	if _, isLeader := kv.rf.GetState(); isLeader{
		kv.finished <- true
	}
}


func (kv *KVServer) listenForCommitment() {
	for {
		select{
		case commit := <- kv.applyCh:
			fmt.Printf("%v receive a commitment %v\n", kv.me, commit)
			kv.handleCommitment(commit.Command)
		}
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

	// You may need initialization code here.

	kv.kvMap = make(map[string]string)
	kv.processed = make(map[int]bool)
	kv.finished = make(chan bool, 100)
	go kv.listenForCommitment()

	return kv
}
