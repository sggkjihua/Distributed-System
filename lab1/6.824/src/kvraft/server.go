package kvraft

import (
	"encoding/gob"
	"bytes"
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

	seqOfClient map[int64]int
	dispatcher map[int] chan Notification
	persister *raft.Persister
}

type Notification struct{
	Cid int64
	Seq int
}


func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.

	op := Op{Operation:"Get" , Key:args.Key, Seq: args.Seq, Cid:args.Cid}

	index, _, isLeader := kv.rf.Start(op)

	// start 递交上去的command不应该有重复的sequence
	if ! isLeader {
		reply.IsLeader = false
		reply.Err = "Not leader, try other server"
		return
	}

	kv.mu.Lock()
	if _, ok := kv.dispatcher[index]; !ok {
		kv.dispatcher[index] = make(chan Notification, 1)
	}
	ch := kv.dispatcher[index]
	kv.mu.Unlock()

	select{
		// leader should wait until it get the result
		// should we set a timer here?
	case  notification := <- ch :
		// as required, all previous operation should reveal on the get request
		kv.mu.Lock()
		delete(kv.dispatcher, index)
		kv.mu.Unlock()
		if notification.Seq == op.Seq && notification.Cid == op.Cid {
			reply.IsLeader = true
			reply.Value = kv.kvMap[args.Key]
		}else{
			reply.IsLeader = false
		}
		return
	case <- time.After(time.Duration(600)*time.Millisecond):
		reply.IsLeader = false
		return
	}
}


func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	op := Op{Operation:args.Op , Key:args.Key, Val:args.Value, Seq: args.Seq, Cid:args.Cid}
	index, _, isLeader := kv.rf.Start(op)
	// start 递交上去的command不应该有重复的sequence
	if !isLeader {
		reply.IsLeader = false
		reply.Err = "Not leader, try other server"
		return
	}

	kv.mu.Lock()
	if _, ok := kv.dispatcher[index]; !ok {
		kv.dispatcher[index] = make(chan Notification, 1)
	}
	ch := kv.dispatcher[index]
	kv.mu.Unlock()
	select{
		// leader should wait until it get the result
		// should we set a timer here?
	case  notification := <- ch:
		kv.mu.Lock()
		delete(kv.dispatcher, index)
		kv.mu.Unlock()

		if notification.Seq == op.Seq && notification.Cid == op.Cid {
			reply.IsLeader = true
		}else{
			reply.IsLeader = false
		}
		return
	case <- time.After(time.Duration(600)*time.Millisecond):
		reply.IsLeader = false
		return
	}
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
	command := commit.Command

	op, ok := command.(Op)
	if !ok {
		// transformation failed, normally will not have this issue
		return
	}

	// get the Seq and Cid from the commitment
	Seq := op.Seq
	Cid := op.Cid
	kv.mu.Lock()
	val, exists := kv.seqOfClient[Cid]

	if !exists || val<Seq {
		Operation := op.Operation
		Key := op.Key
		Value := op.Val
		if Operation == "Put" {
			kv.kvMap[Key] = Value
		}else if Operation == "Append" {
			kv.kvMap[Key] += Value
		}else if Operation == "Get" {
			fmt.Printf("Get command %v has been processed\n", command)
		}
		// update the maxSeq for this client ID after all has been done
		kv.seqOfClient[Cid] = Seq
	}
	kv.mu.Unlock()
	ch, ok := kv.dispatcher[commit.CommandIndex]
	if ok{
		notify := Notification{
			Cid:  op.Cid,
			Seq: op.Seq,
		}
		ch <- notify
	}
}

func (kv *KVServer) decodeSnapshot(commit raft.ApplyMsg) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	data := commit.Data

	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var kvMap map[string]string
	var seqOfClient map[int64]int
	fmt.Printf("[Before Installing Snapshot] %v before kvMap: %v\n",kv.me, kv.kvMap)

	if d.Decode(&seqOfClient) != nil ||
		d.Decode(&kvMap) != nil {
		fmt.Printf("[Error!]: occured when reading Snapshotfrom persistence!\n")
	}else{
		kv.kvMap = kvMap
		kv.seqOfClient = seqOfClient
	}
	fmt.Printf("[Snapshot Installed] %v after kvMap: %v\n",kv.me, kv.kvMap)

}


func (kv *KVServer) listenForCommitment() {
	for commit := range kv.applyCh {
		// for log compact logic
		if commit.CommandValid {
			fmt.Printf("[Commitment] %v receive a commitment %v\n", kv.me, commit)

			kv.handleCommitment(commit)
			kv.checkSnapShot(commit)
		}else {
			fmt.Printf("[Snapshot] %v receive a snapShot\n", kv.me)
			kv.decodeSnapshot(commit)
		}
	}
}


func (kv *KVServer) checkSnapShot(commit raft.ApplyMsg){
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if kv.maxraftstate == -1{
		return
	}
	//fmt.Printf("RaftStateSize %v, Max: %v \n", kv.persister.RaftStateSize(), kv.maxraftstate)

	if kv.persister.RaftStateSize() < kv.maxraftstate*9/10 {
		// when not exceed
		return
	}
	fmt.Printf("[Compacting Required] %v will need to compact, kvMap:%v \n", kv.me, kv.kvMap)
	// taking the index of the current commit as the lastIncludedIndex
	commitedIndex := commit.CommandIndex
	term := commit.CommandTerm
	data := kv.encodeSnapshot()
	go 	kv.rf.TakeSnapShot(commitedIndex, term, data)
}


func (kv *KVServer) encodeSnapshot() []byte {
    w := new(bytes.Buffer)
    e := gob.NewEncoder(w)
    if err := e.Encode(kv.seqOfClient); err != nil {
        panic(fmt.Errorf("encode seqOfClient fail: %v", err))
    }
    if err := e.Encode(kv.kvMap); err != nil {
        panic(fmt.Errorf("encode kvMap fail: %v", err))
    }
    return w.Bytes()
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
	kv.persister = persister
	kv.mu =  sync.Mutex{}
	// You may need initialization code here.

	kv.kvMap = make(map[string]string)
	kv.seqOfClient = make(map[int64]int)
	kv.dispatcher = make(map[int]chan Notification)
	go kv.listenForCommitment()

	return kv
}
