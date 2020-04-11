package shardkv


// import "../shardmaster"
import (
	"math/rand"
	"encoding/gob"
	"bytes"
	"fmt"
	"time"
	"../labrpc"
	"../raft"
	"sync"
	"../labgob"
	"../shardmaster"
)



type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Operation string
	Key string
	Val string
	Seq int
	Cid int64

	Num int
	// used to syn with follower?
	KvMap map[string]string
}

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	masters      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	config   shardmaster.Config
	sm       *shardmaster.Clerk
	kvMap map[string] string
	seqOfClient map[int64]int
	dispatcher map[int] chan Notification
	persister *raft.Persister

	maxNum       int
	completeTransfer map[int]bool

}

type Notification struct{
	Cid int64
	Seq int
	Valid bool
}



func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	isMyShard := kv.checkShard(args.Key)
	kv.mu.Unlock()
	if !isMyShard{
		fmt.Printf("[Get] shard does not match, reject\n")
		reply.Err = ErrWrongGroup
		return
	}
	op := Op{Operation:"Get" , Key:args.Key, Seq: args.Seq, Cid:args.Cid}

	index, _, isLeader := kv.rf.Start(op)

	// start 递交上去的command不应该有重复的sequence
	if ! isLeader {
		reply.IsLeader = false
		reply.Err = ErrWrongLeader
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
			reply.Err = OK
			reply.Value = kv.kvMap[args.Key]
		}else{
			if notification.Valid {
				reply.Err = ErrWrongLeader
			}else{
				// when the key should not be processed by this
				reply.Err = ErrWrongGroup
			}
		}
		return
	case <- time.After(time.Duration(600)*time.Millisecond):
		reply.Err = ErrWrongLeader
		return
	}
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	isMyShard := kv.checkShard(args.Key)
	kv.mu.Unlock()
	if !isMyShard{
		fmt.Printf("[PutA] shard does not match, reject\n")
		reply.Err = ErrWrongGroup
		return
	}
	op := Op{Operation:args.Op , Key:args.Key, Val:args.Value, Seq: args.Seq, Cid:args.Cid}
	index, _, isLeader := kv.rf.Start(op)
	// start 递交上去的command不应该有重复的sequence
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	fmt.Printf("[Start] Leader%v receive a putAppend request %v\n",kv.me, *args)

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
			reply.Err = OK
		}else{
			if notification.Valid {
				reply.Err = ErrWrongLeader
			}else{
				// when the key should not be processed by this
				reply.Err = ErrWrongGroup
			}
		}
		return
	case <- time.After(time.Duration(600)*time.Millisecond):
		reply.Err = ErrWrongLeader
		return
	}
}





func (kv *ShardKV) handleCommitment(commit raft.ApplyMsg){
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

	// uncomment this since we are not yet dealing with the duplicated request
	//val, exists := kv.seqOfClient[Cid]
	//if !exists || val<Seq {
	
	// one more filter for checking whether I should handle this request
	shouldProcess := len(op.Key)==0 || kv.config.Shards[key2shard(op.Key)] == kv.gid
	if shouldProcess{
		Operation := op.Operation
		Key := op.Key
		Value := op.Val
		KvMap := op.KvMap
		if Operation == "Put" {
			kv.kvMap[Key] = Value
		}else if Operation == "Append" {
			kv.kvMap[Key] += Value
		}else if Operation == "Get" {
			fmt.Printf("Get command %v has been processed\n", command)
		}else if Operation == "Reconfiguration"{
			fmt.Printf("Receive a commitment for reconfiguration\n")
			kv.reconfigurate(KvMap)
		}
		fmt.Printf("[KvMap] %v after Commitment: %v\n",kv.me, kv.kvMap)
		// update the maxSeq for this client ID after all has been done
		kv.seqOfClient[Cid] = Seq
	}
	kv.mu.Unlock()
	ch, ok := kv.dispatcher[commit.CommandIndex]
	if ok{
		notify := Notification{
			Cid:  op.Cid,
			Seq: op.Seq,
			Valid: shouldProcess,
		}
		ch <- notify
	}
}

func (kv *ShardKV) reconfigurate(kvMap map[string]string){
	fmt.Printf("[Reconfig] %v gid: %v, before %v\n", kv.me, kv.gid, kv.kvMap)
	nKvMap := make(map[string]string)
	for k, v:= range kvMap {
		val, ok := kv.kvMap[k]
		if ok {
			nKvMap[k] = val
		}else{
			nKvMap[k] = v
		}
	}
	kv.kvMap = nKvMap
	fmt.Printf("[Reconfig] %v gid: %v, after  %v\n", kv.me, kv.gid, kv.kvMap)
}


func (kv *ShardKV) checkShard(key string) bool{
	shard := key2shard(key)
	return  kv.config.Shards[shard] == kv.gid
}


func (kv *ShardKV) decodeSnapshot(commit raft.ApplyMsg) {
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


func (kv *ShardKV) listenForCommitment() {
	for commit := range kv.applyCh {
		// for log compact logic
		if commit.CommandValid {
			fmt.Printf("[ShardKV] %v receive a commitment %v\n", kv.me, commit)
			kv.handleCommitment(commit)
			kv.checkSnapShot(commit)
		}else {
			fmt.Printf("[ShardKV] %v receive a snapShot\n", kv.me)
			kv.decodeSnapshot(commit)
		}
	}
}


func (kv *ShardKV) checkSnapShot(commit raft.ApplyMsg){
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if kv.maxraftstate == -1{
		return
	}
	//fmt.Printf("RaftStateSize %v, Max: %v \n", kv.persister.RaftStateSize(), kv.maxraftstate)

	if kv.persister.RaftStateSize() < kv.maxraftstate*8/10 {
		// when not exceed
		return
	}
	//fmt.Printf("[Compacting Required] %v will need to compact, kvMap:%v \n", kv.me, kv.kvMap)
	// taking the index of the current commit as the lastIncludedIndex
	commitedIndex := commit.CommandIndex
	term := commit.CommandTerm
	data := kv.encodeSnapshot()
	go 	kv.rf.TakeSnapShot(commitedIndex, term, data)
}


func (kv *ShardKV) encodeSnapshot() []byte {
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
// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *ShardKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
}



func (kv *ShardKV) pollLatestConfig() {
	for{
		kv.mu.Lock()
		lastestConfig := kv.sm.Query(-1)
		if lastestConfig.Num > kv.config.Num {
			kv.maxNum = Max(kv.maxNum, lastestConfig.Num)
			kv.config = lastestConfig
			kv.transferShard(kv.config.Num)
		}
		kv.mu.Unlock()
		time.Sleep(100 * time.Millisecond)
	}
}

func (kv *ShardKV) transferShard(num int){
	// called in pollLatestConfig, lock here will lead to dead lock
	//kv.mu.Lock()
	//defer kv.mu.Unlock()

	if num < kv.config.Num {
		fmt.Printf("[Transfer] found an old config, discard\n")
		return
	}

	keys := []string{}
	gids := make(map[int]map[string]string)
	nKvMap := make(map[string]string)
	for k,v := range kv.kvMap{
		keys = append(keys, k)
		gid := kv.config.Shards[key2shard(k)]
		if gid==kv.gid{
			nKvMap[k] = v
			continue
		}
		gidMap, ok := gids[gid]
		if !ok{
			gids[gid] = make(map[string]string)
			gidMap =gids[gid]
		}
		gidMap[k] = v
	}
	// reset the map and delete those don't belong to me

	complete := true
	for gid, kvMap := range gids {
		// can deal with dead lock?
		succeed := kv.sendShardToGid(gid, kvMap)
		if !succeed{
			complete = false
			break
		}
	}
	if complete {
		kv.kvMap = nKvMap
		kv.syncWithFollower(nKvMap)
	}else{
		kv.config.Num --
		// so as to trigger deadlock
		// may be time out for some cases such as deadlock?
	}
}


func RandTime() int{
	return 500 + rand.Intn(400)
}


func (kv *ShardKV) sendShardToGid(gid int, kvMap map[string]string)bool{
	for {
		if servers, ok := kv.config.Groups[gid]; ok {
			// try each server for the shard.
			args := TransferArgs{Num:kv.config.Num, KvMap :kvMap}

			// waitTime is used to avoid keep waiting and lead to deadlock
			waitTime := RandTime()
			for si := 0; si < len(servers); si++ {
				srv := kv.make_end(servers[si])
				var reply TransferReply
				finished := make(chan bool,1)
				go func(){
					finished <- srv.Call("ShardKV.UpdateKvMap", &args, &reply)
				}()
				select{
				case ok := <-finished:
					if ok && (reply.Err == OK) {
						return true
					}
					if ok && (reply.Err == ErrWrongGroup) {
						// once we found we make a mistake, we need to get the lastest config again
						return false
					}		
					break							
				case <- time.After(time.Duration(waitTime) * time.Millisecond):
					// possibly that both are sending and not able to proceed, so dead lock
					reply.Err = ErrWrongGroup
					return false
				}
				// ... not ok, or ErrWrongLeader
			}
		}
		//time.Sleep(100 * time.Millisecond)
	}
}

func Max(a int, b int) int{
	if a>b{
		return a
	}
	return b
}

func (kv *ShardKV) UpdateKvMap(args *TransferArgs, reply *TransferReply){
	kv.mu.Lock()
	defer kv.mu.Unlock()
	// holding a lock may cause dead lock
	// if all are holding a lock and sending message, possiblely the other side does not hold the lock
	num := args.Num
	kvMap := args.KvMap
	if num < kv.config.Num || num < kv.maxNum{
		reply.Err = ErrWrongGroup
		return
	}
	kv.maxNum = Max(kv.maxNum, num)
	// make sure not older than itself
	_, isLeader := kv.rf.GetState()
	if !isLeader{
		reply.Err = ErrWrongLeader
		return
	}
	for k,v := range kvMap{
		// is it possible that this KV is actually not valid?
		kv.kvMap[k] = v
	}
	reply.Err = OK
	// how to trigger the update for followers
}


func (kv *ShardKV) syncWithFollower(kvMap map[string]string){
	// need to figure out how to synchronize with followers
	op := Op{Operation:"Reconfiguration", KvMap:kvMap}
	kv.rf.Start(op)
}


//
// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardmaster.
//
// pass masters[] to shardmaster.MakeClerk() so you can send
// RPCs to the shardmaster.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use masters[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, masters []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.masters = masters

	// Your initialization code here.

	// Use something like this to talk to the shardmaster:
	// kv.mck = shardmaster.MakeClerk(kv.masters)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	kv.sm = shardmaster.MakeClerk(masters)

	kv.config = kv.sm.Query(-1)
	kv.kvMap =  make(map[string]string)
	kv.seqOfClient = make(map[int64]int)
	kv.dispatcher = make(map[int]chan Notification)
	kv.persister = persister
	kv.maxNum = 0
	kv.completeTransfer = make(map[int]bool)
	go kv.listenForCommitment()
	go kv.pollLatestConfig()
	return kv
}
