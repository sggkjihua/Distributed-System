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
	Shard Shard
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

	myShard map[int]bool          // shard that I am responsible for
	shardMap map[int] Shard       // map for the id -> shard 
	seqOfClient map[int64]int     // should this still exist?????
	dispatcher map[int] chan Notification 
	persister *raft.Persister

	maxNumOfShard [10]int
	maxNum       int
}

type Notification struct{
	Cid int64
	Seq int
	Valid bool
}


type Shard struct{
	Id int
	Num int
	KvMap map[string]string
	SeqOfCid map[int64]int
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
	//Shard := kv.generateShard(args.Key, "", args.Cid, args.Seq)
	op := Op{Operation:"Get", Key:args.Key, Cid:args.Cid, Seq:args.Seq}
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
			reply.Value = kv.getValueByKey(args.Key)
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

// 要保证整个迁移的期间是不能够去接受新的 PutAppend 和 Get 的请求的
// 但是应该如何保证呢？ 比如我这个leader发现了新的 config
// 之后尝试向其它负责这个shard的 leader 发送，如果刚好双方在互相发送那怎么办？
// 几率其实也比较低吧，不妨先尝试一下，什么情况下会使双方互相 hold 的 shard 向对方发送？

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

	Shard := kv.generateShard(args.Key, args.Value, args.Cid, args.Seq)
	op := Op{Operation:args.Op ,Key:args.Key, Cid:args.Cid, Seq:args.Seq, Shard:Shard}
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


func (kv *ShardKV) generateShard(key string, val string, cid int64, seq int)Shard{
	kvMap := make(map[string]string)
	SeqOfClient := make(map[int64]int)
	SeqOfClient[cid] = seq
	kvMap[key] = val
	Shard := Shard{
		Id: key2shard(key),
		Num: kv.config.Num,
		KvMap: kvMap,
		SeqOfCid: SeqOfClient,
	}
	return Shard
}

func (kv *ShardKV) getValueByKey(key string) string{
	ShardId := key2shard(key)
	Shard, ok := kv.shardMap[ShardId]
	if ok {
		val, exists := Shard.KvMap[key]
		if exists {
			return val
		}
	}
	return ""
}

func (kv *ShardKV) putShard(key string, val string, shard Shard, append bool){
	Shard, ok := kv.shardMap[shard.Id]
	if !ok{
		// not exists, simply put it
		kv.shardMap[shard.Id] = shard
		return
	}
	if append {
		Shard.KvMap[key] += val
		// update seq of cid
	}else {
		Shard.KvMap[key] = val
	}
	Shard.SeqOfCid = shard.SeqOfCid
}

func (kv *ShardKV) shardNotExistsOrNotOldRequest(key string, cid int64, seq int, shard Shard) bool{
	if len(key)==0{
		// accept or delete
		// should be in the same Num, or can we simply pass it?
		return shard.Num >= kv.config.Num
	}else{
		Shard, ok := kv.shardMap[shard.Id]
		if !ok {
			// not exists
			return true
		}
		seqMap := Shard.SeqOfCid
		val, ok := seqMap[cid]
		return !ok || val<seq 
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
	Operation := op.Operation
	Shard := op.Shard
	kv.mu.Lock()

	// uncomment this since we are not yet dealing with the duplicated request
	//val, exists := kv.seqOfClient[Cid]
	//if !exists || val<Seq {
	
	// one more filter for checking whether I should handle this request
	//shouldProcess := len(op.Key)==0 || kv.config.Shards[key2shard(op.Key)] == kv.gid || Operation=="Remove" || Operation == "Accept"
	shouldProcess := kv.shardNotExistsOrNotOldRequest(op.Key, Cid, Seq, Shard)
	if shouldProcess{
		Key := op.Key
		Value := op.Val
		if Operation == "Put" {
			kv.putShard(Key, Value, Shard, false)
		}else if Operation == "Append" {
			kv.putShard(Key, Value, Shard, true)
		}else if Operation == "Get" {
			fmt.Printf("Get command %v has been processed\n", command)
		}else if Operation == "Remove"{
			delete(kv.myShard, Shard.Id)
			delete(kv.shardMap, Shard.Id)
			fmt.Printf("Remove shards %v successfully\n", Shard)
		}else if Operation == "Accept" {
			kv.shardMap[Shard.Id] = Shard
			kv.myShard[Shard.Id] = true
			fmt.Printf("Accept new shards %v successfully\n", Shard)
		}
		fmt.Printf("[ShardMap] %v [GID: %v] after Commitment: %v\n",kv.me, kv.gid, kv.shardMap)
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

/*
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
*/


func (kv *ShardKV) checkShard(key string) bool{
	shard := key2shard(key)
	_, ok := kv.myShard[shard]
	return  ok
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
	var shardMap map[int]Shard
	var seqOfClient map[int64]int
	fmt.Printf("[Before Installing Snapshot] %v before kvMap: %v\n",kv.me, kv.shardMap)

	if d.Decode(&seqOfClient) != nil ||
		d.Decode(&shardMap) != nil {
		fmt.Printf("[Error!]: occured when reading Snapshotfrom persistence!\n")
	}else{
		kv.shardMap = shardMap
		kv.seqOfClient = seqOfClient
	}
	fmt.Printf("[Snapshot Installed] %v after kvMap: %v\n",kv.me, kv.shardMap)

}


func (kv *ShardKV) listenForCommitment() {
	for commit := range kv.applyCh {
		// for log compact logic
		if commit.CommandValid {
			fmt.Printf("[ShardKV] %v [GID: %v] receive a commitment %v\n", kv.me, kv.gid, commit)
			kv.handleCommitment(commit)
			kv.checkSnapShot(commit)
		}else {
			fmt.Printf("[ShardKV] %v [GID: %v] receive a snapShot\n", kv.me, kv.gid)
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
	op, ok := commit.Command.(Op)
	if kv.persister.RaftStateSize() < kv.maxraftstate*8/10 && (ok && op.Operation!="Reconfiguration") {
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
    if err := e.Encode(kv.shardMap); err != nil {
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
		//succeed := true
		if lastestConfig.Num > kv.config.Num {
			kv.maxNum = Max(kv.maxNum, lastestConfig.Num)
			kv.config = lastestConfig
			kv.transferShard()
			//kv.fetchShards(lastestConfig)
		}
		kv.mu.Unlock()
		/*
		if !succeed{
			time.Sleep(time.Duration(RandTime())*time.Millisecond)
		}
		*/
		time.Sleep(100 * time.Millisecond)
	}
}


/*
func (kv *ShardKV) fetchShards(config shardmaster.Config) bool{
	Num := config.Num
	Shards := config.Shards
	Groups := config.Groups
	myShards := make(map[int]bool)
	for shard, gid:= range Shards{
		if gid == kv.gid {
			// first collect all shards that I need
			myShards[shard] = true
		}
	}
	failed := false
	// 但是不能保证别人需要的， 所以只能到完全干净了之后才commit
	for gid, servers := range Groups{
		if gid == kv.gid {
			continue
		}
		succeed := kv.pollFromGid(myShards, Shards, Num, servers, Groups)	
		if !succeed{
			fmt.Printf("[Poll] %v polling from gid: %v fail\n", kv.gid, gid)
			failed = true
			break
		}
	}
	if !failed{
		kv.config = config
		kv.syncWithFollower(kv.kvMap)
	}
	return !failed
}

func (kv *ShardKV) pollFromGid(myShards map[int]bool, Shards [10]int, num int, servers []string, Groups map[int][]string) bool{
	for {
		// try each server for the shard.
		args := FetchArgs{Num:num, Groups:Groups, ShardsNeeded:myShards, Shards:Shards}
		// waitTime is used to avoid keep waiting and lead to deadlock
		waitTime := RandTime()
		for si := 0; si < len(servers); si++ {
			srv := kv.make_end(servers[si])
			var reply FetchReply
			finished := make(chan bool,1)
			go func(){
				finished <- srv.Call("ShardKV.GetShard", &args, &reply)
			}()
			select{
			case ok := <-finished:
				if ok && (reply.Err == OK) {
					kvMap := reply.KvMap
					fmt.Printf("[FetchSucceed] %v [GID: %v] from gid: %v kvMap: %v \n", kv.me, kv.gid,servers[si],kvMap)
					for k,v := range kvMap{
						kv.kvMap[k] = v
					}
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
				fmt.Printf("Timeout %v for %v\n", kv.gid, servers[si])
				return false
			}
				// ... not ok, or ErrWrongLeader
		}
		//time.Sleep(100 * time.Millisecond)
	}
}

func (kv *ShardKV) GetShard(args *FetchArgs, reply *FetchReply){
	//fmt.Printf("%v [GID: %v] receive fetchArgs %v\n", kv.me, kv.gid, args)
	kv.mu.Lock()
	defer kv.mu.Unlock()
	Num := args.Num
	Shards := args.Shards
	ShardsNeeded := args.ShardsNeeded
	mapNeeded := make(map[string]string)
	myShards := make(map[int]bool)
	if Num < kv.config.Num || Num < kv.maxNum {
		// not a good
		reply.Err = ErrWrongGroup
		return
	}
	kv.maxNum = Max(kv.maxNum, Num)
	// make sure not older than itself
	_, isLeader := kv.rf.GetState()
	if !isLeader{
		reply.Err = ErrWrongLeader
		return
	}
	for shard, gid := range Shards{
		if gid == kv.gid {
			myShards[shard] = true
		}
	}

	canSotre := true
	for k,v := range kv.kvMap{
		shard := key2shard(k)
		_, ok := ShardsNeeded[shard]
		if ok {
			mapNeeded[k] = v
			continue
		}
		_, mine := myShards[shard]
		if !mine{
			canSotre = false
		}
	}
	for k,_ := range mapNeeded{
		delete(kv.kvMap, k)
	}
	reply.KvMap = mapNeeded
	fmt.Printf("%v [GID: %v] reply with %v \n", kv.me, kv.gid,reply)

	if canSotre{
		//kv.syncWithFollower(kv.kvMap)
	}
}
*/


func RandTime() int{
	return 500 + rand.Intn(400)
}

func (kv *ShardKV) transferShard(){
	// called in pollLatestConfig, lock here will lead to dead lock
	_, isLeader := kv.rf.GetState()
	if !isLeader{
		// only the leader need to handle the shard transfer process
		return
	}
	shardsSend := make([]Shard, 0)
	for k,s := range kv.shardMap{
		gid := kv.config.Shards[k]
		if gid == kv.gid{
			// if I am responsible for it
			continue
		}
		shardsSend = append(shardsSend, s)
	}
	// reset the map and delete those don't belong to me
	for _, shard := range shardsSend {
		succeed := kv.sendShardToGid(shard)
		if succeed {
			kv.maxNumOfShard[shard.Id] = shard.Num
			deleteOp := Op{ Operation:"Delete", Shard: shard}
			kv.rf.Start(deleteOp)
		}
	}

}

func (kv *ShardKV) shouldSendShard(shard Shard) bool{
	if kv.maxNumOfShard[shard.Id] < shard.Num {
		return true
	}
	return false
}

func (kv *ShardKV) sendShardToGid(shard Shard)bool{
	if !kv.shouldSendShard(shard) {
		return true
	}
	Gid := kv.config.Shards[shard.Id]
	for {
		if servers, ok := kv.config.Groups[Gid]; ok {
			// try each server for the shard.
			args := TransferArgs{Num:kv.config.Num, Shard :shard}
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
				case ok := <- finished:
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
					// is it ok here to release the lock? Since we are still waiting for it to complete
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
	Shard := args.Shard
	// 这里就是决定到底哪些需要接受，哪些需要转发，哪些是直接的拒绝的
	if num < kv.config.Num || num < kv.maxNum{
		reply.Err = ErrWrongGroup
		return
	}
	kv.maxNum = Max(kv.maxNum, num)
	// make sure not older than itself
	Op := Op{Operation:"Accept", Shard:Shard}
	_, _, isLeader := kv.rf.Start(Op)
	if !isLeader{
		reply.Err = ErrWrongLeader
		return
	}
	reply.Err = OK
	// how to trigger the update for followers
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
	kv.seqOfClient = make(map[int64]int)
	kv.dispatcher = make(map[int]chan Notification)
	kv.persister = persister
	kv.maxNum = 0
	go kv.listenForCommitment()
	go kv.pollLatestConfig()
	//kv.readSnapshot(persister.ReadSnapshot())

	return kv
}
