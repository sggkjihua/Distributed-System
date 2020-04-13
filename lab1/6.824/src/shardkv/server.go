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
	dispatcher map[int] chan Notification 
	persister *raft.Persister

	maxNumAsked map[int]int   // the Num I have asked from Gid
	maxNumOfShard [10]int    // to denote the Num of shard we have so as to avoid duplicated request
	shardsReceived []Shard
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
		_, isLeader := kv.rf.GetState()
		fmt.Printf("[Get] IsLeader: %v  GID: %v shard %v does not match %v,  shardMap %v reject\n", isLeader, kv.gid, key2shard(args.Key), kv.myShard,  kv.printAllKeys())
		reply.Err = ErrWrongGroup
		return
	}
	//Shard := kv.generateShard(args.Key, "", args.Cid, args.Seq)
	op := Op{Operation:"Get", Key:args.Key, Cid:args.Cid, Seq:args.Seq, Num: kv.config.Num}
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
		if notification.Seq == op.Seq && notification.Cid == op.Cid{
			reply.Err = OK
			reply.Value = kv.getValueByKey(args.Key)
			fmt.Printf("Get command %v has been processed with val %v\n", op, reply.Value)

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
		fmt.Printf("[PutA] shard %v does not match %v , reject\n", key2shard(args.Key), kv.myShard)
		reply.Err = ErrWrongGroup
		return
	}

	Shard := kv.generateShard(args.Key, args.Value, args.Cid, args.Seq)
	op := Op{Operation:args.Op ,Key:args.Key,Val:args.Value, Cid:args.Cid, Seq:args.Seq, Shard:Shard}
	index, _, isLeader := kv.rf.Start(op)
	// start 递交上去的command不应该有重复的sequence
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	fmt.Printf("[Leader] Leader %v receive a putAppend request %v\n",kv.me, *args)
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

func (kv *ShardKV) putShard(key string, val string, shard Shard, append bool, cid int64, seq int){
	Shard, ok := kv.shardMap[shard.Id]
	if !ok{
		// not exists, simply put it
		kv.shardMap[shard.Id] = shard
		return
	}
	if append {
		// exist and need op is append
		Shard.KvMap[key] += val
		// update seq of cid
	}else {
		Shard.KvMap[key] = val
	}
	// make sure that SeqOfCid is never null
	Shard.SeqOfCid[cid] = seq
}

func (kv *ShardKV) shardNotExistsOrNotOldRequest(key string, cid int64, seq int, shard Shard, Num int) bool{
	if len(key)==0 {
		// Get, PutAppend should have a key
		// So this possiblely is Accept or Delete
		// If this is Delete simply check the Num
		// should be in the same Num, or can we simply pass it?
		return Num >= kv.config.Num
	}else{
		// PutAppend or Get, check if shard exist
		_, mine := kv.myShard[shard.Id]
		if !mine {
			return false
		}
		Shard, ok := kv.shardMap[shard.Id]
		if !ok {
			// not exists
			return shard.Num >= kv.config.Num
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
	Num := op.Num
	kv.mu.Lock()

	// uncomment this since we are not yet dealing with the duplicated request
	//val, exists := kv.seqOfClient[Cid]
	//if !exists || val<Seq {
	
	// one more filter for checking whether I should handle this request
	//shouldProcess := len(op.Key)==0 || kv.config.Shards[key2shard(op.Key)] == kv.gid || Operation=="Remove" || Operation == "Accept"
	shouldProcess := kv.shardNotExistsOrNotOldRequest(op.Key, Cid, Seq, Shard, Num)
	//shouldProcess = true
	if shouldProcess{
		Key := op.Key
		Value := op.Val
		if Operation == "Put" {
			kv.putShard(Key, Value, Shard, false, Cid, Seq)
		}else if Operation == "Append" {
			kv.putShard(Key, Value, Shard, true , Cid, Seq)
		}else if Operation == "Get" {
			// simply update the cidSeq map
			kv.shardMap[key2shard(Key)].SeqOfCid[Cid] = Seq
			//fmt.Printf("Get command %v has been processed\n", command)
		}else if Operation == "Delete"{
			delete(kv.myShard, Shard.Id)
			delete(kv.shardMap, Shard.Id)
			fmt.Printf("Remove shards %v successfully\n", Shard)
		}else if Operation == "Accept" {
			kv.shardMap[Shard.Id] = Shard
			kv.myShard[Shard.Id] = true
			fmt.Printf("Accept new shards %v successfully\n", Shard)
		}else if Operation == "Sync" {
			kv.updateShardsNum(Num)
		}
		//fmt.Printf("[ShardMap] %v [GID: %v] after Commitment: %v\n",kv.me, kv.gid, kv.shardMap)
		// update the maxSeq for this client ID after all has been done
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
func (kv *ShardKV) updateShardsNum(Num int){
	for _, shard := range kv.shardMap{
		shard.Num = Num
	}
	Shards := kv.config.Shards
	for shard, gid := range Shards{
		if gid != kv.gid{
			continue
		}
		kv.myShard[shard] = true
	}
	fmt.Printf("[After Sync] GID: %v, ShardMap: %v, MyShard: %v\n", kv.gid, kv.printAllKeys(), kv.myShard)
}

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
	var myShard map[int]bool
	fmt.Printf("[Before Installing Snapshot] %v before kvMap: %v\n",kv.me, kv.printAllKeys())

	if d.Decode(&shardMap) != nil ||
		d.Decode(&myShard) != nil {
		fmt.Printf("[Error!]: occured when reading Snapshotfrom persistence!\n")
	}else{
		kv.shardMap = shardMap
		kv.myShard = myShard
	}
	fmt.Printf("[Snapshot Installed] %v after kvMap: %v\n",kv.me, kv.printAllKeys())

}

func (kv *ShardKV) listenForCommitment() {
	for commit := range kv.applyCh {
		// for log compact logic
		if commit.CommandValid {
			fmt.Printf("[Commitment] %v [GID: %v] receive a commitment %v\n", kv.me, kv.gid, commit)
			kv.handleCommitment(commit)
			kv.checkSnapShot(commit)
			fmt.Printf("[AfterCommit] %v [GID: %v] after commitment %v\n", kv.me, kv.gid, kv.printAllKeys())

		}else {
			fmt.Printf("[ShardKV] %v [GID: %v] receive a snapShot\n", kv.me, kv.gid)
			kv.decodeSnapshot(commit)
		}
	}
}

func (kv *ShardKV) printAllKeys() string{
	res := ""
	for _, shard := range kv.shardMap {
		for k,v := range shard.KvMap{
			res += k+":"+v+" "
		}
	}
	return res
}



func (kv *ShardKV) checkSnapShot(commit raft.ApplyMsg){
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if kv.maxraftstate == -1{
		return
	}
	//fmt.Printf("RaftStateSize %v, Max: %v \n", kv.persister.RaftStateSize(), kv.maxraftstate)
	op, ok := commit.Command.(Op)
	if kv.persister.RaftStateSize() < kv.maxraftstate*8/10 && (ok && op.Operation!="Sync") {
		// when not exceed
		fmt.Printf("Operation: %v \n", op.Operation)
		return
	}
	fmt.Printf("[Compacting Required] %v will need to compact \n", kv.me)
	// taking the index of the current commit as the lastIncludedIndex
	commitedIndex := commit.CommandIndex
	term := commit.CommandTerm
	data := kv.encodeSnapshot()
	go 	kv.rf.TakeSnapShot(commitedIndex, term, data)
}


func (kv *ShardKV) encodeSnapshot() []byte {
    w := new(bytes.Buffer)
    e := gob.NewEncoder(w)
    if err := e.Encode(kv.shardMap); err != nil {
        panic(fmt.Errorf("encode shardMap fail: %v", err))
    }
    if err := e.Encode(kv.myShard); err != nil {
        panic(fmt.Errorf("encode myShard fail: %v", err))
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
		lastestConfig := kv.sm.Query(-1)
		if lastestConfig.Num > kv.config.Num{
			kv.mu.Lock()
			kv.initMyShards(lastestConfig)
			succeed := kv.fetchShards(lastestConfig)
			if succeed {
				kv.config = lastestConfig
			}
			kv.mu.Unlock()
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (kv *ShardKV) initMyShards(config shardmaster.Config){
	myShard := make(map[int]bool)
	for shard, gid := range config.Shards{
		if gid == kv.gid{
			_, ok := kv.myShard[shard]
			if ok{
				myShard[shard] = true				
			}
		}
	}
	kv.myShard = myShard
}



func (kv *ShardKV) fetchShards(config shardmaster.Config) bool{
	// basic idea is to get all shards from others.
	// and then I am able to sync with followers
	_, isLeader := kv.rf.GetState()
	if !isLeader{
		return true
	}
	Num := config.Num
	Shards := config.Shards
	//Groups := config.Groups
	Groups := kv.config.Groups  // 只能向当前任期内的 gids 请求，不能向下一个，否则有可能是离开的，就要不到了
	// 因为我不知道那些shard具体分布在哪个gid里面，所以只能一次全部都发出去
	// 或者我应该维护一个 map[gid] = Num, 来记录我现在已经打扰过的人
	shardsNeeded := make(map[int]bool)
	askedNeeded  := make(map[int]bool)
	for shard, gid:= range Shards{
		if gid != kv.gid {
			continue
		}
		_, alreadyExist := kv.myShard[shard]
		if !alreadyExist{
			// if the shard is already processed by me, no need to ask from others
			shardsNeeded[shard] = true
			askedNeeded[kv.config.Shards[shard]] = true
		}
	}
	succeed := true
	// 但是不能保证别人需要的， 所以只能到完全干净了之后才commit
	for gid, servers := range Groups{
		maxNum, exists := kv.maxNumAsked[gid]
		_, needed := askedNeeded[gid]

		if (exists && maxNum >= Num) || !needed || gid==kv.gid{
			// asked before, no need to bother
			kv.maxNumAsked[gid] = Num
			continue
		}
		succeed := kv.pollShardFromGid(Num, shardsNeeded, servers)	
		if !succeed{
			fmt.Printf("[Poll] %v polling from gid: %v fail\n", kv.gid, gid)
			succeed = false
			break
		}
		// update the maxNum asked so as not to bother again
		kv.maxNumAsked[gid] = Num
	}
	if succeed{
		// how to sync with followers with my current status
		for _, shard := range kv.shardsReceived {
			op := Op{Operation:"Accept", Shard:shard, Num:Num}
			kv.rf.Start(op)
		}
		// clear it
		kv.shardsReceived = []Shard{}
		op := Op{Operation:"Sync", Num:Num}
		fmt.Printf("[Succeed] Poll of [GID: %v] succeed sending sync for %v\n", kv.gid, Num)
		kv.rf.Start(op)
		// most likely the leader need to sync config
		//kv.config = config
	}
	return succeed
}

func (kv *ShardKV) pollShardFromGid(Num int, shardsNeeded map[int]bool, servers []string) bool{
	for {
		// try each server for the shard.
		args := FetchArgs{Num:Num, ShardsNeeded:shardsNeeded, From:kv.gid}
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
					Shards := reply.Shards
					fmt.Printf("Fetch shards from gid: %v with %v.............\n", servers[si], Shards)
					kv.shardsReceived = append(kv.shardsReceived, Shards...)
					return true
				}
				if ok && (reply.Err == ErrWrongGroup) {
					// once we found we make a mistake, we need to get the lastest config again
					return false
				}		
				break							
			case <- time.After(time.Duration(waitTime) * time.Millisecond):
				// possibly that both are sending and not able to proceed, so dead lock
				// 但是timeout的危险之处在于，如果对方回复了并且删除了自己的log，但是这边却因为timeOut delete掉了
				// 所以其实还是可以多加一个delete的请求，让对方把log delete掉，或者干脆就不delete了
				reply.Err = ErrWrongGroup
				fmt.Printf("Timeout %v for %v\n", kv.gid, servers[si])
				return false
			}
				// ... not ok, or ErrWrongLeader
		}
	}
}

func (kv *ShardKV) GetShard(args *FetchArgs, reply *FetchReply){
	fmt.Printf("%v [GID: %v] receive fetchArgs %v from %v\n", kv.me, kv.gid, args, args.From)
	kv.mu.Lock()
	defer kv.mu.Unlock()
	Num := args.Num
	ShardsNeeded := args.ShardsNeeded
	if Num < kv.config.Num{
		// and old request, reject
		reply.Err = ErrWrongGroup
		return
	}
	//fmt.Printf("Gid[%v] did I come here for %v with %v\n", kv.gid, ShardsNeeded, kv.shardMap)
	Shards := make([]Shard, 0)
	for shardId, _ := range ShardsNeeded {
		shard, ok := kv.shardMap[shardId]
		if ok {
			Shards = append(Shards, shard)
		}
	}
	for _, shard := range Shards {
		op := Op{Operation:"Delete", Shard:shard, Num:Num}
		_, _, isLeader := kv.rf.Start(op)
		if !isLeader{
			reply.Err = ErrWrongLeader
			return 
		}
	}
	reply.Shards = Shards
	reply.Err = OK
	fmt.Printf("[GetShard] %v reply successfully %v for request %v\n", kv.gid, reply, ShardsNeeded)

}


func RandTime() int{
	return 500 + rand.Intn(400)
}


func Max(a int, b int) int{
	if a>b{
		return a
	}
	return b
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

	kv.dispatcher = make(map[int]chan Notification)
	kv.persister = persister
	kv.shardsReceived = make([]Shard,0)
	kv.maxNumAsked = make(map[int]int)
	kv.myShard = make(map[int]bool)

	for shard, gid := range kv.config.Shards {
		if gid == kv.gid {
			kv.myShard[shard] = true
		}
	}
	
	//fmt.Printf("[GID] %v come to live again!!!!!!\n", kv.gid)

	kv.shardMap = make(map[int]Shard)
	go kv.listenForCommitment()
	go kv.pollLatestConfig()
	//kv.readSnapshot(persister.ReadSnapshot())

	return kv
}
