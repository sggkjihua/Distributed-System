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
	Config shardmaster.Config
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

	maxNumOfShard map[int]int   // the Num I have asked from Gid
	
	shardsNeeded map[int]bool
	shardsToDiscard map[int]bool
	askShardsFrom [10]int

	isLeader bool
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


func (kv *ShardKV) printState(op string){
	if kv.isLeader{
		fmt.Printf("[After %v] Server %v [Gid: %v], myShards: %v, keyMap: %v\n",op, kv.me, kv.gid, kv.myShard, kv.printAllKeys())
	}
}


func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	isMyShard := kv.checkShard(args.Key)
	kv.mu.Unlock()
	if !isMyShard{
		_, isLeader := kv.rf.GetState()
		fmt.Printf("[Get] IsLeader: %v  GID: %v is not responsible for shard %v REJECT %v\n", isLeader, kv.gid, key2shard(args.Key), kv.printAllKeys())
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

		if notification.Seq == op.Seq && notification.Cid == op.Cid && kv.hasBeenProcessed(args.Key, args.Cid, args.Seq){
			delete(kv.dispatcher, index)
			reply.Err = OK
			reply.Value = kv.getValueByKey(args.Key)
			fmt.Printf("[Get Value] GID: %v Command %v has been processed with val %v\n", kv.gid, op, reply.Value)

		}else{
			if notification.Valid {
				reply.Err = ErrWrongLeader
			}else{
				// when the key should not be processed by this
				reply.Err = ErrWrongGroup
			}
		}
		kv.mu.Unlock()
		return
	case <- time.After(time.Duration(600)*time.Millisecond):
		reply.Err = ErrWrongLeader
		return
	}
}


func (kv *ShardKV) hasBeenProcessed(key string, cid int64, seq int) bool {
	shard := key2shard(key)
	Shard, exists := kv.shardMap[shard]
	if !exists {
		return false
	}
	maxSeq, ok := Shard.SeqOfCid[cid]
	return !ok || maxSeq>=seq 
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
		fmt.Printf("[PutAppend] GID: %v is not responsible for shard %v , reject %v\n", kv.gid, key2shard(args.Key), kv.myShard)
		reply.Err = ErrWrongGroup
		return
	}
	// for PutAppend, only key, val, cid, seq are needed, no need to go for shard, space consuming
	op := Op{Operation:args.Op ,Key:args.Key,Val:args.Value, Cid:args.Cid, Seq:args.Seq, Num:kv.config.Num}
	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		// if not leader
		reply.Err = ErrWrongLeader
		return
	}
	fmt.Printf("[Request Received] Leader %v [GID: %v] receive a putAppend request %v\n",kv.me, kv.gid, *args)
	
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
		if notification.Seq == op.Seq && notification.Cid == op.Cid && kv.hasBeenProcessed(args.Key, args.Cid, args.Seq){
			kv.mu.Lock()
			delete(kv.dispatcher, index)
			kv.mu.Unlock()
			fmt.Printf("[Request Commited] Leader %v [GID: %v] commited a putAppend request %v\n",kv.me, kv.gid, *args)
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

func (kv *ShardKV) generateShard(key string, val string, cid int64, seq int) Shard{
	KvMap := make(map[string]string)
	SeqOfCid := make(map[int64]int)
	SeqOfCid[cid] = seq
	KvMap[key] = val
	shard := Shard{Id: key2shard(key), KvMap:KvMap, SeqOfCid:SeqOfCid, Num: kv.config.Num}
	return shard
}

func (kv *ShardKV) putShard(key string, val string, append bool, cid int64, seq int){
	shard := key2shard(key)
	Shard, ok := kv.shardMap[shard]
	if !ok{
		// not exists, simply put it
		kv.shardMap[shard] = kv.generateShard(key, val, cid, seq)
		return
	}
	if append {
		// exist and need op is append
		Shard.KvMap[key] += val
	}else {
		Shard.KvMap[key] = val
	}
	// make sure that SeqOfCid is never null
	Shard.SeqOfCid[cid] = seq
	// update the config num
	Shard.Num = kv.config.Num
}

func (kv *ShardKV) shouldProcessRequest(Key string, Cid int64, Seq int, Shard Shard, Num int, Op string) bool{
	shard := key2shard(Key)
	if Op=="Get" || Op=="Put" || Op=="Append" {
		// Put/Append/Get, first check if we are responsible for this
		_, responsible := kv.myShard[shard]
		if !responsible{
			// we are not responsible for this shard currently
			if kv.isLeader{
				fmt.Printf("[Not in myShard] Key %v [Shard: %v] is not int myShard %v\n", Key, shard, kv.myShard)
			}
			return false			
		}
		// Next we need to check whether this is actually an old request
		Shard, ok := kv.shardMap[shard]
		if ok {
			seq, exist := Shard.SeqOfCid[Cid]
			if exist && seq >= Seq {
				// 这里其实应该是大于等于的，但是不知道为什么当start的时候会重新去accept
				fmt.Printf("[Sequence Less] Key %v [Shard: %v] Seq %v is <= record %v\n", Key, shard, Seq, seq)
				return false
			}
		}
		//return true
	}else if Op == "Accept" {
		shard = Shard.Id
		if mShard, ok := kv.shardMap[shard]; ok {
			preSeqOfCid := mShard.SeqOfCid
			return len(Shard.SeqOfCid) > len(preSeqOfCid)
		}
	}
	if Num >= kv.config.Num {
		return true
	}
	fmt.Printf("[My Num is higher] Key %v [Shard: %v] my num %v is higher than %v\n", Key, shard, kv.config.Num, Num)
	return false
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
	Config := op.Config
	kv.mu.Lock()

	shouldProcess := kv.shouldProcessRequest(op.Key, Cid, Seq, Shard, Num, Operation)
	if shouldProcess{
		Key := op.Key
		Value := op.Val
		// for put, append, get we all need to make sure that we are currently resoisible for that shard
		if Operation == "Put" {
			kv.putShard(Key, Value, false, Cid, Seq)
		}else if Operation == "Append" {
			kv.putShard(Key, Value, true , Cid, Seq)
		}else if Operation == "Get" {
			// simply update the cidSeq map
			kv.shardMap[key2shard(Key)].SeqOfCid[Cid] = Seq
		}else if Operation == "Delete"{
			kv.handleDelete(Shard)
			//kv.printState("DELETE")
		}else if Operation == "Accept" {
			kv.handleAccept(Shard)
			//kv.printState("ACCEPT")
		}else if Operation == "Reconfiguration" {
			kv.handleReconfiguration(Config)
			//kv.printState("RECONFIGURATION")
		}
		kv.printState(Operation)
	}else{
		if kv.isLeader{
			fmt.Printf("[Ignore] Server: %v, Gid[%v] ignore Commitment: %v since myShard: %v, shardMap: %v\n", kv.me, kv.gid, commit, kv.myShard, kv.shardMap)
		}
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

func (kv *ShardKV) handleAccept(shard Shard){
	shard.Num = kv.config.Num
	kv.shardMap[shard.Id] = shard
	kv.myShard[shard.Id] = true
	delete(kv.shardsNeeded, shard.Id)
	//fmt.Printf("[Accept] Gid: %v new shards %v successfully\n",kv.gid, shard)
}

func (kv *ShardKV) handleDelete(shard Shard){
	delete(kv.myShard, shard.Id)
	delete(kv.shardMap, shard.Id)
	delete(kv.shardsToDiscard, shard.Id)
	//fmt.Printf[("[Remove] Gid: %v shards %v successfully\n",kv.gid, shard)
}

// handle the reconfiguration logic
// will update the myShard and determine what shards to discard
func (kv *ShardKV) handleReconfiguration(config shardmaster.Config){
	Shards := config.Shards
	for shard, gid := range Shards {
		shouldDelete := true
		if gid != kv.gid {
			// not my shard and I hold it [DISCARD]
			if _, hold := kv.myShard[shard]; hold {
				kv.shardsToDiscard[shard] = true
			}
		}else{
			if _, alreadyHold := kv.myShard[shard];!alreadyHold && config.Num!=1{
				// not hold by me currently, will need to [ASK] for it
				kv.shardsNeeded[shard] = true
			}else if config.Num == 1{
				// 针对第一个加入的gid，特别进行全部都为true的处理
				kv.myShard[shard] = true
				shouldDelete = false
			}else{
				if Shard, ok:= kv.shardMap[shard]; ok{
					// if mine and exists a Shard, update the Num for this shard
					Shard.Num = config.Num
				}
				shouldDelete = false
			}
		}
		if shouldDelete {
			// if I am not responsible for it anymore
			// so that we do not need to consider too much when receiving request from client
			delete(kv.myShard, shard)
		}
	}
	kv.askShardsFrom = kv.config.Shards
	kv.config = config
	//fmt.Printf("[After Reconfiguration] GID: %v, ShardMap: %v, MyShard: %v\n", kv.gid, kv.printAllKeys(), kv.myShard)
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
			if kv.isLeader {
				fmt.Printf("[Commitment] %v [GID: %v] receive a commitment %v\n", kv.me, kv.gid, commit)
			}
			kv.handleCommitment(commit)
			kv.checkSnapShot(commit)
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
	op, _ := commit.Command.(Op)
	//if kv.persister.RaftStateSize() < kv.maxraftstate*8/10 && (ok && op.Operation!="Sync") {
	if kv.persister.RaftStateSize() < kv.maxraftstate*8/10 {
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

func (kv *ShardKV) pollExpectedConfig(num int)(shardmaster.Config, bool){
	config := kv.sm.Query(num)
	if config.Num == kv.config.Num {
		return kv.config, false
	}
	return config, true
}

func (kv *ShardKV) pollNextConfig() {
	// used to poll the configuration
	// will trigger only when isLeader and the preConfigs is finished
	for{
		_, isLeader := kv.rf.GetState()
		kv.mu.Lock()
		if !isLeader || len(kv.shardsNeeded)>0 || len(kv.shardsToDiscard)>0 {
			// if there is still some shards that we did not receive for this configuration
			// wait until we fully get all of them
			kv.mu.Unlock()
		}else{
			nextNum := kv.config.Num + 1
			config, shouldUpdateMyState := kv.pollExpectedConfig(nextNum)
			// release the lock earlier so as to avoid dead lock for start
			kv.mu.Unlock()
			if shouldUpdateMyState {
				fmt.Printf("[Reconfig] Server %v [GID: %v] sending reconfiguration for %v\n", kv.me, kv.gid, config)
				op := Op{Operation:"Reconfiguration",Config:config, Num:nextNum}
				// 通过leader来统一进行 reconfiguration
				kv.rf.Start(op)
			}
			kv.isLeader = true
		}
		time.Sleep(100 * time.Millisecond)
	}
}


func (kv *ShardKV) pollShards(){
	// periodically poll shards from others
	// used this function in case that the leader breakdown
	for{
		kv.fetchShards()
		time.Sleep(200 * time.Millisecond)
	}
}

func (kv *ShardKV) fetchShards(){
	// basic idea is to get all shards from others.
	// and then I am able to sync with followers
	_, isLeader := kv.rf.GetState()
	kv.mu.Lock()
	if !isLeader || len(kv.shardsNeeded)==0 {
		// no need to ask for any shards from others
		kv.mu.Unlock()
		return 
	}
	fmt.Printf("[Fetching] Server %v [GID: %v] is requesting shards %v from others\n", kv.me, kv.gid, kv.shardsNeeded)
	curConfig := kv.config
	preConfig := kv.sm.Query(curConfig.Num-1)
	Num := curConfig.Num
	Shards := preConfig.Shards
	Groups := preConfig.Groups
	Gids := kv.gidsToAsked(Shards)
	wg := sync.WaitGroup{}
	args := FetchArgs{Num:Num, ShardsNeeded:kv.shardsNeeded, From:kv.gid}
	for gid := range Gids {
		servers := Groups[gid]
		// used this to prevent from being called again
		wg.Add(1)
		go func(servers []string) {
			defer wg.Done()
			for si := 0; si < len(servers); si++ {
				reply := FetchReply{}
				srv := kv.make_end(servers[si])
				if srv.Call("ShardKV.GetShard", &args, &reply) && reply.Err == OK{
					shardsReturned := reply.Shards
					for _, sh := range shardsReturned {
						// receive the shard return from other gid
						// put it into log and syn with followers
						op := Op{Operation:"Accept", Num:Num, Shard:sh}
						kv.rf.Start(op)
					}
				}
			}
		}(servers)
	}
	kv.mu.Unlock()
	wg.Wait()
	fmt.Printf("[Fetched Successfully] %v [GID: %v]\n", kv.me, kv.gid)
}

func (kv *ShardKV) gidsToAsked(preShards [10]int) map[int]bool{
	// used to get the gids that we need to asked for the shards
	gids := make(map[int]bool)
	for shard := range kv.shardsNeeded {
		gid:= preShards[shard]
		if gid != kv.gid{
			gids[gid] = true
		}
	}
	return gids
}


func (kv *ShardKV) GetShard(args *FetchArgs, reply *FetchReply){
	_, isLeader := kv.rf.GetState()
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	fmt.Printf("[GetShard] %v [GID: %v] receive fetchArgs %v from %v\n", kv.me, kv.gid, args, args.From)
	Num := args.Num
	ShardsNeeded := args.ShardsNeeded
	preConfig := kv.sm.Query(Num-1)
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if Num < kv.config.Num{
		// and old request, reject
		reply.Err = ErrWrongGroup
		return
	}
	Shards := make([]Shard, 0)
	//kv.printState("GetShard")
	fmt.Printf("[GetShard] Server %v [GID: %v] myShard: %v, shardMap: %v\n", kv.me, kv.gid, kv.myShard, kv.shardMap)
	for shardId := range ShardsNeeded {
		if preConfig.Shards[shardId] != kv.gid{
			continue
		}
		shard, ok := kv.shardMap[shardId]
		// condition here might be really critical
		if ok {
			Shards = append(Shards, deepCopyOfShard(shard))
		}else{
			fakeShard := Shard{Id: shardId, Num: Num-1, KvMap:make(map[string]string), SeqOfCid:make(map[int64]int)}
			Shards = append(Shards, fakeShard)
		}
		// only return those which is hold by me last configuration
	}
	// 也许之后需要把这里优化一下，确保之前传出去的shards都不在这里了
	for _, shard := range Shards {
		// 具体意思就是在 Num th config 需要将这个shard 从自己这里抹除
		op := Op{Operation:"Delete", Shard:shard, Num:Num}
		kv.rf.Start(op)
	}
	reply.Shards = Shards
	reply.Err = OK
	fmt.Printf("[GetShard] %v reply successfully %v for request %v\n", kv.gid, reply, ShardsNeeded)

}

func deepCopyOfShard(shard Shard)Shard{
	kvMap := make(map[string]string)
	seqOfCid := make(map[int64]int)
	for k,v := range shard.KvMap{
		kvMap[k] = v
	}
	for k,v := range shard.SeqOfCid{
		seqOfCid[k] = v
	}
	nShard := Shard{Id:shard.Id, Num:shard.Num, SeqOfCid:seqOfCid, KvMap:kvMap}
	return nShard

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
	kv.maxNumOfShard = make(map[int]int)
	kv.myShard = make(map[int]bool)
	kv.shardsNeeded = make(map[int]bool)
	kv.shardsToDiscard = make(map[int]bool)


	kv.shardMap = make(map[int]Shard)

	
	
	for shard, gid := range kv.config.Shards {
		if gid == kv.gid {
			kv.myShard[shard] = true
		}
	}

	fmt.Printf("[After %v] Server %v [Gid: %v], myShards: %v, keyMap: %v\n", "INIT", kv.me, kv.gid, kv.myShard, kv.printAllKeys())

	//fmt.Printf("[GID] %v come to live again!!!!!!\n", kv.gid)

	go kv.listenForCommitment()
	go kv.pollNextConfig()
	go kv.pollShards()
	//kv.readSnapshot(persister.ReadSnapshot())

	return kv
}
