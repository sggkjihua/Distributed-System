package shardmaster


import (
	"sort"
	"time"
	"encoding/gob"
	"fmt"
	"bytes"
	"../raft"
	"../labrpc"
	"sync"
	"../labgob"
)


type ShardMaster struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.

	configs []Config // indexed by config num

	seqOfClient map[int64]int
	dispatcher map[int] chan Notification
	persister *raft.Persister

	// term, gid, servers
	gid2Servers map[int][]string
	gid2Shards map[int][]int
}

type Notification struct{
	Cid int64
	Seq int
}

type Op struct {
	// Your data here.
	Operation string
	// join args
	Servers map[int][]string
	// leave args
	GIDs []int

	// move args
	Shard int
	GID   int

	// get the configuration of that sequence
	Num int 

	Seq int
	Cid int64
}


func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	op := Op{Operation:"Join" , Servers:args.Servers, Cid:args.Cid, Seq:args.Seq}

	index, _, isLeader := sm.rf.Start(op)

	// start 递交上去的command不应该有重复的sequence
	if ! isLeader {
		reply.WrongLeader = true
		reply.Err = "Not leader, try other server"
		return
	}

	sm.mu.Lock()
	if _, ok := sm.dispatcher[index]; !ok {
		sm.dispatcher[index] = make(chan Notification, 1)
	}
	ch := sm.dispatcher[index]
	sm.mu.Unlock()
	select{
	case  notification := <- ch :
		// as required, all previous operation should reveal on the get request
		sm.mu.Lock()
		delete(sm.dispatcher, index)
		sm.mu.Unlock()
		if notification.Seq == op.Seq && notification.Cid == op.Cid {
			reply.WrongLeader = false
			fmt.Printf("[ShardMaster] After join %v\n", sm.configs[len(sm.configs)-1])
		}else{
			reply.WrongLeader = true
		}
		return
	case <- time.After(time.Duration(600)*time.Millisecond):
		reply.WrongLeader = true
		return
	}
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	op := Op{Operation:"Leave" , GIDs:args.GIDs, Cid:args.Cid, Seq:args.Seq}

	index, _, isLeader := sm.rf.Start(op)

	if ! isLeader {
		reply.WrongLeader = true
		reply.Err = "Not leader, try other server"
		return
	}

	sm.mu.Lock()
	if _, ok := sm.dispatcher[index]; !ok {
		sm.dispatcher[index] = make(chan Notification, 1)
	}
	ch := sm.dispatcher[index]
	sm.mu.Unlock()
	select{
	case  notification := <- ch :
		// as required, all previous operation should reveal on the get request
		sm.mu.Lock()
		delete(sm.dispatcher, index)
		sm.mu.Unlock()
		if notification.Seq == op.Seq && notification.Cid == op.Cid {
			reply.WrongLeader = false
			fmt.Printf("[ShardMaster] After leave %v\n", sm.configs[len(sm.configs)-1])

		}else{
			reply.WrongLeader = true
		}
		return
	case <- time.After(time.Duration(600)*time.Millisecond):
		reply.WrongLeader = true
		return
	}
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) {
	op := Op{Operation:"Move" , GID:args.GID, Shard:args.Shard, Seq:args.Seq, Cid:args.Cid}

	index, _, isLeader := sm.rf.Start(op)

	// start 递交上去的command不应该有重复的sequence
	if ! isLeader {
		reply.WrongLeader = true
		reply.Err = "Not leader, try other server"
		return
	}

	sm.mu.Lock()
	if _, ok := sm.dispatcher[index]; !ok {
		sm.dispatcher[index] = make(chan Notification, 1)
	}
	ch := sm.dispatcher[index]
	sm.mu.Unlock()
	select{
	case  notification := <- ch :
		// as required, all previous operation should reveal on the get request
		sm.mu.Lock()
		delete(sm.dispatcher, index)
		sm.mu.Unlock()
		if notification.Seq == op.Seq && notification.Cid == op.Cid {
			reply.WrongLeader = false
		}else{
			reply.WrongLeader = true
		}
		return
	case <- time.After(time.Duration(600)*time.Millisecond):
		reply.WrongLeader = true
		return
	}
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) {
	op := Op{Operation:"Query" , Num:args.Num,  Cid:args.Cid, Seq:args.Seq}

	index, _, isLeader := sm.rf.Start(op)

	// start 递交上去的command不应该有重复的sequence
	if ! isLeader {
		reply.WrongLeader = true
		reply.Err = "Not leader, try other server"
		return
	}

	sm.mu.Lock()
	if _, ok := sm.dispatcher[index]; !ok {
		sm.dispatcher[index] = make(chan Notification, 1)
	}
	ch := sm.dispatcher[index]
	sm.mu.Unlock()
	select{
	case  notification := <- ch :
		// as required, all previous operation should reveal on the get request
		sm.mu.Lock()
		delete(sm.dispatcher, index)
		sm.mu.Unlock()
		if notification.Seq == op.Seq && notification.Cid == op.Cid {
			reply.Config = sm.handleQuery(op.Num)
			//reply.Config.Num = args.Num
			reply.WrongLeader = false
		}else{
			reply.WrongLeader = true
		}
		return
	case <- time.After(time.Duration(600)*time.Millisecond):
		reply.WrongLeader = true
		return
	}
}

func (sm *ShardMaster) handleQuery(num int)Config{
	//config := Config{Groups:sm.gid2Servers}
	//fmt.Printf("[QUery] %v sm.configs: %v\n",sm.me, sm.configs)
	lastNum := len(sm.configs)
	index := -1
	if num == -1 || num >= lastNum{
		index = lastNum-1
	}else{
		index = num
	}
	//config.Num = index
	return sm.configs[index]
}


//
// the tester calls Kill() when a ShardMaster instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sm *ShardMaster) Kill() {
	sm.rf.Kill()
	// Your code here, if desired.
}

// needed by shardsm tester
func (sm *ShardMaster) Raft() *raft.Raft {
	return sm.rf
}



func (sm *ShardMaster) handleCommitment(commit raft.ApplyMsg){
	command := commit.Command
	op, ok := command.(Op)
	if !ok {
		// transformation failed, normally will not have this issue
		return
	}

	// get the Seq and Cid from the commitment
	Seq := op.Seq
	Cid := op.Cid
	sm.mu.Lock()
	val, exists := sm.seqOfClient[Cid]
	if !exists || val < Seq {

		Operation := op.Operation
		Servers := op.Servers
		Gids := op.GIDs
		Shard := op.Shard
		Gid := op.GID
		//Num := op.Num
		switch Operation{
		case "Join":
			//fmt.Printf("Join request received for servers %v\n", Servers)
			sm.handleJoinRequest(Servers)
			//fmt.Printf("[Join] %v Currently the configs is %v\n",sm.me, sm.configs)
		case "Leave":
			//fmt.Printf("Leave request received for servers %v\n", Gids)
			sm.handleLeaveRequest(Gids)
			//fmt.Printf("[Leave] %v Currently the configs is %v\n",sm.me, sm.configs[len(sm.configs)-1])
		case "Move":
			//fmt.Printf("Move request received for group %v to shard %v\n",Gid, Shard)
			sm.handleMoveRequest(Gid, Shard)
		case "Query":
			//fmt.Printf("Query request received for num %v\n", Num)
		}
		// update the maxSeq for this client ID after all has been done
		sm.seqOfClient[Cid] = Seq
	}
	sm.mu.Unlock()

	ch, ok := sm.dispatcher[commit.CommandIndex]
	if ok{
		notify := Notification{
			Cid:  op.Cid,
			Seq: op.Seq,
		}
		
		ch <- notify
	}
}


func GetMax(a int , b int)int{
	if a>b{
		return a
	}
	return b
}

func (sm *ShardMaster) handleJoinRequest(Servers map[int][]string){
	// record the newly joined servers
	config := Config{}
	nGids := make(map[int]bool)
	nKeys := make([]int, 0)
	nextGid2Shards := make(map[int][]int)
	nextGid2Servers:= make(map[int][]string)
	for k, v := range Servers {
		nGids[k] = true
		nKeys = append(nKeys, k)
		sm.gid2Servers[k] = v
	}
	keys := make([]int,0)
	for k,v := range sm.gid2Servers {
		nextGid2Servers[k] = v
		keys = append(keys,k)
	}
	sort.Ints(keys)
	sort.Ints(nKeys)
	// get remaining shards, if all are new joiner, simply take all shards
	// if not, collect those more than average out
	remaingShards := make([]int, 0)
	if len(keys) == len(nGids) {
		for i:=0;i<NShards;i++{
			remaingShards = append(remaingShards, i)
		}
	}else{
		average := GetMax(NShards/len(keys), 1)
		/*
		if average > NShards-average*(len(keys)-1) {
			average ++
		}
		*/
		for _, gid := range keys {
			_, ok := nGids[gid]
			if ok {
				continue
			}
			shards, hold:= sm.gid2Shards[gid]
			if hold && len(shards) > average{
				shardsToCollect := shards[average:]
				remaingShards = append(remaingShards, shardsToCollect...)
				nextGid2Shards[gid] = shards[:average]
				for _, shard := range nextGid2Shards[gid]{
					config.Shards[shard] = gid
				}
			} else if hold{
				nextGid2Shards[gid] = shards[:]
				for _, shard := range nextGid2Shards[gid]{
					config.Shards[shard] = gid
				}
			}
		}
	}
	gidIndex := 0
	sort.Ints(remaingShards)
	for _, shard := range remaingShards{
		gid := nKeys[gidIndex]
		config.Shards[shard] = gid
		nextGid2Shards[gid] = append(nextGid2Shards[gid], shard)
		gidIndex = (gidIndex+1)%len(nKeys)
	}

	sm.gid2Shards = nextGid2Shards
	config.Groups = nextGid2Servers
	config.Num    = len(sm.configs) 
	sm.configs = append(sm.configs, config)
}


func (sm *ShardMaster) handleLeaveRequest(Gids []int){
	// get the next number of gid2shards
	config := Config{}

	gid2Servers := make(map[int][]string)

	preGid2Shards := sm.gid2Shards
	nextGid2Shards := make(map[int][]int)

	// delete the gid and collect all the shards
	remaining := make([]int, 0)
	for _, gid := range Gids{
		remaining = append(remaining, preGid2Shards[gid]...)
		delete(sm.gid2Servers,gid)
	}

	keys := make([]int, 0)
	for gid, servers:= range sm.gid2Servers{
		gid2Servers[gid]= servers
		keys = append(keys, gid)
		shards := preGid2Shards[gid]
		for _,val := range shards{
			config.Shards[val] = gid
			nextGid2Shards[gid] = append(nextGid2Shards[gid], val)
		}
	}
	sort.Ints(keys)
	average := 1
	if len(keys) >0 {
		average = NShards/len(keys)
		if NShards%len(keys) != 0 {
			average ++
		}
	}
	Index := 0
	sIndex :=0 
	for sIndex < len(remaining) && len(keys)>0{
		gid := keys[Index]
		shards, hold := nextGid2Shards[gid]
		if !hold{
			nextGid2Shards[gid] = make([]int, 0)
			shards = nextGid2Shards[gid]
		}
		if len(shards) < average {
			shard := remaining[sIndex]
			shards = append(shards, shard)
			config.Shards[shard] = gid
			sIndex ++
			nextGid2Shards[gid] = shards
		}
		Index = (Index+1)%len(keys)
	}
	// initialize the shards
	
	config.Groups = gid2Servers
	config.Num = len(sm.configs)
	sm.configs = append(sm.configs, config)
	sm.gid2Shards = nextGid2Shards
}



func (sm *ShardMaster) handleMoveRequest(GID int, shard int){
	// move this shard to that GID
	config := Config{}
	preConfig := sm.configs[len(sm.configs)-1]
	if preConfig.Shards[shard] == GID {
		config = sm.deepCopyOfConfig()
		config.Num = len(sm.configs)
		sm.configs = append(sm.configs, config)
		return
	}
	preGid := preConfig.Shards[shard]

	// initialize the shards since we simply need to modify two position
	copy(config.Shards[:], preConfig.Shards[:])
	preGid2Shards := sm.gid2Shards
	nextGid2Shards := make(map[int][]int)
	for gid, shards := range preGid2Shards{
		size := len(shards)
		nShards := make([]int, size)
		copy(nShards[:], shards[:])
		nextGid2Shards[gid] = nShards
	}
	targetShards, ok := nextGid2Shards[GID]
	var oldShard int
	if ok {
		oldShard := targetShards[0]
		config.Shards[oldShard] = preGid
		targetShards[0] = shard
	}else{
		nextGid2Shards[GID] = append(make([]int,0), shard)
	}
	for index, val := range nextGid2Shards[preGid]{
		if val == shard {
			if ok {
				// if we did found an other shard to switch, we do not need to cancate
				nextGid2Shards[preGid][index] = oldShard
			}else{
				nextGid2Shards[preGid] = append(nextGid2Shards[preGid][:index], nextGid2Shards[preGid][index+1:]...)
			}
			break
		}
	}

	config.Shards[shard] = GID
	config.Groups = sm.gid2Servers	
	config.Num = len(sm.configs)
	sm.configs = append(sm.configs, config)
	sm.gid2Shards = nextGid2Shards
}

func (sm *ShardMaster) deepCopyOfConfig() Config{
	config := Config{}
	preConfig := sm.configs[len(sm.configs)-1]
	copy(config.Shards[:], preConfig.Shards[:])
	nGroups := make(map[int][]string)
	Groups := preConfig.Groups
	for k,v := range Groups {
		nGroups[k] = v
	} 
	config.Groups = nGroups
	return config
}



func (sm *ShardMaster) decodeSnapshot(commit raft.ApplyMsg) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	/*
	data := commit.Data
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var smMap map[string]string
	//fmt.Printf("[Before Installing Snapshot] %v before smMap: %v\n",sm.me)

	if d.Decode(&seqOfClient) != nil ||
		d.Decode(&smMap) != nil {
		fmt.Printf("[Error!]: occured when reading Snapshotfrom persistence!\n")
	}else{
		sm.kvMap = smMap
		sm.seqOfClient = seqOfClient
	}
	//fmt.Printf("[Snapshot Installed] %v after smMap: %v\n",sm.me, sm.kvMap)
	*/
}


func (sm *ShardMaster) listenForCommitment() {
	for commit := range sm.applyCh {
		// for log compact logic
		if commit.CommandValid {
			//fmt.Printf("[ShardMaster] %v receive a commitment %v\n", sm.me, commit)
			sm.handleCommitment(commit)
			sm.checkSnapShot(commit)
		}else {
			//fmt.Printf("[ShardMaster] %v receive a snapShot\n", sm.me)
			sm.decodeSnapshot(commit)
		}
	}
}


func (sm *ShardMaster) checkSnapShot(commit raft.ApplyMsg){
	sm.mu.Lock()
	defer sm.mu.Unlock()
	/*
	if sm.maxraftstate == -1{
		return
	}
	//fmt.Printf("RaftStateSize %v, Max: %v \n", sm.persister.RaftStateSize(), sm.maxraftstate)

	if sm.persister.RaftStateSize() < sm.maxraftstate*8/10 {
		// when not exceed
		return
	}
	//fmt.Printf("[Compacting Required] %v will need to compact, smMap:%v \n", sm.me, sm.smMap)
	// taking the index of the current commit as the lastIncludedIndex
	commitedIndex := commit.CommandIndex
	term := commit.CommandTerm
	data := sm.encodeSnapshot()
	go 	sm.rf.TakeSnapShot(commitedIndex, term, data)
	*/
}


func (sm *ShardMaster) encodeSnapshot() []byte {
    w := new(bytes.Buffer)
    e := gob.NewEncoder(w)
    if err := e.Encode(sm.seqOfClient); err != nil {
        panic(fmt.Errorf("encode seqOfClient fail: %v", err))
	}
	/*
    if err := e.Encode(sm.kvMap); err != nil {
        panic(fmt.Errorf("encode smMap fail: %v", err))
	}
	*/
    return w.Bytes()
}


//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardMaster {
	sm := new(ShardMaster)
	sm.me = me

	sm.configs = make([]Config, 1)
	sm.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sm.applyCh = make(chan raft.ApplyMsg)
	sm.rf = raft.Make(servers, me, persister, sm.applyCh)

	sm.mu = sync.Mutex{}
	// Your code here.

	sm.configs = []Config{} // indexed by config num
	sm.configs = append(sm.configs, Config{})

	sm.seqOfClient = make(map[int64]int)
	sm.dispatcher =  make(map[int] chan Notification)
	sm.persister = persister

	sm.gid2Servers = make(map[int][]string)
	go sm.listenForCommitment()

	return sm
}