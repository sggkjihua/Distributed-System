package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"fmt"
	"math/rand"
	"time"
	"sync"
	"sync/atomic"
	"../labrpc"
)

import "bytes"
import "../labgob"



//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
	CommandTerm int
	Data []byte
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	votedFor int
	term int
	total int
	// 0: follower, 1:candidate, 2:leader
	role int
	electionTimeout chan bool
	convertToFollower chan FollowerInfo
	convertToFollowerDone chan bool
	terminated chan bool
	applyCh chan ApplyMsg
	timeFormat string
	timer *time.Timer

	entries []LogEntry
	commitIndex int
	lastApplied int
	nextIndex []int
	matchIndex []int

	lastIncludedIndex int
	lastIncludedTerm  int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	var term  = rf.term
	// when connect back, it will still remain 2
	var isleader = rf.role==2
	// Your code here (2A).
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// persist may need to be locked, should check whether the calling function has been locked
	data := rf.generateRaftState()
	rf.persister.SaveRaftState(data)
}

func (rf *Raft) generateRaftState() []byte{
	// called by persist, depending on persist
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	e.Encode(rf.term)
	e.Encode(rf.votedFor)
	e.Encode(rf.entries)
	e.Encode(rf.lastIncludedIndex)
	e.Encode(rf.lastIncludedTerm)
	return w.Bytes()
}


//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	// could add a lock here
	// called only when started
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var term int
	var votedFor int
	var logs []LogEntry
	var lastIncludedIndex int
	var lastIncludedTerm int
	if d.Decode(&term) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&logs)!=nil||
		d.Decode(&lastIncludedIndex)!=nil||
		d.Decode(&lastIncludedTerm)!=nil {
		fmt.Printf("[Error!]: occured when reading from persistence!\n")
	} else {
	   rf.term = term
	   rf.votedFor = votedFor
	   rf.entries  = logs
	   rf.lastIncludedIndex = lastIncludedIndex
	   rf.lastIncludedTerm = lastIncludedTerm

	   rf.commitIndex = GetMax(rf.commitIndex, lastIncludedIndex)
	   rf.lastApplied = GetMax(rf.lastApplied, lastIncludedIndex)

	}
	//fmt.Printf("[Read from persistence]: %v : term: %v votedFor: %v logs: %v\n", rf.me, rf.term, rf.votedFor, rf.entries)
}

func (rf *Raft) readSnapshot(data []byte){
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	go func(){
		commitment := ApplyMsg{CommandValid:false, Data:data}
		rf.applyCh <- commitment
	}()

}


//here for taking snapshot logic
func (rf *Raft) TakeSnapShot(index int, term int, snapShot []byte){
	fmt.Printf("[Take Snap Shot] %v is taking a snapshot for index: %v of term: %v\n",rf.me,index, term)
	rf.mu.Lock()
	fmt.Printf("[Take Snap Shot] %v takes the lock successfully\n", rf.me)
	defer rf.mu.Unlock()
	// adding the lock here will result in dead lock, will need to figure out a way to deal with this 
	if index < rf.lastIncludedIndex {
		// when an older request arrived
		return
	}
	// 如果只是单纯的自己更新的话，现在应该是没有问题的

	// lastincluded Index 应该至少是总长度-1
	// 如果比总长度-1要大，那么应该变成
	baseIndex := rf.lastIncludedIndex
	rf.lastIncludedIndex = index  // update index
	// 想象成 1 + 1， 本来是[{0, nil}, {1, command}], 所以总数还是2
	// index + 1 就是理论上的总数，如果size是小于这个值的，那么直接把最后的那个index的term
	size := len(rf.entries) + baseIndex
	if size -1 < index {
		// term here might need to be clearreplaced
		rf.entries =append([]LogEntry{}, LogEntry{Term:term})
	}else{
		rf.lastIncludedTerm  = rf.entries[index-baseIndex].Term  // update term
		rf.entries = rf.entries[index-baseIndex:] // only the uncommited entries left
	}
	// update the commitIndex and lastAppliedIndex
	rf.commitIndex = GetMax(rf.commitIndex, index)
	rf.lastApplied = GetMax(rf.lastApplied, index)
	rf.lastIncludedTerm = GetMax(rf.lastIncludedTerm, rf.term)

	fmt.Printf("[After Taking Snapshot] %v lastIncludedIndex: %v, lastIncludedTerm: %v, Commit: %v, lastApplied: %v, logs: %v \n",
	 rf.me, rf.lastIncludedIndex, rf.lastIncludedTerm, rf.commitIndex, rf.lastApplied, rf.entries)


	raftState := rf.generateRaftState()
	rf.persister.SaveStateAndSnapshot(raftState, snapShot)
}



type LogEntry struct {
	Command interface{}
	Term int
}

// To implement heartbeats for empty entries
type AppendEntries struct {
	Term int
	LeaderId int
	PrevLogIndex int
	PrevLogTerm int
	Entries []LogEntry //log entries to store (empty for heartbeat; may send more than one for efficiency)
	LeaderCommit int
}

type AppendEntriesReply struct {
	// Your data here (2A).
	//currentTerm, for leader to update itself
	Term int 
	// true if follower contained entry matching prevLogIndex and prevLogTerm
	Success bool
	ConflictIndex int
	ConflictEntries []LogEntry
}


//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term int
	CandidateId int
	LastLogIndex int
	LastLogTerm int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term int
	VoteGranted bool
}


type InstallSnapshotArgs struct {
	Term int
	LeaderId int
	LastIncludeIndex int
	LastIncludeTerm int
	Offset int
	Data []byte
	Done bool
}

type InstallSnapshotReply struct {
	Term int
}



type FollowerInfo struct {
	Term int
    VotedFor int
    ShouldResetTimer bool
}


func (rf *Raft) pushChangeToFollower(info FollowerInfo){
	rf.convertToFollower <- info
	<- rf.convertToFollowerDone
}


func (rf *Raft) asFollower(){
	rf.role = 0
	//rf.resetTimer()
	for{
		select {
		case <- rf.timer.C:
			fmt.Printf("[%v Follower] %v timed out initialize a voting\n",time.Now().Format(rf.timeFormat), rf.me)
			// if no communication received during this period
			go rf.asCandidate()
			return
		// is there any way to triger this only once??
		// and terminated
		case info := <- rf.convertToFollower:
			// may need more jude here
			if info.Term > rf.term{
				go rf.transferToFollower(info)
				return
			}
			rf.convertToFollowerDone <- true
			if info.ShouldResetTimer{
				rf.resetTimer()
			}
		case <- rf.terminated:
			fmt.Printf("[%v Follower] %v has been terminated\n", time.Now().Format(rf.timeFormat), rf.me)
			return
		}
		
	}
}


func (rf *Raft) asCandidate(){
	// as a candidate, initialize a vote
	rf.role = 1
	//go rf.electionTiming()
	for{
		voteResult := make(chan bool, rf.total)
		go rf.initialVoting(voteResult)
		rf.resetTimer()
		select{
		case info := <- rf.convertToFollower:
			// need to judge more
			go rf.transferToFollower(info)
			return 
		case win := <- voteResult:
			if win{
				go rf.asLeader()
				return
			}
		case <- rf.terminated:
			fmt.Printf("[%v Candidate] %v has been terminated\n", time.Now().Format(rf.timeFormat), rf.me)
			return
		case <- rf.timer.C:
			// need some logic here to check when election hsa timed out
			fmt.Printf("[%v Candidate] Election initiated from %d has timed out, restart election\n",time.Now().Format(rf.timeFormat), rf.me)
		}
	}
}

func (rf *Raft) asLeader(){
	go func(){
		rf.mu.Lock()
		defer rf.mu.Unlock()
		if rf.role != 1{
			return
		}
		rf.initializeNext()
        rf.matchIndex[rf.me] = rf.nextIndex[rf.me] - 1
		rf.role = 2
	}()
	// initialize a hearBeat interval
	heartBeatInterval := rand.Intn(100)+150
	//fmt.Printf("..... %v is now a leader, logs are %v\n", rf.me, rf.entries)
	for{
		select{
		case info := <- rf.convertToFollower:
			// if reveived an appendies >= my term, convert to follower
			fmt.Printf("[%v Leader] %v found higher term: %v,convert to follower\n",time.Now().Format(rf.timeFormat),  rf.me, info)
			//fmt.Printf("[%v Leader] logs are: %v\n", rf.me, rf.entries)
			go rf.transferToFollower(info)
			return
		case <- rf.terminated:
			fmt.Printf("[%v Leader] %v has been terminated\n", time.Now().Format(rf.timeFormat), rf.me)
			return
		default:
            if rf.role == 2 {
				//fmt.Printf("[%v Leader] logs are: %v\n", rf.me, rf.entries)
                rf.heartBeating()
                time.Sleep(time.Duration(heartBeatInterval)*time.Millisecond)
            }
		}
	}
}

func (rf *Raft) transferToFollower(info FollowerInfo){
	rf.role = 0
	rf.term = GetMax(rf.term, info.Term)
    if info.VotedFor != -1 {
        rf.votedFor = info.VotedFor
    }
    //为什么重置nextIndex?
    //避免本身是旧leader，makeAppendEntryRequest还在使用nextIndex + rf.Logs构造AppendEntriesArgs
    //而如果rf.Logs被新的leader截断，那么可能出现nextIndex > len(rf.Logs)情况，导致makeAppendEntryRequest里index out of range
    //为什么重置nextIndex为0?
    //注意nextIndex不能设置为len(logs)，比如以下场景：
    //1. 发送AppendEntries时response，转化为follower，此时rf.Logs未修改，nextIndex = len(rf.Logs)
    //2. 接着收到新leader的AppendEntries，可能删减rf.Logs
    //3. BeFollower里的follower的case v:= <- changeToFollower触发，调用go rf.TransitionToFollower(v)后返回，释放AppendEntries函数里的锁
    //4. makeAppendEntryRequest使用删减后的rf.Logs 与 未修改的nextIndex，可能出错
    //5. go rf.TransitionToFollower(v)异步运行到这里，才设置nextIndex = len(rf.Logs)
	
	rf.InitNextIndex()
    rf.persist()
    rf.convertToFollowerDone <- true

    if info.ShouldResetTimer {
		rf.resetTimer()
    }
    rf.asFollower()
}

	
func (rf *Raft) InitNextIndex()  {
    for i := 0; i < rf.total; i++ {
		rf.nextIndex[i] = 1
		rf.matchIndex[i] = 0
    }
}

func (rf *Raft) initialVoting(vote chan bool){
	
	voted := make([] bool, rf.total)
	voted[rf.me] = true

	rf.mu.Lock()
	if rf.role != 1{
		// if turns out not to be a leader anymore
		rf.mu.Unlock()
		return
	}
	term := rf.term + 1
	rf.term ++
	rf.votedFor = rf.me

	rf.persist()
	args := rf.GenerateVoteRequest(term)
	rf.mu.Unlock()

	for i:=0;i< rf.total ;i++ {
		if i == rf.me {
			continue
		}
		go func(server int, args *RequestVoteArgs){
			reply := RequestVoteReply{}
			// need to figure out what valid actually represents
			valid := rf.sendRequestVote(server, args, &reply)
			// if accepted and voteGranted by the receiver
			voted[server] = valid && reply.VoteGranted
			// the read of role == 1 here should we delete it?
			if rf.role == 1 && WinMajority(voted) {
				rf.votedFor = rf.me     // set voting for it self
				vote <- true            // vote is set to be buffered, therefore no need to worry about being stucked
			}
			if reply.Term > rf.term {
				rf.term = reply.Term
			}
		}(i, &args)
	}
}

func WinMajority(voted []bool) bool {
	// check whether the candidate has received the majority
	cnt := 0
	for i:=0;i<len(voted);i++{
		if voted[i]{
			cnt ++
		}
	}
	return cnt > len(voted)/2
}


func (rf *Raft) GenerateVoteRequest(term int) RequestVoteArgs{
	baseIndex := rf.lastIncludedIndex
	args := RequestVoteArgs{}
	args.Term = term
	args.CandidateId = rf.me
	args.LastLogIndex = len(rf.entries)-1+baseIndex
	args.LastLogTerm = func(index int) int{
		if index>=0 {
			return rf.entries[index].Term
		}
		return 0
	}(args.LastLogIndex-baseIndex)
	return args
} 


// HeartBeating logic
func (rf *Raft) heartBeating(){
	//rf.mu.Lock()
	//defer rf.mu.Unlock()
	term := rf.term
	for i :=0 ;i< rf.total;i++ {
		if i == rf.me{
			continue
		}
		shouldSendSnapShot := rf.nextIndex[i] < rf.lastIncludedIndex
		if shouldSendSnapShot {
			go rf.sendSnapShot(i)
		}else{
			go rf.sendAppendEntries(i, term)
		}
	}
}


func (rf *Raft) sendAppendEntries(i int, term int){
	args := rf.GenerateAppendEntries(term, i)
	//fmt.Printf("[Leader] %v is now sending heartbeating\n", rf.me)
	//fmt.Printf("[%v Leader] %v sending AppendEntries in term: %v to %v with logs %v \n", time.Now().Format(rf.timeFormat), rf.me, rf.term, i,args.Entries)
	reply := AppendEntriesReply{}
	reachable := rf.sendAppendEntriesRPC(i, &args, &reply)
	if reachable && term==rf.term {
		if reply.Term > term{
			// if found that a higher term is actually higher
			// might turn into a follower

			// defect here, since there might be tons of cases when term is higher than mine
			// and in the meantime, there is nothing waiting for this so it might stuck here
			info := FollowerInfo{reply.Term, -1, false}
			rf.pushChangeToFollower(info)
		}else if reply.Success{
			// if succes
			rf.matchIndex[i] = len(args.Entries)+ args.PrevLogIndex
			rf.nextIndex[i] = rf.matchIndex[i] + 1
			rf.updateCommit()
		}else{
			rf.mu.Lock()
			baseIndex := rf.lastIncludedIndex
			conflictIndex := reply.ConflictIndex
			conflictEntries := reply.ConflictEntries
			tmp := rf.nextIndex[i]
			maxMatchIndex := conflictIndex
			for index:= conflictIndex;index< len(rf.entries)+ baseIndex && index-baseIndex>=0  && index-conflictIndex < len(conflictEntries);index++{
				if rf.entries[index-baseIndex].Term != conflictEntries[index-conflictIndex].Term{
					rf.nextIndex[i] = index
					rf.matchIndex[i] = rf.nextIndex[i]-1
					break
				}else{
					maxMatchIndex = index
				}
			}
			if rf.nextIndex[i] == tmp {
				rf.nextIndex[i] = maxMatchIndex + 1
				rf.matchIndex[i] = rf.nextIndex[i]-1
			}
			rf.mu.Unlock()

		}
	}
}


func (rf *Raft) sendSnapShot(i int){
	// May possibly need to add a lock here
	data := rf.persister.ReadSnapshot()
	args := InstallSnapshotArgs{
		LastIncludeIndex: rf.lastIncludedIndex,
		LastIncludeTerm: rf.lastIncludedTerm,
		Term: rf.term,
		LeaderId: rf.me,
		Data: data,
	}
	//fmt.Printf("%v sending Snapshot to %v with %v\n",rf.me, i, args)
	//fmt.Printf("[%v Leader] %v sending InstallSnaShot in term: %v to %v with logs %v \n", time.Now().Format(rf.timeFormat), rf.me, rf.term, i,args.Entries)
	reply := InstallSnapshotReply{}
	reachable := rf.sendInstallSnapShotRPC(i, &args, &reply)
	if reachable {
		if reply.Term > rf.term{
			// when found higher term, may need to convert to follower
			// should reset timer?
			info := FollowerInfo{reply.Term, -1, false}
			rf.pushChangeToFollower(info)
		}else{
			// install successfully, can update next and match now
			rf.nextIndex[i] = rf.lastIncludedIndex+1
			rf.matchIndex[i] = rf.lastIncludedIndex
		}
	}
}



func (rf *Raft) updateCommit(){
	rf.mu.Lock()
	defer rf.mu.Unlock()

	baseIndex := rf.lastIncludedIndex
	//fmt.Printf("%v commitIndex: %v, lastIncludedIndex: %v, entries: %v, len: %v\n",rf.me, rf.commitIndex, baseIndex, rf.entries, len(rf.entries))
	if rf.commitIndex == baseIndex + len(rf.entries)-1{
		return
	}
	for index:=len(rf.entries)-1;index>rf.commitIndex-baseIndex;index--{
		cnt := 0
		for _, otherCommit := range rf.matchIndex{
			if otherCommit >= index + baseIndex {
				cnt ++
			}
		}
		if cnt > rf.total/2 && rf.entries[index].Term == rf.term{
			// if found a highest index that has been confirmed by most of the followers
			tmp := rf.commitIndex
			rf.commitIndex = index + baseIndex
			for i:= rf.lastApplied+1; i<=rf.commitIndex;i++{
				msg := ApplyMsg{CommandValid:true, Command: rf.entries[i-baseIndex].Command, CommandIndex: i, CommandTerm:rf.entries[i-baseIndex].Term}
				rf.applyCh <- msg
				rf.lastApplied = index+baseIndex
			}
			fmt.Printf("[Update Commit Index] %v update its commit index from %v to %v\n", rf.me, tmp, rf.commitIndex)
			return
		}
	}
}

func (rf *Raft) GenerateAppendEntries(term int, i int) AppendEntries{
	// may need to add a lock here, called by sendAppendEntries
	baseIndex := rf.lastIncludedIndex
	args := AppendEntries{}
	args.Term = term
	args.LeaderId = rf.me
	args.PrevLogIndex = rf.nextIndex[i]-1
	args.PrevLogTerm = func(index int) int{
		if index < len(rf.entries) && index >= 0{
			return rf.entries[index].Term
		}
		return rf.lastIncludedTerm
	}(args.PrevLogIndex-baseIndex)

	if args.PrevLogIndex-baseIndex+1 == 0{
		args.Entries = rf.entries
	}else if args.PrevLogIndex-baseIndex+1 <= len(rf.entries){
		startIndex := GetMax(0, args.PrevLogIndex-baseIndex+1)
		args.Entries = rf.entries[startIndex:]
	}
	args.LeaderCommit = rf.commitIndex
	return args
}

func (rf *Raft) initializeNext(){
	// no need to add lock, see asLeader
	for i:=0;i<rf.total;i++{
		rf.matchIndex[i] = 0
		rf.nextIndex[i] = rf.lastIncludedIndex + len(rf.entries)
	}
}

func (rf *Raft) sendInstallSnapShotRPC(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapShot", args, reply)
	return ok
}

func (rf *Raft) InstallSnapShot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term := args.Term
	leaderId := args.LeaderId
	lastIncludedIndex := args.LastIncludeIndex
	lastIncludedTerm  := args.LastIncludeTerm
	data := args.Data

	fmt.Printf("[InstallSnapshot] %v receive a snapshotRequest from %v\n", rf.me, leaderId)
	if term < rf.term {
		// old leader detected
		reply.Term = rf.term
		return
	}else{
		reply.Term = args.Term
	}
	// should send snapshot to kvServer
	if lastIncludedIndex > rf.lastIncludedIndex{
		// only when leader's index is higher than mine
		rf.lastIncludedTerm = lastIncludedTerm
		rf.lastApplied = GetMax(lastIncludedIndex, rf.lastApplied)
		rf.commitIndex = GetMax(lastIncludedIndex, rf.commitIndex)
		go rf.TakeSnapShot(lastIncludedIndex, lastIncludedTerm, data)
	}
	go func(){
		commitment := ApplyMsg{CommandValid:false, Data:data}
		rf.applyCh <- commitment
	}()
	// if Success
	info := FollowerInfo{term, leaderId, true}
	rf.pushChangeToFollower(info)

}



func (rf *Raft) sendAppendEntriesRPC(server int, args *AppendEntries, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) AppendEntries(args *AppendEntries, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	baseIndex := rf.lastIncludedIndex
	term := args.Term
	leaderId := args.LeaderId
	prevLogIndex := args.PrevLogIndex
	prevLogTerm  := args.PrevLogTerm
	logs := args.Entries
	leaderCommit := args.LeaderCommit
	less := false
	//fmt.Printf("[%v AppendEntries] %v received from %v with logs: %v in term %v\n", time.Now().Format(rf.timeFormat), rf.me, leaderId,args.Entries,term)
	if rf.term > term {
		// first judge the term, if the term of the leader is lower, reject
		reply.Success = false
		reply.Term = rf.term
		fmt.Printf("[%v AppendEntries] reject since term of %v[%v] is higher than %v[%v]\n", time.Now().Format(rf.timeFormat), rf.me, rf.term, leaderId, term)
	}else {
		//fmt.Printf("prevLogIndex: %v,len(leaderEntries):%v, baseIndex: %v, len(entries): %v\n", prevLogIndex,len(logs),  baseIndex, len(rf.entries))
		//考虑 prevLogIndex: 53,baseIndex: 57, len(entries): 4，那么 prevLogIndex确实小于当前baseIndex+len(entries)-1，但是却是在lastIncluded之前了
		lessEntriesThanExpected := prevLogIndex > baseIndex+len(rf.entries)-1
		ahead := baseIndex - prevLogIndex >0
		doesNotMatch := !lessEntriesThanExpected && !ahead && rf.entries[prevLogIndex-baseIndex].Term != prevLogTerm
		if lessEntriesThanExpected || doesNotMatch  {
			reply.Success = false
			if doesNotMatch {
				// 这里应该是使得其长度为0的唯一可能？？
				rf.entries = rf.entries[:prevLogIndex-baseIndex]
			}else if lessEntriesThanExpected{
				less = true
			}
			lastIndex := rf.handleConflict()
			reply.ConflictIndex = lastIndex + baseIndex
			reply.ConflictEntries = rf.entries[lastIndex:]
			fmt.Printf("[Handle Conflict]%v conflict index is %v, my entries:%v and logs from leader %v\n", rf.me, reply.ConflictIndex, rf.entries, logs)
		}else if ahead{
			// when the included index is actually larger than the leader thinks me has
			reply.Success = false
			reply.ConflictIndex = GetMax(baseIndex, rf.lastApplied)+1

		}else{
			if len(rf.entries)-1+baseIndex != prevLogIndex {
				// now we have more than expected and previous all nmatch
				rf.entries = rf.entries[:prevLogIndex-baseIndex+1]
			}
			rf.entries = append(rf.entries, logs...)
			reply.Success = true
			rf.persist()
			if leaderCommit > rf.commitIndex {
				pre := rf.commitIndex
				rf.commitIndex = GetMin(leaderCommit, baseIndex + len(rf.entries)-1)
				if rf.commitIndex > pre{
					for index:= rf.lastApplied+1; index<=rf.commitIndex;index++{
						//fmt.Printf("%v going to apply command: %v with lastApplied: %v commitIndex: %v base: %v\n", rf.me, rf.entries, rf.lastApplied, rf.commitIndex, baseIndex)
						msg := ApplyMsg{CommandValid: true, Command: rf.entries[index-baseIndex].Command, CommandIndex:index, CommandTerm: term}
						rf.applyCh <- msg
						rf.lastApplied = index
					}
				}
			}
		}
	}
	if reply.Success {
		// if a success reply
		info := FollowerInfo{args.Term, args.LeaderId, true}
		rf.pushChangeToFollower(info)
		fmt.Printf("[%v AppendEntries] term of %v[%v] is <= than %v[%v], accept it\n",time.Now().Format(rf.timeFormat), rf.me, rf.term, leaderId, term)
	}else if less {
		info := FollowerInfo{args.Term, args.LeaderId, true}
		rf.pushChangeToFollower(info)
	}
}

func (rf *Raft) handleConflict() int {
	// no need to add lock, see AppendEntries
	fmt.Printf("[Handle Conflict] %v entries: %v\n", rf.me, rf.entries)
	lastIndex := len(rf.entries)-1
	// for 3B when it is empty
	if lastIndex == -1 {
		return 0
	}
	lastTerm := rf.entries[GetMax(len(rf.entries)-1, 0)].Term
	for i:= len(rf.entries)-1; i>=0;i--{
		if rf.entries[i].Term == lastTerm {
			lastIndex = i
		}else{
			break
		}
	}
	return lastIndex
}

func GetMin(a, b int) int {
	if a > b{
		return b
	}
	return a
}

func GetMax(a, b int) int {
	if a > b{
		return a
	}
	return b
}



func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	fmt.Printf("[%v Candidate] %v sending VoteRequest to %v for term %v\n",time.Now().Format(rf.timeFormat),  rf.me, server, rf.term)
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term := args.Term
	candidateId := args.CandidateId
	lastLogIndex := args.LastLogIndex
	lastLogTerm  := args.LastLogTerm 

	fmt.Printf("[%v VoteRequest] %v received a VoteRequest from %v for term %v\n",time.Now().Format(rf.timeFormat), rf.me, candidateId, term)
	// 1. Reply false if term < currentTerm (§5.1)
	// 2. If votedFor is null or candidateId, and candidate’s log is at 
	// least as up-to-date as receiver’s log, grant vote (§5.2, §5.4)
	if term < rf.term {
		// if my term is actually higher than the candidate, reject
		reply.VoteGranted = false
		reply.Term = rf.term
	}else{
		baseIndex := rf.lastIncludedIndex
		// compare whether the candidate is at least up to date as mine
		mlastLogIndex, mlastLogTerm := len(rf.entries)-1, 0
		if mlastLogIndex >= 0{
			mlastLogTerm = rf.entries[mlastLogIndex].Term
		}
		if lastLogTerm < mlastLogTerm || (lastLogTerm==mlastLogTerm && lastLogIndex<mlastLogIndex+baseIndex){
			// last log of candidate is stale than mine in term or index
			reply.VoteGranted = false
			reply.Term = args.Term
		}else if rf.term < term {
			// if candidate at higher term, accept
			reply.VoteGranted = true
			reply.Term = args.Term
		}else{
			// if at the same term, reject
			reply.VoteGranted = false
			reply.Term = rf.term
		}
	}
	//set currentTerm = T, convert to follower
	if rf.term < args.Term {
		info := FollowerInfo{args.Term ,args.CandidateId, reply.VoteGranted}
		rf.pushChangeToFollower(info)
	}
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	index := rf.term
	term := rf.term
	isLeader := rf.role == 2
	if isLeader {
		baseIndex := rf.lastIncludedIndex
		index = baseIndex + len(rf.entries)
		rf.entries = append(rf.entries, LogEntry{Term:term,Command:command}) // append new entry from client
		rf.nextIndex[rf.me] = baseIndex +  len(rf.entries)
		rf.matchIndex[rf.me] = baseIndex + len(rf.entries)-1
		rf.persist()
	}
	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	rf.timer.Stop()
	rf.terminated <- true
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// reset the timmer since a certain leader has been found
func (rf *Raft) resetTimer(){
	rf.timer.Reset(rf.generateTimeout()*time.Millisecond)
}

func (rf*Raft) generateTimeout() time.Duration{
	interval := time.Duration(rand.Intn(300)+300)
	return interval
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.mu = sync.Mutex{}
	rf.term = 0
	rf.total = len(peers)
	rf.convertToFollower = make(chan FollowerInfo)
	rf.convertToFollowerDone = make(chan bool)
	rf.terminated = make(chan bool)
	rf.electionTimeout = make(chan bool)
	rf.timeFormat = "15:04:05.000"
	rf.timer = time.NewTimer(time.Duration(rf.generateTimeout()) * time.Millisecond)
	rf.entries = []LogEntry{}
	rf.entries = append(rf.entries, LogEntry{nil, 0})
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.votedFor = -1
	// the index of the log that the ith server should receive
	rf.nextIndex = make([]int, rf.total)
	rf.matchIndex = make([]int, rf.total)
	rf.applyCh = applyCh
	
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.readSnapshot(persister.ReadSnapshot())

	go rf.asFollower()

	// Your initialization code here (2A, 2B, 2C).
	// Fill in the RequestVoteArgs and RequestVoteReply structs
	// Modify Make() to create a background goroutine
	// that will kick off leader election periodically
	// by sending out RequestVote RPCs when it hasn't 
	// heard from another peer for a while.
	// This way a peer will learn who is the leader, 
	// if there is already a leader, or become the leader itself. 

	// Implement the RequestVote() RPC handler so that servers will vote for one another.
	return rf
}
