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

// import "bytes"
// import "../labgob"



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
	convertToFollower chan int
	terminated chan bool
	applyCh chan ApplyMsg
	timeFormat string
	timer *time.Timer

	entries []LogEntry
	commitIndex int
	lastApplied int
	nextIndex []int
	matchIndex []int
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
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}


//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
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


func (rf *Raft) asFollower(){
	rf.role = 0
	rf.resetTimer()
	for{
		select {
		case <- rf.timer.C:
			fmt.Printf("[%v Follower] %v timed out initialize a voting\n",time.Now().Format(rf.timeFormat), rf.me)
			// if no communication received during this period
			go rf.asCandidate()
			return
		case term := <- rf.convertToFollower:
			// may need more jude here
			rf.transferToFollower(term)
			return
		case <- rf.terminated:
			fmt.Printf("[%v Follower] %v has been terminated\n", time.Now().Format(rf.timeFormat), rf.me)
			return
		}
	}
}


func (rf *Raft) asCandidate(){
	// as a candidate, initialize a vote
	rf.role = 1
	voteResult := make(chan bool, rf.total)
	go rf.initialVoting(voteResult)
	go rf.electionTiming()
	for{
		select{
		case term := <- rf.convertToFollower:
			// need to judge more
			rf.transferToFollower(term)
			return 
		case win := <- voteResult:
			if win{
				go rf.asLeader()
				return
			}
		case <- rf.terminated:
			fmt.Printf("[%v Candidate] %v has been terminated\n", time.Now().Format(rf.timeFormat), rf.me)
			return
		case <- rf.electionTimeout:
			// need some logic here to check when election hsa timed out
			fmt.Printf("[%v Candidate] Election initiated from %d has timed out, restart election\n",time.Now().Format(rf.timeFormat), rf.me)
			go rf.asCandidate()
			return
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
	for{
		select{
		case term := <- rf.convertToFollower:
			// if reveived an appendies >= my term, convert to follower
				fmt.Printf("[%v Leader] %v found higher term: %v,convert to follower\n",time.Now().Format(rf.timeFormat),  rf.me, term)
				go rf.transferToFollower(term)
				return
		case <- rf.terminated:
			fmt.Printf("[%v Leader] %v has been terminated\n", time.Now().Format(rf.timeFormat), rf.me)
			return
		default:
            if rf.role == 2 {
				fmt.Printf("[%v Leader] logs are: %v\n", rf.me, rf.entries)
                rf.heartBeating()
                time.Sleep(time.Duration(heartBeatInterval)*time.Millisecond)
            }
		}
	}
}

func (rf *Raft) transferToFollower(term int){
	rf.term = term
	go rf.asFollower()
}

func (rf *Raft) initialVoting(vote chan bool){
	rf.mu.Lock()
	term := rf.term + 1
	rf.term ++
	rf.votedFor = -1
	args := rf.GenerateVoteRequest(term)
	rf.mu.Unlock()
	voted := make([] bool, rf.total)
	voted[rf.me] = true
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
				vote <- true
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
	args := RequestVoteArgs{}
	args.Term = term
	args.CandidateId = rf.me
	args.LastLogIndex = len(rf.entries)-1
	args.LastLogTerm = func(index int) int{
		if index>=0 {
			return rf.entries[index].Term
		}
		return 0
	}(args.LastLogIndex)
	return args
} 

// HeartBeating logic
func (rf *Raft) heartBeating(){
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term := rf.term
	for i :=0 ;i< rf.total;i++ {
		if i == rf.me{
			continue
		}
		go func(i int){
			args := rf.GenerateAppendEntries(term, i)
			fmt.Printf("[%v Leader] %v sending AppendEntries in term: %v to %v with logs %v \n", time.Now().Format(rf.timeFormat), rf.me, rf.term, i,args.Entries)
			reply := AppendEntriesReply{}
			reachable := rf.sendHeartbeat(i, &args, &reply)
			if reachable {
				if reply.Term > term{
					// if found that a higher term is actually higher
					// might turn into a follower
					rf.convertToFollower <- reply.Term
				}else if reply.Success{
					// if success
					rf.matchIndex[i] = len(args.Entries)+ args.PrevLogIndex
					rf.nextIndex[i] = rf.matchIndex[i] + 1
					rf.updateCommit()
				}else{
					rf.nextIndex[i] --
					rf.matchIndex[i] --
				}
			}
		}(i)
	}
}

func (rf *Raft) updateCommit(){
	if rf.commitIndex == len(rf.entries)-1{
		return
	}
	for index:=len(rf.entries)-1;index>rf.commitIndex;index--{
		cnt := 0
		for _, otherCommit := range rf.matchIndex{
			if otherCommit >= index {
				cnt ++
			}
		}
		if cnt > rf.total/2{
			tmp := rf.commitIndex
			rf.commitIndex = index
			//msg := ApplyMsg{true, rf.entries[rf.commitIndex].Command, rf.commitIndex}
			go func(){
				//rf.mu.Lock()
				//defer rf.mu.Unlock()
				for index:= rf.lastApplied+1; index<=rf.commitIndex;index++{
					msg := ApplyMsg{true, rf.entries[index].Command, index}
					rf.applyCh <- msg
					//fmt.Printf("me:%d %v\n",rf.me,msg)
					rf.lastApplied = index
				}
			}()
			fmt.Printf("[Update Commit Index] %v update its commit index from %v to %v\n", rf.me, tmp, rf.commitIndex)
			return
		}
	}
}

func (rf *Raft) GenerateAppendEntries(term int, i int) AppendEntries{
	args := AppendEntries{}
	args.Term = term
	args.LeaderId = rf.me
	args.PrevLogIndex = rf.nextIndex[i]-1
	args.PrevLogTerm = func(index int) int{
		if index < len(rf.entries) && index >= 0{
			return rf.entries[index].Term
		}
		return -1
	}(args.PrevLogIndex)

	if args.PrevLogIndex+1 == 0{
		args.Entries = rf.entries
	}else if args.PrevLogIndex+1 <= len(rf.entries){
		args.Entries = rf.entries[args.PrevLogIndex+1:]
	}
	// the PrevLogTerm may set to 0 if it actually not exists


	args.LeaderCommit = rf.commitIndex
	return args
}

func (rf *Raft) initializeNext(){
	for i:=0;i<rf.total;i++{
		rf.matchIndex[i] = 0
		rf.nextIndex[i] = len(rf.entries)
	}
}

func (rf *Raft) sendHeartbeat(server int, args *AppendEntries, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) AppendEntries(args *AppendEntries, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term := args.Term
	leaderId := args.LeaderId
	prevLogIndex := args.PrevLogIndex
	prevLogTerm  := args.PrevLogTerm
	logs := args.Entries
	leaderCommit := args.LeaderCommit
	fmt.Printf("[%v AppendEntries] %v received from %v with logs: %v in term %v\n", time.Now().Format(rf.timeFormat), rf.me, leaderId,args.Entries,term)
	if rf.term > term {
		// first judge the term, if the term of the leader is lower, reject
		reply.Success = false
		reply.Term = rf.term
		fmt.Printf("[%v AppendEntries] reject since term of %v[%v] is higher than %v[%v]\n", time.Now().Format(rf.timeFormat), rf.me, rf.term, leaderId, term)
	}else {
		reply.Term = term
		if prevLogIndex < len(rf.entries){
			// if at least it has that much logs, then it should be accepted
			reply.Success = true
			// should we consider the index out of range? 
			if prevLogIndex>=0 && rf.entries[prevLogIndex].Term != prevLogTerm {
				// delete the log after 
				rf.entries = rf.entries[:prevLogIndex]
				reply.Success = false
			}
		}else{
			fmt.Printf("[%v AppendEntries] notify false since %v has less logs than %v \n", time.Now().Format(rf.timeFormat), rf.me, rf.term)
			reply.Success = false
		}
	}
	if reply.Success {
		// if a success reply
		rf.entries = append(rf.entries, logs...)
		if leaderCommit > rf.commitIndex {
			pre := rf.commitIndex
			rf.commitIndex = GetMin(leaderCommit, len(rf.entries)-1)
			if rf.commitIndex > pre{
				go func(){
					//rf.mu.Lock()
					//defer rf.mu.Unlock()
					for index:= rf.lastApplied+1; index<=rf.commitIndex;index++{
						msg := ApplyMsg{true, rf.entries[index].Command, index}
						rf.applyCh <- msg
						//fmt.Printf("me:%d %v\n",rf.me,msg)
						rf.lastApplied = index
					}
				}()
			}
			fmt.Printf("%v update its commit index from %v to %v\n", rf.me, pre, rf.commitIndex)
		}
		rf.votedFor = leaderId
		rf.convertToFollower <- term
		fmt.Printf("[%v AppendEntries] term of %v[%v] is <= than %v[%v], accept it\n",time.Now().Format(rf.timeFormat), rf.me, rf.term, leaderId, term)
		if len(logs) >0 {
			fmt.Printf("[AppendEntries] %v logs is currently %v\n", rf.me, rf.entries)
		}
	}
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
		// compare whether the candidate is at least up to date as mine
		mlastLogIndex, mlastLogTerm := len(rf.entries)-1, 0
		if mlastLogIndex >= 0{
			mlastLogTerm = rf.entries[mlastLogIndex].Term
		}
		if lastLogTerm < mlastLogTerm || (lastLogTerm==mlastLogTerm && lastLogIndex<mlastLogIndex){
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
		rf.term = args.Term
	}
	if reply.VoteGranted {
		rf.votedFor = candidateId
		rf.convertToFollower <- term
		fmt.Printf("[%v VoteRequest] %v has accepted the request from %v\n", time.Now().Format(rf.timeFormat), rf.me, candidateId)
	}else{
		fmt.Printf("[%v VoteRequest] %v has rejected the request from %v\n", time.Now().Format(rf.timeFormat), rf.me, candidateId)
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
		index = len(rf.entries)
		rf.entries = append(rf.entries, LogEntry{Term:term,Command:command}) // append new entry from client
		rf.nextIndex[rf.me] = len(rf.entries)
		rf.matchIndex[rf.me] = len(rf.entries)-1
		//go rf.heartBeating()
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
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// reset the timmer since a certain leader has been found
func (rf *Raft) resetTimer(){
	rf.timer.Reset(rf.generateTimeout()*time.Millisecond)
}

func (rf *Raft) electionTiming(){
	waitTime := rand.Intn(2000)+1000
	for{
		time.Sleep(time.Duration(50)*time.Millisecond)
		waitTime -= 50
		if waitTime <0{
			rf.mu.Lock()
			if rf.role == 1 {
				rf.electionTimeout <- true
			}
			rf.mu.Unlock()
			return
		}
	}
}

func (rf*Raft) generateTimeout() time.Duration{
	interval := time.Duration(rand.Intn(200)+350)
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
	rf.convertToFollower = make(chan int)
	rf.terminated = make(chan bool)
	rf.electionTimeout = make(chan bool)
	rf.timeFormat = "15:04:05.000"
	rf.timer = time.NewTimer(time.Duration(rf.generateTimeout()) * time.Millisecond)
	rf.entries = []LogEntry{}
	rf.entries = append(rf.entries, LogEntry{nil, 0})
	rf.commitIndex = 0
	rf.lastApplied = -1
	rf.votedFor = -1
	// the index of the log that the ith server should receive
	rf.nextIndex = make([]int, rf.total)
	rf.matchIndex = make([]int, rf.total)
	rf.applyCh = applyCh
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

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	return rf
}
