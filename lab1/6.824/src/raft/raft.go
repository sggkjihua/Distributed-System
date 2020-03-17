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
	currentTerm int
	votedFor int
	term int
	entries []LogEntry
	total int
	foundLeader bool
	// 0: follower, 1:candidate, 2:leader
	role int
	timeout int


	followerTimeout chan bool
	leaderTimeout   chan bool
	electionTimeout chan bool
	convertToFollower chan int
	terminated chan bool
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	var term  = rf.term
	// when connect back, it will still remain 2
	var isleader = (rf.role==2 && !rf.killed())
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
	Command string
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
	Entries []LogEntry
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
	// 0 indicates that 
	rf.role = 0
	//fmt.Printf("[Follower] %v currently as a fllower\n", rf.me)
	for{
		select {
		case <- rf.followerTimeout:
			// if no communication received during this period
			go rf.asCandidate()
			return
		case term := <- rf.convertToFollower:
			// may need more jude here
			rf.transferToFollower(term)
			return
		case <- rf.terminated:
			fmt.Printf("[Follower] %v has been terminated\n", rf.me)
			return
		}
	}
}


func (rf *Raft) asCandidate(){
	// as a candidate, initialize a vote
	rf.role = 1
	voteResult := make(chan bool, rf.total)
	go rf.initialVoting(voteResult)
	for{
		select{
		case term := <- rf.convertToFollower:
			// need to judge more
			if term > rf.term {
				rf.transferToFollower(term)
			}
		case win := <- voteResult:
			if win{
				go rf.asLeader()
				return
			}
		case <- rf.terminated:
			fmt.Printf("[Candidate] %v has been terminated\n", rf.me)
			return
		case <- rf.electionTimeout:
			fmt.Printf("[Candidate] Election initiated from %d has timed out, restart election\n",rf.me)
		}
	}
}

func (rf *Raft) asLeader(){
	rf.mu.Lock()
	rf.role = 2
	rf.mu.Unlock()
	// initialize a hearBeat interval
	heartBeatInterval := rand.Intn(100)+150
	for{
		select{
		case term := <- rf.convertToFollower:
			// if reveived an appendies >= my term, convert to follower
			if term >= rf.term{
				fmt.Printf("[Leader] %v found higher term: %v,convert to follower\n", rf.me, term)
				go rf.transferToFollower(term)
				return
			}
		case <- rf.terminated:
			fmt.Printf("[Leader] %v has been terminated\n", rf.me)
			return
		default:
            if rf.role == 2 {
				fmt.Printf("[Leader] %v sending heartBeat\n", rf.me)
                rf.heartBeating()
                time.Sleep(time.Duration(heartBeatInterval)*time.Millisecond)
            }
		}
	}
}

func (rf *Raft) transferToFollower(term int){
	rf.term = term
	rf.resetTimer()
	go rf.asFollower()
}

func (rf *Raft) initialVoting(vote chan bool){
	rf.mu.Lock()
	term := rf.term + 1
	rf.term ++                 // increase term
	numVoteForMe := 1          // counting the number voting for itself
	rf.votedFor = -1           // set voting for it self
	rf.mu.Unlock()
	args := RequestVoteArgs{term, rf.me, 0, 0, rf.entries}
	for i:=0;i< rf.total ;i++ {
		if i == rf.me {
			continue
		}
		go func(server int, args *RequestVoteArgs){
			reply := RequestVoteReply{}
			// need to figure out what valid actually represents
			valid := rf.sendRequestVote(server, args, &reply)

			if valid {
				if reply.VoteGranted {
					rf.mu.Lock()
					numVoteForMe ++
					rf.mu.Unlock()
				} else if reply.Term > rf.term {
					rf.convertToFollower <- reply.Term
					return
				}
			}else {
				fmt.Printf("[Candidate] %v did not receive reply from %v\n", rf.me, server)
			}
			if rf.role == 1 && numVoteForMe > rf.total/2 {
				rf.votedFor = rf.me     // set voting for it self
				vote <- true
			}
		}(i, &args)
		
	}
}

// HeartBeating logic
func (rf *Raft) heartBeating(){
	term := rf.term
	for i :=0 ;i< rf.total;i++ {
		if i == rf.me{
			continue
		}
		go func(i int){
			args := AppendEntries{term,rf.me,0,0,rf.entries,0}
			reply := AppendEntriesReply{}
			reachable := rf.sendHeartbeat(i, &args, &reply)
			if reachable {
				if reply.Term > term || !reply.Success {
					// if found that a higher term is actually higher
					rf.convertToFollower <- reply.Term
				}
			}
		}(i)
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
	fmt.Printf("[AppendEntries] %v received HeartBeat from %v with term %v\n", rf.me, leaderId, term)
	if rf.term > term {
		reply.Success = false
		reply.Term = rf.term
		fmt.Printf("[AppendEntries] term of %v[%v] is higher than %v[%v]\n", rf.me, rf.term, leaderId, term)
	}else {
		// even equals should return true, acknoledge the failure
		rf.votedFor = leaderId
		reply.Term = term
		reply.Success = true
		rf.convertToFollower <- term
		fmt.Printf("[AppendEntries] term of %v[%v] is <= than %v[%v], accept it\n", rf.me, rf.term, leaderId, term)
	}
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	fmt.Printf("[Candidate] %v sending VoteRequest to %v for term %v\n", rf.me, server, rf.term)
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term := args.Term
	candidateId := args.CandidateId
	fmt.Printf("[VoteRequest] %v received a VoteRequest from %v for term %v\n",rf.me, candidateId, term)
	if term > rf.term {
		// if the received request has a higher term number
		// accept it and change our term
		rf.votedFor = candidateId
		reply.VoteGranted = true
		reply.Term = term
		// set this as the follower channel
		rf.convertToFollower <- term
		fmt.Printf("[VoteRequest] %v accept request from %v\n", rf.me, candidateId)
	}else{
		reply.Term = rf.term
		reply.VoteGranted = false
		fmt.Printf("[VoteRequest] %v reject request from %v\n", rf.me, candidateId)
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
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
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
	rf.terminated <- true
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// reset the timmer since a certain leader has been found
func (rf *Raft) resetTimer(){
	time := rand.Intn(2500)+2000
	rf.timeout = time
}

// used to judge whether we will need to initialize the voting process
func (rf *Raft) timing(){
	for {
		time.Sleep(time.Duration(500)*time.Millisecond)
		rf.timeout -= 500;
		if rf.timeout < 0{
			rf.followerTimeout <- true
			rf.resetTimer()
		}
	}
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
	rf.timeout = rand.Intn(2000)+1000

	rf.followerTimeout = make(chan bool)
	rf.convertToFollower = make(chan int)
	rf.terminated = make(chan bool)
	go rf.asFollower()
	go rf.timing()

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
