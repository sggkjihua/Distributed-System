package kvraft

import (
	"time"
	
	"fmt"
	"../labrpc"
	"crypto/rand"
	"math/big"
)


type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	cid int64
	requestId int
	leaderId int


}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	ck.cid = nrand()
	// You'll have to add code here.
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//

func (ck *Clerk) Get(key string) string {
	server := ck.leaderId
	ck.requestId ++
	args := GetArgs{Key:key, Cid:ck.cid, Seq:ck.requestId}
	for{
		requestDone := make(chan bool, 1)
		reply := GetReply{}
		go func(i int){
			requestDone <- ck.servers[i].Call("KVServer.Get", &args, &reply)
		}(server)
		select{
		case ok:= <- requestDone:
			if ok && reply.IsLeader{
				// found leader and get the answer
				val := reply.Value
				ck.leaderId = server
				fmt.Printf("[Get] Map[%v] = %v from leader %v\n", key, val, ck.leaderId)
				return val
			}else{
				server = (server+1) % len(ck.servers)
				fmt.Printf("Request Get not leader, retry other server %v\n", server)
			}
		case <- time.After(time.Duration(500)*time.Millisecond):
			server = (server+1) % len(ck.servers)
			fmt.Printf("Request Get timeout, retry other server %v\n", server)
			continue
		}
	}
}


/*
func (ck *Clerk) Get(key string) string {
	//val := ""
	args := GetArgs{Key:key, Cid:ck.cid}
	server := ck.leaderId
	for{
		requestDone := make(chan bool, 1)
		reply := GetReply{}
		go func(i int){
			requestDone <- ck.servers[i].Call("KVServer.Get", &args, &reply)
		}(server)
		select{
		case ok:= <- requestDone:
			if ok && reply.IsLeader{
				// found leader and get the answer
				val := reply.Value
				ck.leaderId = server
				fmt.Printf("[Get] find Map[%v]:value %v from leader %v\n", key, val, ck.leaderId)
				return val
			}else{
				server = (server+1) % len(ck.servers)
				fmt.Printf("Request Get not leader, retry other server %v\n", server)

			}
		case <- time.After(time.Duration(500)*time.Millisecond):
			server = (server+1) % len(ck.servers)
			fmt.Printf("Request Get timeout, retry other server %v\n", server)
			continue
		}
	}
}

*/
//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	server := ck.leaderId
	ck.requestId ++
	args := PutAppendArgs{Op:op, Key:key, Value:value, Cid: ck.cid, Seq:ck.requestId}
	
	for {
		requestDone := make(chan bool, 1)
		reply := PutAppendReply{}
		go func(i int){
			requestDone <- ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
		}(server)
		select{
		case reachable := <- requestDone:
			if reachable && reply.IsLeader || reply.Processed{
				// do remember to update the current leader again
				ck.leaderId = server
				fmt.Printf("PutAppend Request %v has been commited by leader %v\n", args, ck.leaderId)
				return
			}else{
				server = (server+1) % len(ck.servers)
				fmt.Printf("Request PutA not leader, retry other server %v\n", server)
			}
		case <- time.After(time.Duration(500)*time.Millisecond):
			// request time out for server, will try next
			server = (server+1) % len(ck.servers)
			fmt.Printf("Request PutA not commited, retry server %v\n", server)
			continue
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
