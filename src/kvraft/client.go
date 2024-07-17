package kvraft

import (
	"6.5840/labrpc"
)
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	lastActiveLeader int
	seq              int
	id               int64
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
	// You'll have to add code here.
	ck.seq = 0
	ck.id = nrand()
	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer."+op, &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {
	// You will have to modify this function.
	args := GetArgs{key, ck.id, ck.seq}
	ck.seq++
	reply := GetReply{}
	for {
		ok := ck.servers[ck.lastActiveLeader].Call("KVServer.Get", &args, &reply)
		if !ok || reply.Err == ErrWrongLeader {
			ck.lastActiveLeader = (ck.lastActiveLeader + 1) % len(ck.servers)
			reply = GetReply{}
		} else {
			if reply.Err == ErrNoKey {
				return ""
			}
			return reply.Value
		}
	}
	return ""
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	args := PutAppendArgs{key, value, ck.id, ck.seq}
	ck.seq++
	reply := PutAppendReply{}
	for {
		ok := ck.servers[ck.lastActiveLeader].Call("KVServer."+op, &args, &reply)
		if !ok || reply.Err == ErrWrongLeader {
			ck.lastActiveLeader = (ck.lastActiveLeader + 1) % len(ck.servers)
			reply = PutAppendReply{}
		} else {
			return
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
