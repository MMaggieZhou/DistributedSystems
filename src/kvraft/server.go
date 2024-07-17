package kvraft

import (
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
	"log"
	"sync"
	"sync/atomic"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type OpType int

const (
	PutOp OpType = iota + 1
	AppendOp
	GetOp
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Type  OpType
	Key   string
	Value string
}

type ApplyResponse struct {
	success bool
	value   string
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	data       map[string]string
	pendingOps map[int]chan ApplyResponse

	mmu              sync.Mutex
	pendingApplyMsgs []raft.ApplyMsg
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	op := Op{GetOp, args.Key, ""}
	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	ch := make(chan ApplyResponse)
	kv.mu.Lock()
	kv.pendingOps[index] = ch
	kv.mu.Unlock()
	response := <-ch
	if !response.success {
		reply.Err = ErrNoKey
	} else {
		reply.Err = OK
		reply.Value = response.value
	}
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	op := Op{PutOp, args.Key, args.Value}
	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	ch := make(chan ApplyResponse)
	kv.mu.Lock()
	kv.pendingOps[index] = ch
	kv.mu.Unlock()
	_ = <-ch
	reply.Err = OK
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	op := Op{AppendOp, args.Key, args.Value}
	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	ch := make(chan ApplyResponse)
	kv.mu.Lock()
	kv.pendingOps[index] = ch
	kv.mu.Unlock()
	_ = <-ch
	reply.Err = OK
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func (kv *KVServer) apply() {
	for m := range kv.applyCh {
		if m.CommandValid {
			index := m.CommandIndex
			op := m.Command.(Op)
			kv.mu.Lock()
			ch := kv.pendingOps[index]
			delete(kv.pendingOps, index)
			kv.mu.Unlock()
			switch op.Type {
			case GetOp:
				value, ok := kv.data[op.Key]
				if !ok {
					ch <- ApplyResponse{false, ""}
				} else {
					ch <- ApplyResponse{true, value}
				}
			case AppendOp:
				kv.data[op.Key] = kv.data[op.Key] + op.Value
				ch <- ApplyResponse{true, ""}
			case PutOp:
				kv.data[op.Key] = kv.data[op.Value]
				ch <- ApplyResponse{true, ""}
			}
		}
	}
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.data = make(map[string]string)
	kv.pendingOps = make(map[int]chan ApplyResponse)
	//go kv.takeMsg()
	//go kv.applyMsg()
	go kv.apply()
	return kv
}
