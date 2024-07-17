package kvraft

import (
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

// failure scenarios:
// partition:
// function could hang forever if a leader is partitioned. (single client)
// need add timeout; if there are more requests coming in, then
// either use term returned from rf.Start or applyMsg to detect leader change
// unreliable network:
// apply msg dedup

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
	Type      OpType
	Key       string
	Value     string
	ClientId  int64
	ClientSeq int
}

type ApplyResponse struct {
	value string
	error Err
}

type PendingOp struct {
	clientId int64
	seq      int
	ch       chan ApplyResponse
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	data       map[string]string
	pendingOps map[int]PendingOp // log index -> PendingOp

	mmu                sync.Mutex
	maxCommitClientSeq map[int64]int
	currentTerm        int
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	op := Op{GetOp, args.Key, "", args.ClientId, args.ClientSeq}
	index, term, isLeader := kv.rf.Start(op)
	DPrintf("[starts]kv store %d takes get key %s,seq %d, index %d, term %d, isleader %t", kv.me, args.Key, args.ClientSeq, index, term, isLeader)
	if !isLeader {
		kv.mu.Lock()
		if term > kv.currentTerm {
			kv.currentTerm = term
			kv.onTermChange()
		}
		kv.mu.Unlock()
		reply.Err = ErrWrongLeader
		return
	}
	ch := make(chan ApplyResponse)
	kv.mu.Lock()
	kv.pendingOps[index] = PendingOp{args.ClientId, args.ClientSeq, ch}
	kv.mu.Unlock()
	select {
	case response := <-ch:
		reply.Err = response.error
		reply.Value = response.value
	case <-time.After(time.Second * 2):
		reply.Err = ErrWrongLeader
	}
	kv.mu.Lock()
	delete(kv.pendingOps, index)
	kv.mu.Unlock()
	DPrintf("[returns] kv store %d takes get key %s, seq %d, returns %s, val %s, index %d, term %d, isleader %t", kv.me, args.Key, args.ClientSeq, reply.Err, reply.Value, index, term, isLeader)
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	op := Op{PutOp, args.Key, args.Value, args.ClientId, args.ClientSeq}
	index, term, isLeader := kv.rf.Start(op)
	DPrintf("[starts]kv store %d takes put %s:%s,seq%d, index %d, term %d, isleader %t", kv.me, args.Key, args.Value, args.ClientSeq, index, term, isLeader)
	if !isLeader {
		kv.mu.Lock()
		if term > kv.currentTerm {
			kv.currentTerm = term
			kv.onTermChange()
		}
		kv.mu.Unlock()
		reply.Err = ErrWrongLeader
		return
	}
	ch := make(chan ApplyResponse)
	kv.mu.Lock()
	kv.pendingOps[index] = PendingOp{args.ClientId, args.ClientSeq, ch}
	kv.mu.Unlock()
	select {
	case response := <-ch:
		reply.Err = response.error
	case <-time.After(time.Second * 2):
		reply.Err = ErrWrongLeader
	}
	kv.mu.Lock()
	delete(kv.pendingOps, index)
	kv.mu.Unlock()
	DPrintf("[returns]kv store %d takes put %s:%s,seq %d, returns %s, index %d, term %d, isleader %t", kv.me, args.Key, args.Value, args.ClientSeq, reply.Err, index, term, isLeader)
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	op := Op{AppendOp, args.Key, args.Value, args.ClientId, args.ClientSeq}
	index, term, isLeader := kv.rf.Start(op)
	DPrintf("[starts]kv store %d takes append %s:%s,seq %d, index %d, term %d, isleader %t", kv.me, args.Key, args.Value, args.ClientSeq, index, term, isLeader)
	if !isLeader {
		kv.mu.Lock()
		if term > kv.currentTerm {
			kv.currentTerm = term
			kv.onTermChange()
		}
		kv.mu.Unlock()
		reply.Err = ErrWrongLeader
		return
	}
	ch := make(chan ApplyResponse)
	kv.mu.Lock()
	kv.pendingOps[index] = PendingOp{args.ClientId, args.ClientSeq, ch}
	kv.mu.Unlock()
	select {
	case response := <-ch:
		reply.Err = response.error
	case <-time.After(time.Second * 2):
		reply.Err = ErrWrongLeader
	}
	kv.mu.Lock()
	delete(kv.pendingOps, index)
	kv.mu.Unlock()
	DPrintf("[returns]kv store %d takes append %s:%s seq %d, returns %s, index %d, term %d, isleader %t", kv.me, args.Key, args.Value, args.ClientSeq, reply.Err, index, term, isLeader)
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

// not thread safe
func (kv *KVServer) onTermChange() { // might change from leader to follower
	for _, op := range kv.pendingOps {
		op.ch <- ApplyResponse{"", ErrWrongLeader}
	}
}

func (kv *KVServer) apply() {
	for m := range kv.applyCh {
		if m.CommandValid {
			index := m.CommandIndex
			applOp := m.Command.(Op)
			DPrintf("[apply]server %d, index %d, key %s, value %s", kv.me, index, applOp.Key, applOp.Value)
			kv.mu.Lock()
			pendingOp, exists := kv.pendingOps[index]
			if exists && (applOp.ClientId != pendingOp.clientId || applOp.ClientSeq != pendingOp.seq) {
				kv.onTermChange()
			}
			pendingOp, exists = kv.pendingOps[index]
			switch applOp.Type {
			case GetOp:
				seq, ok := kv.maxCommitClientSeq[applOp.ClientId]
				if !ok || seq < applOp.ClientSeq {
					kv.maxCommitClientSeq[applOp.ClientId] = applOp.ClientSeq
				}
				value, ok := kv.data[applOp.Key]
				if exists {
					if !ok {
						pendingOp.ch <- ApplyResponse{"", ErrNoKey}
					} else {
						pendingOp.ch <- ApplyResponse{value, OK}
					}
				}
			case AppendOp:
				seq, ok := kv.maxCommitClientSeq[applOp.ClientId]
				if !ok || seq < applOp.ClientSeq {
					kv.data[applOp.Key] = kv.data[applOp.Key] + applOp.Value
					kv.maxCommitClientSeq[applOp.ClientId] = applOp.ClientSeq
				}
				if exists {
					pendingOp.ch <- ApplyResponse{"", OK}
				}
			case PutOp:
				seq, ok := kv.maxCommitClientSeq[applOp.ClientId]
				if !ok || seq < applOp.ClientSeq {
					kv.data[applOp.Key] = applOp.Value
					kv.maxCommitClientSeq[applOp.ClientId] = applOp.ClientSeq
				}
				if exists {
					pendingOp.ch <- ApplyResponse{"", OK}
				}
			}
			kv.mu.Unlock()
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
	kv.pendingOps = make(map[int]PendingOp)
	kv.maxCommitClientSeq = make(map[int64]int)
	go kv.apply()
	return kv
}
