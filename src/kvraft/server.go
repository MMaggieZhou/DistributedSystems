package kvraft

import (
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
	"bytes"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

// failure scenarios:
// leader partition:
// 		function could hang forever if a leader is partitioned
//		so the log is never commited.
// 		need to add timeout to handler method while waiting for ch
//		to return
//		caveat: after leader becomes follower and receive commited log,
//		it may not match that in kv.pendingOps so need to check it explicitly
//
// unreliable network:
// 		kvservers could receive duplicate msg
//		dedup before apply msg, by kv.maxCommitClientSeq
// kv server crash:
// 		should be restored by raft, which upon restart,
// 		will reach consensus and apply logs from beginning
//
// Snapshot:
// Data structure: kv.data, kv.maxCommitClientSeq, kv.lastSnapshotIndex
// Creating Snapshot: kv server checks persister for raft log size, and creates snapshot
// 		if state size is too large (see kv.apply())
// Install Snapshot: load the snapshot, and send error to all pendingOps with index prior
// 		to the snapshot, since there' no way to verify it's duplicate or not
// Restart: read snapshot from persister

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

	maxraftstate       int // snapshot if log grows this big
	data               map[string]string
	maxCommitClientSeq map[int64]int
	lastLogIndex       int
	lastSnapshotIndex  int
	persister          *raft.Persister

	pendingOps map[int]PendingOp // log index -> PendingOp
}

func (kv *KVServer) takeSnapshot() {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	if e.Encode(kv.data) != nil || e.Encode(kv.maxCommitClientSeq) != nil || e.Encode(kv.lastLogIndex) != nil {
		DPrintf("[erro takesnapshot] kv %d", kv.me)
		return
	}
	DPrintf("[takesnapshot] kv %d, size %d, lastindex %d", kv.me, w.Len(), kv.lastSnapshotIndex)
	kv.rf.Snapshot(kv.lastLogIndex, w.Bytes())
	kv.lastSnapshotIndex = kv.lastLogIndex
	DPrintf("[done takesnapshot] kv %d, lastsnapshot %d", kv.me, kv.lastSnapshotIndex)
}

func (kv *KVServer) readSnapshot(data []byte) {
	if data == nil || len(data) < 1 {
		return
	}
	DPrintf("[start readsnapshot] kv %d, log index %d, snapshot index %d, datasize %d", kv.me, kv.lastLogIndex, kv.lastSnapshotIndex, len(data))
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var datamap map[string]string
	var maxCommitClientSeq map[int64]int
	var lastSnapshotIndex int
	// needs to be reference
	if d.Decode(&datamap) != nil || d.Decode(&maxCommitClientSeq) != nil || d.Decode(&lastSnapshotIndex) != nil {
		DPrintf("[erro readsnapshot] kv %d", kv.me)
		return
	}

	kv.data = datamap
	kv.maxCommitClientSeq = maxCommitClientSeq
	kv.lastSnapshotIndex = lastSnapshotIndex
	DPrintf("[done readsnapshot] kv %d, snapshot index %d", kv.me, kv.lastSnapshotIndex)
}

func (kv *KVServer) handlerUtil(op Op, method string) ApplyResponse {
	response := ApplyResponse{}
	index, term, isLeader := kv.rf.Start(op)
	DPrintf("[starts]kv store %d takes %s op %s:%s,seq %d, index %d, term %d, isleader %t", kv.me, method, op.Key, op.Value, op.ClientSeq, index, term, isLeader)
	if !isLeader {
		response.error = ErrWrongLeader
		return response
	}
	ch := make(chan ApplyResponse)
	kv.mu.Lock()
	kv.pendingOps[index] = PendingOp{op.ClientId, op.ClientSeq, ch}
	kv.mu.Unlock()
	select {
	case val := <-ch:
		response = val
	case <-time.After(time.Second * 1):
		response.error = ErrWrongLeader
	}
	kv.mu.Lock()
	delete(kv.pendingOps, index)
	kv.mu.Unlock()
	DPrintf("[returns] kv store %d takes %s op %s:%s , seq:%d, returns %s, val %s", kv.me, method, op.Key, op.Value, op.ClientSeq, response.error, response.value)
	return response
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	op := Op{GetOp, args.Key, "", args.ClientId, args.ClientSeq}
	response := kv.handlerUtil(op, "get")
	reply.Err = response.error
	reply.Value = response.value
	return
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	op := Op{PutOp, args.Key, args.Value, args.ClientId, args.ClientSeq}
	response := kv.handlerUtil(op, "put")
	reply.Err = response.error
	return
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	op := Op{AppendOp, args.Key, args.Value, args.ClientId, args.ClientSeq}
	response := kv.handlerUtil(op, "append")
	reply.Err = response.error
	return
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
			applOp := m.Command.(Op)
			DPrintf("[apply]server %d, index %d, key %s, value %s", kv.me, index, applOp.Key, applOp.Value)
			kv.mu.Lock()
			pendingOp, exists := kv.pendingOps[index]
			if exists && (applOp.ClientId != pendingOp.clientId || applOp.ClientSeq != pendingOp.seq) {
				pendingOp.ch <- ApplyResponse{"", ErrWrongLeader}
				exists = false
			}
			execute := false
			seq, ok := kv.maxCommitClientSeq[applOp.ClientId]
			if !ok || seq < applOp.ClientSeq {
				execute = true
				kv.maxCommitClientSeq[applOp.ClientId] = applOp.ClientSeq
			}
			switch applOp.Type {
			case GetOp:
				value, ok := kv.data[applOp.Key]
				if exists {
					if !ok {
						pendingOp.ch <- ApplyResponse{"", ErrNoKey}
					} else {
						pendingOp.ch <- ApplyResponse{value, OK}
					}
				}
			case AppendOp:
				if execute {
					kv.data[applOp.Key] = kv.data[applOp.Key] + applOp.Value
				}
				if exists {
					pendingOp.ch <- ApplyResponse{"", OK}
				}
			case PutOp:
				if execute {
					kv.data[applOp.Key] = applOp.Value
				}
				if exists {
					pendingOp.ch <- ApplyResponse{"", OK}
				}
			}
			kv.lastLogIndex = index
			raftStateSize := kv.persister.RaftStateSize()
			DPrintf("[apply] kv %d, maxsize %d, raftStateSize %d", kv.me, kv.maxraftstate, raftStateSize)
			// in case server have too many applymsg clogging up, don't
			// let taking snapshot further slow it down by only do it if
			// after a number of new logs have been applied.
			if kv.maxraftstate > 0 && raftStateSize > kv.maxraftstate && kv.lastLogIndex-kv.lastSnapshotIndex > 20 {
				DPrintf("[start takesnapshot] kv %d, max size %d, raft size %d, lastlog %d, lastsnapshot %d", kv.me, kv.maxraftstate, raftStateSize, kv.lastLogIndex, kv.lastSnapshotIndex)
				kv.takeSnapshot()
			}
			kv.mu.Unlock()
		} else {
			DPrintf("[apply snapshot] kv %d, snapshot index %d", kv.me, m.SnapshotIndex)
			kv.mu.Lock()
			kv.readSnapshot(m.Snapshot)
			kv.lastLogIndex = m.SnapshotIndex
			// no way to verify if this pendingOps is duplicate or not,
			// since log is dumped.
			// hence fail the op to be safe
			for i, pendingOp := range kv.pendingOps {
				if i <= kv.lastLogIndex {
					pendingOp.ch <- ApplyResponse{"", ErrWrongLeader}
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

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.data = make(map[string]string)
	kv.pendingOps = make(map[int]PendingOp)
	kv.maxCommitClientSeq = make(map[int64]int)
	kv.persister = persister
	kv.readSnapshot(persister.ReadSnapshot())
	kv.lastLogIndex = kv.lastSnapshotIndex
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	go kv.apply()
	return kv
}
