package shardkv

import (
	"6.5840/labrpc"
	"6.5840/shardctrler"
	"bytes"
	"log"
	"time"
)
import "6.5840/raft"
import "sync"
import "6.5840/labgob"

type OpType int

const (
	PutOp OpType = iota + 1
	AppendOp
	GetOp
	ConfigOp
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Type      OpType
	Key       string             // for put, append, get op
	Value     string             // for put, append, get op
	Config    shardctrler.Config // for ConfigOp
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

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	ctrlers      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	data               map[string]string
	configs            []shardctrler.Config
	maxCommitClientSeq map[int64]int
	lastLogIndex       int
	lastSnapshotIndex  int
	persister          *raft.Persister

	mck *shardctrler.Clerk
	id  int64 // for updating config
	seq int   // for updating config

	pendingOps map[int]PendingOp // log index -> PendingOp

}

func (kv *ShardKV) handlerUtil(op Op, method string) ApplyResponse {
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

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	op := Op{GetOp, args.Key, "", shardctrler.Config{}, args.ClientId, args.ClientSeq}
	response := kv.handlerUtil(op, "get")
	reply.Err = response.error
	reply.Value = response.value
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	op := Op{}
	if args.Op == "Put" {
		op.Type = PutOp
	} else {
		op.Type = AppendOp
	}
	op.Key = args.Key
	op.Value = args.Value
	op.ClientId = args.ClientId
	op.ClientSeq = args.ClientSeq
	response := kv.handlerUtil(op, args.Op)
	reply.Err = response.error
}

// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (kv *ShardKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
}

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

func (kv *ShardKV) takeSnapshot() {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	if e.Encode(kv.data) != nil || e.Encode(kv.configs) != nil || e.Encode(kv.maxCommitClientSeq) != nil || e.Encode(kv.lastLogIndex) != nil {
		DPrintf("[erro takesnapshot] kv %d", kv.me)
		return
	}
	DPrintf("[takesnapshot] kv %d, size %d, lastindex %d", kv.me, w.Len(), kv.lastSnapshotIndex)
	kv.rf.Snapshot(kv.lastLogIndex, w.Bytes())
	kv.lastSnapshotIndex = kv.lastLogIndex
	DPrintf("[done takesnapshot] kv %d, lastsnapshot %d", kv.me, kv.lastSnapshotIndex)
}

func (kv *ShardKV) readSnapshot(data []byte) {
	if data == nil || len(data) < 1 {
		return
	}
	DPrintf("[start readsnapshot] kv %d, log index %d, snapshot index %d, datasize %d", kv.me, kv.lastLogIndex, kv.lastSnapshotIndex, len(data))
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var datamap map[string]string
	var configs []shardctrler.Config
	var maxCommitClientSeq map[int64]int
	var lastSnapshotIndex int
	// needs to be reference
	if d.Decode(&datamap) != nil || d.Decode(&configs) != nil || d.Decode(&maxCommitClientSeq) != nil || d.Decode(&lastSnapshotIndex) != nil {
		DPrintf("[erro readsnapshot] kv %d", kv.me)
		return
	}

	kv.data = datamap
	kv.maxCommitClientSeq = maxCommitClientSeq
	kv.lastSnapshotIndex = lastSnapshotIndex
	DPrintf("[done readsnapshot] kv %d, snapshot index %d", kv.me, kv.lastSnapshotIndex)
}

func (kv *ShardKV) watchConfig() {
	// fetch ctr client
	// get value
	// if there's new change, write into log
	// update client seq
	for { // check kv.killed() == false ?
		config := kv.mck.Query(-1)
		kv.mu.Lock()
		if len(kv.configs) > 0 && kv.configs[len(kv.configs)-1].Num >= config.Num {
			kv.mu.Unlock()
			continue
		} else {
			kv.mu.Unlock()
		}

		op := Op{}
		op.Type = ConfigOp
		op.Config = config
		op.ClientId = kv.id
		op.ClientSeq = kv.seq
		kv.seq++
		kv.handlerUtil(op, "ConfigUpdate")
		time.Sleep(time.Duration(50) * time.Millisecond)
	}
}

func (kv *ShardKV) apply() {
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
			wrongShard := false
			if applOp.Type != ConfigOp {
				key := applOp.Key
				shard := key2shard(key)
				if len(kv.configs) == 0 || kv.configs[len(kv.configs)-1].Shards[shard] != kv.gid {
					wrongShard = true
					if exists {
						pendingOp.ch <- ApplyResponse{"", ErrWrongGroup}
						exists = false
					}
				}
				if !wrongShard {
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
				}
			} else {
				if execute {
					if len(kv.configs) == 0 || kv.configs[len(kv.configs)-1].Num < applOp.Config.Num {
						kv.configs = append(kv.configs, applOp.Config)
					}
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
// gid is this group's GID, for interacting with the shardctrler.
//
// pass ctrlers[] to shardctrler.MakeClerk() so you can send
// RPCs to the shardctrler.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use ctrlers[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers
	// Your initialization code here.

	// Use something like this to talk to the shardctrler:
	// kv.mck = shardctrler.MakeClerk(kv.ctrlers)

	kv.applyCh = make(chan raft.ApplyMsg)

	kv.data = make(map[string]string)
	kv.configs = make([]shardctrler.Config, 0)
	kv.pendingOps = make(map[int]PendingOp)
	kv.maxCommitClientSeq = make(map[int64]int)
	kv.persister = persister
	kv.readSnapshot(persister.ReadSnapshot())
	kv.lastLogIndex = kv.lastSnapshotIndex
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	kv.mck = shardctrler.MakeClerk(kv.ctrlers)
	kv.seq = 0
	kv.id = nrand()

	go kv.apply()
	go kv.watchConfig()
	return kv
}
