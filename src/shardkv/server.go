package shardkv

import (
	"6.5840/labrpc"
	"6.5840/shardctrler"
	"bytes"
	"fmt"
	"log"
	"time"
)
import "6.5840/raft"
import "sync"
import "6.5840/labgob"

const Debug = false

func (kv *ShardKV) DPrintf(method string, format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(fmt.Sprintf("["+method+"]"+"gid %d kv %d, configNum %d, transition %t, serve %v, shardToSend %v, shardToReceive %v \n", kv.gid, kv.me, kv.configNum, kv.shardTransition, kv.shardsToServe, kv.shardsToSend, kv.shardsToReceive)+format, a...)
	}
	return
}

func (kv *ShardKV) DPrintClientRequestStart(method string, op *Op, index int, term int) (n int, err error) {
	shard := key2shard(op.Key)
	kv.DPrintf("start "+method, "key %s:%s, shard %d, clientid %d,seq %d, index %d, term %d", op.Key, op.Value, shard, op.ClientId, op.ClientSeq, index, term)
	return
}
func (kv *ShardKV) DPrintClientRequestEnd(method string, op *Op, index int, term int, response *ApplyResponse) (n int, err error) {
	shard := key2shard(op.Key)
	kv.DPrintf("return "+method, "key %s:%s, shard %d, clientid %d,seq %d, index %d, term %d, value %s, error %s", op.Key, op.Value, shard, op.ClientId, op.ClientSeq, index, term, response.value, response.error)
	return
}

func (kv *ShardKV) DPrintShardOperationStart(method string, op *Op, index int, term int) (n int, err error) {
	if op.Type == ConfigOp {
		kv.DPrintf("start "+method, "config %v", op.Config)
	} else if op.Type == InstallShards {
		shards := make([]int, 0)
		for shard, _ := range op.Shards {
			shards = append(shards, shard)
		}
		kv.DPrintf("start "+method, "configNum %d, shards: %v", op.ConfigNum, shards)
	} else {
		// delete
		kv.DPrintf("start "+method, "configNum %d, gid %d, shards: %v", op.ConfigNum, op.Gid, op.ShardsToDelete)
	}
	return
}
func (kv *ShardKV) DPrintShardOperationEnd(method string, op *Op, index int, term int, response *ApplyResponse) (n int, err error) {
	if op.Type == ConfigOp {
		kv.DPrintf("end "+method, "config %v, error: %s", op.Config, response.error)
	} else if op.Type == InstallShards {
		shards := make([]int, 0)
		for shard, _ := range op.Shards {
			shards = append(shards, shard)
		}
		kv.DPrintf("end "+method, "configNum %d, shards: %v, error: %s", op.ConfigNum, shards, response.error)
	} else {
		// delete
		kv.DPrintf("end "+method, "configNum %d, gid %d, shards: %v, error: %s", op.ConfigNum, op.Gid, op.ShardsToDelete, response.error)
	}
	return
}

type OpType int

const (
	PutOp OpType = iota + 1
	AppendOp
	GetOp
	ConfigOp
	InstallShards
	DeleteShards
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Type               OpType
	Key                string                    // for put, append, get op
	Value              string                    // for put, append, get op
	ClientId           int64                     // for put, append, get op
	ClientSeq          int                       // for put, append, get op
	Config             shardctrler.Config        // for ConfigOp
	ConfigNum          int                       // for InstallShards and DeleteShards
	Shards             map[int]map[string]string // for InstallShards
	MaxCommitClientSeq map[int64]int             // for InstallShards
	Gid                int                       // for DeleteShards
	ShardsToDelete     map[int]bool              // for DeleteShards

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

type InstallShardsArgs struct {
	ConfigNum          int
	Shards             map[int]map[string]string
	MaxCommitClientSeq map[int64]int
}

type InstallShardsReply struct {
	Err Err
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
	configNum          int
	shardsToServe      map[int]bool
	shardsToSend       map[int][]int    // gid->[]shardnum
	groups             map[int][]string // gid->servers for sending shard
	shardsToReceive    map[int]bool
	maxCommitClientSeq map[int64]int
	lastLogIndex       int
	lastSnapshotIndex  int
	persister          *raft.Persister

	mck *shardctrler.Clerk

	pendingOps      map[int]PendingOp // log index -> PendingOp
	shardTransition bool
}

func (kv *ShardKV) handlerUtil(op Op, method string) ApplyResponse {
	response := ApplyResponse{}
	index, term, isLeader := kv.rf.Start(op)
	if !isLeader {
		response.error = ErrWrongLeader
		return response
	}
	ch := make(chan ApplyResponse)
	kv.mu.Lock()
	kv.pendingOps[index] = PendingOp{op.ClientId, op.ClientSeq, ch}
	clientRequest := false
	if op.Type == GetOp || op.Type == PutOp || op.Type == AppendOp {
		clientRequest = true
		kv.DPrintClientRequestStart(method, &op, index, term)
	} else {
		kv.DPrintShardOperationStart(method, &op, index, term)
	}
	kv.mu.Unlock()
	select {
	case val := <-ch:
		response = val
	case <-time.After(time.Second * 1):
		response.error = ErrWrongLeader
	}
	kv.mu.Lock()
	delete(kv.pendingOps, index)
	if clientRequest {
		kv.DPrintClientRequestEnd(method, &op, index, term, &response)
	} else {
		kv.DPrintShardOperationEnd(method, &op, index, term, &response)
	}
	kv.mu.Unlock()
	return response
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	op := Op{}
	op.Type = GetOp
	op.Key = args.Key
	op.ClientId = args.ClientId
	op.ClientSeq = args.ClientSeq
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

func (kv *ShardKV) InstallShards(args *InstallShardsArgs, reply *InstallShardsReply) {
	op := Op{}
	op.Type = InstallShards
	op.ConfigNum = args.ConfigNum
	op.Shards = args.Shards
	op.MaxCommitClientSeq = args.MaxCommitClientSeq
	response := kv.handlerUtil(op, "InstallShards")
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

func (kv *ShardKV) takeSnapshot() {
	if kv.maxraftstate < 0 {
		return
	}
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	if e.Encode(kv.data) != nil || e.Encode(kv.configNum) != nil || e.Encode(kv.shardsToServe) != nil || e.Encode(kv.shardsToSend) != nil || e.Encode(kv.groups) != nil || e.Encode(kv.shardsToReceive) != nil || e.Encode(kv.maxCommitClientSeq) != nil || e.Encode(kv.lastLogIndex) != nil {
		kv.DPrintf("erro takesnapshot", "")
		return
	}
	kv.DPrintf("takesnapshot", "size %d, lastindex %d", w.Len(), kv.lastSnapshotIndex)
	kv.rf.Snapshot(kv.lastLogIndex, w.Bytes())
	kv.lastSnapshotIndex = kv.lastLogIndex
	kv.DPrintf("done takesnapshot", "lastsnapshot %d", kv.lastSnapshotIndex)
}

func (kv *ShardKV) updateTransitionState() {
	if len(kv.shardsToSend) > 0 || len(kv.shardsToReceive) > 0 {
		kv.shardTransition = true
	} else {
		kv.shardTransition = false
	}
}

// not thread safe
func (kv *ShardKV) readSnapshot(data []byte) {
	if data == nil || len(data) < 1 {
		return
	}
	kv.DPrintf("start readsnapshot", "curr log index %d, curr snapshot index %d, datasize %d", kv.lastLogIndex, kv.lastSnapshotIndex, len(data))
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var datamap map[string]string
	var configNum int
	var shardsToServe map[int]bool
	var shardsToSend map[int][]int
	var groups map[int][]string
	var shardsToReceive map[int]bool
	var maxCommitClientSeq map[int64]int
	var lastSnapshotIndex int
	// needs to be reference
	if d.Decode(&datamap) != nil || d.Decode(&configNum) != nil || d.Decode(&shardsToServe) != nil || d.Decode(&shardsToSend) != nil || d.Decode(&groups) != nil || d.Decode(&shardsToReceive) != nil || d.Decode(&maxCommitClientSeq) != nil || d.Decode(&lastSnapshotIndex) != nil {
		kv.DPrintf("erro readsnapshot", "")
		return
	}

	kv.data = datamap
	kv.configNum = configNum
	kv.shardsToServe = shardsToServe
	kv.shardsToSend = shardsToSend
	kv.groups = groups
	kv.shardsToReceive = shardsToReceive
	kv.maxCommitClientSeq = maxCommitClientSeq
	kv.lastSnapshotIndex = lastSnapshotIndex
	kv.lastLogIndex = kv.lastSnapshotIndex
	kv.updateTransitionState()
	kv.DPrintf("done readsnapshot", "new log index %d, new snapshot index %d", kv.lastLogIndex, kv.lastSnapshotIndex)

}

func (kv *ShardKV) sendShards(gid int, wg *sync.WaitGroup) {
	defer wg.Done()
	kv.mu.Lock()

	args := InstallShardsArgs{}
	args.ConfigNum = kv.configNum
	args.Shards = make(map[int]map[string]string)
	args.MaxCommitClientSeq = kv.maxCommitClientSeq
	shardsToSend := make(map[int]bool)
	shards := make([]int, 0)
	for _, shard := range kv.shardsToSend[gid] {
		shardsToSend[shard] = true
		args.Shards[shard] = make(map[string]string)
		shards = append(shards, shard)
	}
	for k, v := range kv.data {
		shard := key2shard(k)
		if shardsToSend[shard] {
			args.Shards[shard][k] = v
		}
	}
	kv.DPrintf("sendShards", "to gid %d, configNum %d, shards %v", gid, args.ConfigNum, shards)
	kv.mu.Unlock()
	success := false
	for !success {
		servers := kv.groups[gid]
		for si := 0; si < len(servers); si++ {
			srv := kv.make_end(servers[si])
			var reply InstallShardsReply
			ok := srv.Call("ShardKV.InstallShards", &args, &reply)
			if ok && reply.Err == OK {
				success = true
				break
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	kv.mu.Lock()
	kv.DPrintf("success sendShards", "to gid %d, configNum %d, shards %v", gid, args.ConfigNum, shards)
	op := Op{}
	op.Type = DeleteShards
	op.ConfigNum = args.ConfigNum
	op.ShardsToDelete = shardsToSend
	op.Gid = gid
	kv.mu.Unlock()
	kv.handlerUtil(op, "DeleteShards")
}

func (kv *ShardKV) migrateShards() {
	for {
		_, isLeader := kv.rf.GetState()
		kv.mu.Lock()
		if !isLeader || len(kv.shardsToSend) == 0 {
			kv.mu.Unlock()
		} else {
			gids := make([]int, 0)
			for gid, _ := range kv.shardsToSend {
				gids = append(gids, gid)
			}
			kv.mu.Unlock()
			wg := new(sync.WaitGroup)
			wg.Add(len(gids))
			for _, gid := range gids {
				kv.sendShards(gid, wg)
			}
			wg.Wait()
		}

		time.Sleep(time.Duration(100) * time.Millisecond)
	}
}

func (kv *ShardKV) watchConfig() {
	// fetch ctr client
	// get value
	// if there's new change, write into log
	// update client seq
	for { // check kv.killed() == false ?
		_, leader := kv.rf.GetState()
		if leader {
			// find the config number to query
			kv.mu.Lock()
			if !kv.shardTransition {
				kv.mu.Unlock()
				config := kv.mck.Query(-1)
				kv.mu.Lock()
				if config.Num > kv.configNum {
					if config.Num-kv.configNum > 1 {
						kv.mu.Unlock()
						config = kv.mck.Query(kv.configNum + 1)
					} else {
						kv.mu.Unlock()
					}
					op := Op{}
					op.Type = ConfigOp
					op.Config = config
					kv.handlerUtil(op, "ConfigUpdate")
				} else {
					kv.mu.Unlock()
				}
			} else {
				kv.mu.Unlock()
			}
		}
		time.Sleep(time.Duration(50) * time.Millisecond)
	}
}

func (kv *ShardKV) executeDataUpdate(index int, applOp *Op) {
	defer func(kv *ShardKV) {
		raftStateSize := kv.persister.RaftStateSize()
		// in case server have too many applymsg clogging up, don't
		// let taking snapshot further slow it down by only do it if
		// after a number of new logs have been applied.
		if kv.maxraftstate > 0 && raftStateSize > kv.maxraftstate && kv.lastLogIndex-kv.lastSnapshotIndex > 20 {
			kv.takeSnapshot()
		}
	}(kv)
	method := ""
	if applOp.Type == GetOp {
		method = "get"
	} else if applOp.Type == PutOp {
		method = "put"
	} else {
		method = "append"
	}
	key := applOp.Key
	shard := key2shard(key)

	kv.DPrintf("apply client "+method, "index %d, key %s, v %s, shard %d, clientid %d, sep %d", index, applOp.Key, applOp.Value, shard, applOp.ClientId, applOp.ClientSeq)
	pendingOp, exists := kv.pendingOps[index]
	if !kv.shardsToServe[shard] || kv.shardsToReceive[shard] {
		if exists {
			pendingOp.ch <- ApplyResponse{"", ErrWrongGroup}
		}
		return
	}

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

	kv.DPrintf("execute client "+method, "index %d, key %s, value %s, execute %t, shard %d", index, applOp.Key, applOp.Value, execute, shard)
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

func (kv *ShardKV) executeConfigUpdate(index int, applOp *Op) {
	if !kv.shardTransition && applOp.Config.Num == kv.configNum+1 {
		kv.DPrintf("execute config update", "index %d, config %v", index, applOp.Config)
		//kv.configs = append(kv.configs, applOp.Config)
		shardsToServe := make(map[int]bool)
		shardsToSend := make(map[int][]int)
		groups := make(map[int][]string)
		shardsToReceive := make(map[int]bool)
		for shard, gid := range applOp.Config.Shards {
			if gid == kv.gid {
				shardsToServe[shard] = true
				if !kv.shardsToServe[shard] {
					shardsToReceive[shard] = true
				}
			} else if kv.shardsToServe[shard] {
				_, ok := shardsToSend[gid]
				if !ok {
					shardsToSend[gid] = []int{shard}
					groups[gid] = applOp.Config.Groups[gid]
				} else {
					shardsToSend[gid] = append(shardsToSend[gid], shard)
				}
			}
		}
		kv.shardsToServe = shardsToServe
		if kv.configNum != 0 {
			kv.shardsToSend = shardsToSend
			kv.groups = groups
			kv.shardsToReceive = shardsToReceive
		}
		kv.configNum = applOp.Config.Num
		kv.updateTransitionState()

		// take snapshot to avoid shard transition on old config when server restart
		kv.takeSnapshot()
		kv.DPrintf("done execute config update", "index %d", index)
	} else {
		kv.DPrintf("ignore config update", "index %d, config %v", index, applOp.Config)
	}
	pendingOp, exists := kv.pendingOps[index]
	if exists {
		pendingOp.ch <- ApplyResponse{"", OK}
	}
}

func (kv *ShardKV) executeInstallShards(index int, applOp *Op) {
	pendingOp, exists := kv.pendingOps[index]
	shards := make([]int, 0)
	for shard, _ := range applOp.Shards {
		shards = append(shards, shard)
	}
	if kv.configNum-1 == applOp.ConfigNum || (kv.configNum == applOp.ConfigNum && !kv.shardTransition) {
		// this operation has been completed already
		kv.DPrintf("ignore InstallShards", "index %d, configNum %d, shards %v", index, applOp.ConfigNum, shards)
		if exists {
			pendingOp.ch <- ApplyResponse{"", OK}
		}
	} else if kv.configNum < applOp.ConfigNum {
		// new config hasn't arrived yet
		kv.DPrintf("error InstallShards, config too old", "index %d, configNum %d, shards %v", index, applOp.ConfigNum, shards)
		if exists {
			pendingOp.ch <- ApplyResponse{"", ErrWrongLeader}
		}
	} else if kv.configNum == applOp.ConfigNum && kv.shardTransition {
		kv.DPrintf("execute ApplyShard", "index %d, configNum %d, shards %v", index, applOp.ConfigNum, shards)
		for shard, data := range applOp.Shards {
			if kv.shardsToReceive[shard] {
				for k, v := range data {
					kv.data[k] = v
				}
			}
			delete(kv.shardsToReceive, shard)
		}
		for clientId, clientSeq := range applOp.MaxCommitClientSeq {
			_, ok := kv.maxCommitClientSeq[clientId]
			if !ok {
				kv.maxCommitClientSeq[clientId] = clientSeq
			} else {
				kv.maxCommitClientSeq[clientId] = max(kv.maxCommitClientSeq[clientId], clientSeq)
			}
		}
		kv.updateTransitionState()
		kv.DPrintf("done execute ApplyShard", "index %d, configNum %d, shards %v", index, applOp.ConfigNum, shards)
		kv.takeSnapshot()
		if exists {
			pendingOp.ch <- ApplyResponse{"", OK}
		}
	} else {
		kv.DPrintf("error ApplyShard, unknown", "index %d, configNum %d, shards %v", index, applOp.ConfigNum, shards)
		// maybe previous ApplyShard has succeeded, but reply is lost; followed immediately
		// by new config update
		if exists {
			pendingOp.ch <- ApplyResponse{"", OK}
		}
	}
}

func (kv *ShardKV) executeDeleteShards(index int, applOp *Op) {
	pendingOp, exists := kv.pendingOps[index]
	_, ok := kv.shardsToSend[applOp.Gid]
	if !kv.shardTransition || kv.configNum != applOp.ConfigNum || !ok {
		// duplicate msg - unreliable network etc
		kv.DPrintf("ignore DeleteShards", "index %d, configNum %d, gid %d, shards %v", index, applOp.ConfigNum, applOp.Gid, applOp.ShardsToDelete)
		if exists {
			pendingOp.ch <- ApplyResponse{"", OK}
		}
	} else {
		kv.DPrintf("execute DeleteShards", "index %d, configNum %d, gid %d, shards %v", index, applOp.ConfigNum, applOp.Gid, applOp.ShardsToDelete)
		keys := make([]string, 0)
		for k, _ := range kv.data {
			shard := key2shard(k)
			if applOp.ShardsToDelete[shard] {
				keys = append(keys, k)
			}
		}
		for _, key := range keys {
			delete(kv.data, key)
		}
		delete(kv.shardsToSend, applOp.Gid)
		kv.updateTransitionState()
		kv.DPrintf("done DeleteShards", "index %d, configNum %d, gid %d, shards %v", index, applOp.ConfigNum, applOp.Gid, applOp.ShardsToDelete)
		kv.takeSnapshot()
		if exists {
			pendingOp.ch <- ApplyResponse{"", OK}
		}
	}
}

func (kv *ShardKV) apply() {
	for m := range kv.applyCh {
		kv.mu.Lock()
		if m.CommandValid {
			index := m.CommandIndex
			kv.lastLogIndex = index
			applOp := m.Command.(Op)
			if applOp.Type == GetOp || applOp.Type == PutOp || applOp.Type == AppendOp {
				kv.executeDataUpdate(index, &applOp)
			} else if applOp.Type == ConfigOp {
				kv.executeConfigUpdate(index, &applOp)
			} else if applOp.Type == InstallShards {
				kv.executeInstallShards(index, &applOp)
			} else {
				kv.executeDeleteShards(index, &applOp)
			}
		} else {
			kv.DPrintf("execute snapshot", " snapshot index %d", m.SnapshotIndex)
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
		}
		kv.mu.Unlock()
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
	kv.configNum = 0
	kv.shardsToServe = make(map[int]bool)
	kv.shardsToSend = make(map[int][]int)
	kv.shardsToReceive = make(map[int]bool)
	kv.pendingOps = make(map[int]PendingOp)
	kv.shardTransition = false
	kv.maxCommitClientSeq = make(map[int64]int)
	kv.persister = persister
	kv.readSnapshot(persister.ReadSnapshot())
	kv.lastLogIndex = kv.lastSnapshotIndex
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	kv.mck = shardctrler.MakeClerk(kv.ctrlers)

	kv.DPrintf("StartServer", "")

	go kv.apply()
	go kv.watchConfig()
	go kv.migrateShards()
	return kv
}
