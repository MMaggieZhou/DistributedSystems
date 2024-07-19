package shardctrler

import (
	"6.5840/raft"
	"log"
	"slices"
	"sort"
	"time"
)
import "6.5840/labrpc"
import "sync"
import "6.5840/labgob"

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type OpType int

const (
	Join OpType = iota
	Leave
	Move
	Query
)

type Op struct {
	// Your data here.
	Type OpType

	Servers map[int][]string // for Join

	GIDs []int // for Leave

	Shard int // for Move
	GID   int // for Move

	Num int // for Query

	ClientId  int64
	ClientSeq int
}

type ApplyResponse struct {
	wrongLeader bool
	err         Err
	config      Config // for Query
}

type PendingOp struct {
	clientId int64
	seq      int
	ch       chan ApplyResponse
}

type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.
	maxCommitClientSeq map[int64]int
	pendingOps         map[int]PendingOp

	configs []Config // indexed by config num
}

func (sc *ShardCtrler) handlerUtil(op Op, method string) ApplyResponse {
	response := ApplyResponse{}
	index, term, isLeader := sc.rf.Start(op)
	DPrintf("[starts]sc %d takes %s op ,seq %d, index %d, term %d, isleader %t", sc.me, method, op.ClientSeq, index, term, isLeader)
	if !isLeader {
		response.wrongLeader = true
		return response
	}
	ch := make(chan ApplyResponse)
	sc.mu.Lock()
	sc.pendingOps[index] = PendingOp{op.ClientId, op.ClientSeq, ch}
	sc.mu.Unlock()
	select {
	case val := <-ch:
		response = val
	case <-time.After(time.Second * 1):
		response.wrongLeader = true
	}
	sc.mu.Lock()
	delete(sc.pendingOps, index)
	sc.mu.Unlock()
	DPrintf("[returns] sc %d takes %s op, seq:%d", sc.me, method, op.ClientSeq)
	return response
}

// invalid inputs aren't handled:
//
//	gid already exist
//	duplicate gids
func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	op := Op{}
	op.ClientId = args.ClientId
	op.ClientSeq = args.ClientSeq
	op.Type = Join
	op.Servers = args.Servers
	response := sc.handlerUtil(op, "Join")
	reply.WrongLeader = response.wrongLeader
	reply.Err = response.err
}

// invalid inputs aren't handled:
//
//	duplicate gids
//	gid isn't added
func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	op := Op{}
	op.ClientId = args.ClientId
	op.ClientSeq = args.ClientSeq
	op.Type = Leave
	op.GIDs = args.GIDs
	response := sc.handlerUtil(op, "Leave")
	reply.WrongLeader = response.wrongLeader
	reply.Err = response.err
}

// invalid inputs aren't handled:
//
//	gid is not added yet
//	invalid shard
func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	op := Op{}
	op.ClientId = args.ClientId
	op.ClientSeq = args.ClientSeq
	op.Type = Move
	op.Shard = args.Shard
	op.GID = args.GID
	response := sc.handlerUtil(op, "Move")
	reply.WrongLeader = response.wrongLeader
	reply.Err = response.err
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	op := Op{}
	op.ClientId = args.ClientId
	op.ClientSeq = args.ClientSeq
	op.Type = Query
	op.Num = args.Num
	response := sc.handlerUtil(op, "Query")
	reply.WrongLeader = response.wrongLeader
	reply.Err = response.err
	reply.Config = response.config
}

// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (sc *ShardCtrler) Kill() {
	sc.rf.Kill()
	// Your code here, if desired.
}

func toGroupAssignment(config Config) map[int][]int { // gid -> sorted array of shards
	ret := make(map[int][]int)
	for shard, gid := range config.Shards {
		shards, ok := ret[gid]
		if !ok {
			ret[gid] = []int{shard}
		} else {
			ret[gid] = append(shards, shard)
		}
	}
	// if numgroups > numshards,
	// some groups don't show up in config.Shards
	for gid, _ := range config.Groups {
		_, ok := ret[gid]
		if !ok {
			ret[gid] = []int{}
		}
	}
	return ret
}

// move from group with max number of shards
// to group with min number of shards;
// move one shard at a move;
// until the max difference between groups is 1
func addNBalance(groups map[int][]int, newgids []int) [NShards]int {
	var ret [NShards]int

	groupsArr := make([][2]int, 0) // array of [numshards, gid]
	for gid, shards := range groups {
		groupsArr = append(groupsArr, [2]int{len(shards), gid})
	}
	sort.Slice(groupsArr, func(i, j int) bool {
		return groupsArr[i][0] > groupsArr[j][0] || (groupsArr[i][0] == groupsArr[j][0] && groupsArr[i][1] > groupsArr[j][1])
	})

	maxId := 0 // as in groupsArr
	// shard assignment for new group
	// for i, gid ; newgids[i], shardassignment = groups[i]
	for _, gid := range newgids {
		groups[gid] = []int{}
	}
	minId := 0 // as in newgids
	for groupsArr[maxId][0]-len(groups[newgids[minId]]) > 1 {
		// move first shard from maxid to minid
		groups[newgids[minId]] = append(groups[newgids[minId]], groups[groupsArr[maxId][1]][0])
		groups[groupsArr[maxId][1]] = groups[groupsArr[maxId][1]][1:]
		groupsArr[maxId][0]--
		maxId = (maxId + 1) % len(groupsArr)
		minId = (minId + 1) % len(newgids)
	}
	for gid, shards := range groups {
		for _, shard := range shards {
			ret[shard] = gid
		}
	}
	return ret
}

func (sc *ShardCtrler) executeJoin(op Op) {
	DPrintf("[start join]sc %d, old config: %v", sc.me, sc.configs[len(sc.configs)-1])
	DPrintf("[join]sc %d, op: %v", sc.me, op)
	config := sc.configs[len(sc.configs)-1]
	newConfig := Config{}
	newConfig.Num = len(sc.configs)
	newConfig.Groups = make(map[int][]string)
	for gid, servers := range config.Groups {
		serversCp := make([]string, len(servers))
		copy(serversCp, servers)
		newConfig.Groups[gid] = serversCp
	}
	newgids := make([]int, 0)
	for gid, servers := range op.Servers {
		newConfig.Groups[gid] = servers
		newgids = append(newgids, gid)
	}
	// to make the algorithm deterministic and
	// executing op can produce same result in replica
	slices.Sort(newgids)
	// empty group
	if len(newgids) == len(newConfig.Groups) {
		nextIndex := 0
		for i := range NShards {
			newConfig.Shards[i] = newgids[nextIndex]
			nextIndex = (nextIndex + 1) % len(newgids)
		}
	} else {
		groups := toGroupAssignment(config)

		newConfig.Shards = addNBalance(groups, newgids)
	}
	sc.configs = append(sc.configs, newConfig)
	DPrintf("[end join]sc %d, new config: %v", sc.me, sc.configs[len(sc.configs)-1])
}

// move shards for gids into groups with
// min number of shards;
// one shard at a time
func removeNBalance(groups map[int][]int, gids []int) [NShards]int {
	DPrintf("removeNBlance groups %v, gids %v", groups, gids)
	var ret [NShards]int
	// for i, gid = gids[i], shards = shardsToMove[i]
	shardsToMove := make([][]int, len(gids))
	for i, gid := range gids {
		shardsToMove[i] = groups[gid]
		delete(groups, gid)
	}
	// sort remaining groups by numshards, in descending order
	groupsArr := make([][2]int, 0)
	for gid, shards := range groups {
		groupsArr = append(groupsArr, [2]int{len(shards), gid})
	}
	sort.Slice(groupsArr, func(i, j int) bool {
		return groupsArr[i][0] < groupsArr[j][0] || (groupsArr[i][0] == groupsArr[j][0] && groupsArr[i][1] < groupsArr[j][1])
	})
	DPrintf("shardsTOMove %v, groupsArr %v", shardsToMove, groupsArr)
	minId := 0
	for _, shards := range shardsToMove {
		for _, shard := range shards {
			gid := groupsArr[minId][1]
			groups[gid] = append(groups[gid], shard)
			minId = (minId + 1) % len(groupsArr)
		}
	}
	for gid, shards := range groups {
		for _, shard := range shards {
			ret[shard] = gid
		}
	}
	return ret
}

func (sc *ShardCtrler) executeLeave(op Op) {
	DPrintf("[start leave]sc %d, old config: %v", sc.me, sc.configs[len(sc.configs)-1])
	DPrintf("[leave] sc %d, op: %v", sc.me, op)
	config := sc.configs[len(sc.configs)-1]
	newConfig := Config{}
	newConfig.Num = len(sc.configs)
	newConfig.Groups = make(map[int][]string)
	for gid, servers := range config.Groups {
		serversCp := make([]string, len(servers))
		copy(serversCp, servers)
		newConfig.Groups[gid] = serversCp
	}
	for _, gid := range op.GIDs {
		delete(newConfig.Groups, gid)
	}
	if len(newConfig.Groups) == 0 {
		for i := range NShards {
			newConfig.Shards[i] = 0
		}
	} else {
		groups := toGroupAssignment(config)
		newConfig.Shards = removeNBalance(groups, op.GIDs)
	}
	sc.configs = append(sc.configs, newConfig)
	DPrintf("[end leave]sc %d, new config: %v", sc.me, sc.configs[len(sc.configs)-1])
}

func (sc *ShardCtrler) executeMove(op Op) {
	config := sc.configs[len(sc.configs)-1]
	newConfig := Config{}
	newConfig.Num = len(sc.configs)
	newConfig.Shards = [NShards]int{}
	for i, gid := range config.Shards {
		newConfig.Shards[i] = gid
	}
	newConfig.Shards[op.Shard] = op.GID
	newConfig.Groups = make(map[int][]string)
	for gid, servers := range config.Groups {
		serversCp := make([]string, len(servers))
		copy(serversCp, servers)
		newConfig.Groups[gid] = serversCp
	}
	sc.configs = append(sc.configs, newConfig)
}

func (sc *ShardCtrler) apply() {
	for m := range sc.applyCh {
		if m.CommandValid {
			index := m.CommandIndex
			applOp := m.Command.(Op)
			DPrintf("[apply]sc %d, index %d", sc.me, index)
			sc.mu.Lock()
			pendingOp, exists := sc.pendingOps[index]
			// a different log gets commited at same index, indicating that
			// the leader has lost its term
			if exists && (applOp.ClientId != pendingOp.clientId || applOp.ClientSeq != pendingOp.seq) {
				response := ApplyResponse{}
				response.wrongLeader = true
				pendingOp.ch <- response
				exists = false
			}
			execute := false
			seq, ok := sc.maxCommitClientSeq[applOp.ClientId]
			if !ok || seq < applOp.ClientSeq {
				execute = true
				sc.maxCommitClientSeq[applOp.ClientId] = applOp.ClientSeq
			}
			switch applOp.Type {
			case Join:
				if execute {
					sc.executeJoin(applOp)
				}
				if exists {
					pendingOp.ch <- ApplyResponse{false, "", Config{}}
				}
			case Leave:
				if execute {
					sc.executeLeave(applOp)
				}
				if exists {
					pendingOp.ch <- ApplyResponse{false, "", Config{}}
				}
			case Move:
				if execute {
					sc.executeMove(applOp)
				}
				if exists {
					pendingOp.ch <- ApplyResponse{false, "", Config{}}
				}
			case Query:
				if exists {
					if applOp.Num == -1 || applOp.Num > len(sc.configs)-1 {
						pendingOp.ch <- ApplyResponse{false, "", sc.configs[len(sc.configs)-1]}
					} else {
						pendingOp.ch <- ApplyResponse{false, "", sc.configs[applOp.Num]}
					}
				}
			}
			sc.mu.Unlock()
		}
	}
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}
	for i := range NShards {
		sc.configs[0].Shards[i] = 0
	}
	sc.configs[0].Num = 0

	labgob.Register(Op{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	sc.pendingOps = make(map[int]PendingOp)
	sc.maxCommitClientSeq = make(map[int64]int)

	go sc.apply()
	return sc
}
