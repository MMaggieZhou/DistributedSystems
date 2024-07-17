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
	"6.5840/labgob"
	"6.5840/labrpc"
	"bytes"
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 3D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type Log struct {
	Command interface{}
	Term    int
}

// Raft RPCs are idempotent!!!!!
// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	currentTerm   int
	votedFor      int
	logs          []Log
	snapshot      []byte
	snapshotIndex int
	snapshotTerm  int

	lastLeaderHeartbeatTime time.Time
	isLeader                bool

	nextIndex  []int
	matchIndex []int

	commitIndex int
	lastApplied int
	applyCh     chan ApplyMsg

	newState bool
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.isLeader
}

// helper functions to access log, not thread safe

// to be used before rf.getLog
func (rf *Raft) inLogs(index int) bool {
	return index > rf.snapshotIndex && index-rf.snapshotIndex <= len(rf.logs)
}
func (rf *Raft) getLog(index int) Log {
	return rf.logs[index-rf.snapshotIndex-1]
}
func (rf *Raft) inSnapshot(index int) bool {
	return index <= rf.snapshotIndex
}
func (rf *Raft) lastLogIndex() int {
	return rf.snapshotIndex + len(rf.logs)
}
func (rf *Raft) lastLog() (int, int) {
	lastLogIndex := rf.lastLogIndex()
	if lastLogIndex > 0 {
		if rf.inLogs(lastLogIndex) {
			return lastLogIndex, rf.getLog(lastLogIndex).Term
		}
		return rf.snapshotIndex, rf.snapshotTerm
	}
	return 0, 0
}
func (rf *Raft) appendLog(log Log) {
	rf.logs = append(rf.logs, log)
}

// cut logs in halfs and keep one half
func (rf *Raft) trimLogs(leftLastIndex int, keepLeft bool) {
	if keepLeft {
		rf.logs = rf.logs[:leftLastIndex-rf.snapshotIndex]
	} else {
		rf.logs = rf.logs[leftLastIndex-rf.snapshotIndex:]
	}
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
// not thread safe
func (rf *Raft) persist() {
	if !rf.newState {
		return
	}
	DPrintf("persist state for server %d", rf.me)
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	if e.Encode(rf.currentTerm) != nil || e.Encode(rf.votedFor) != nil || e.Encode(rf.logs) != nil || e.Encode(rf.snapshotIndex) != nil || e.Encode(rf.snapshotTerm) != nil {
		DPrintf("error encode persisted state for server %d", rf.me)
	} else {
		state := w.Bytes()
		rf.persister.Save(state, rf.snapshot)
		rf.newState = false
	}
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var logs []Log
	var snapshotIndex int
	var snapshotTerm int
	if d.Decode(&currentTerm) != nil || d.Decode(&votedFor) != nil || d.Decode(&logs) != nil || d.Decode(&snapshotIndex) != nil || d.Decode(&snapshotTerm) != nil {
		DPrintf("error decode persisted state for server %d", rf.me)
	} else {
		DPrintf("load persis state for server %d", rf.me)
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.logs = logs
		rf.snapshotIndex = snapshotIndex
		rf.snapshotTerm = snapshotTerm
	}
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
// thread unsafe
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	if rf.inSnapshot(index) { // make function idempotent
		return
	}
	rf.snapshot = snapshot
	rf.snapshotTerm = rf.getLog(index).Term
	rf.trimLogs(index, false)
	// snapshotindex need to be updated last
	rf.snapshotIndex = index
	rf.newState = true
	DPrintf("take snapshot for server %d, snapshotindex: %d, lastlogindex %d", rf.me, rf.snapshotIndex, rf.lastLogIndex())
}

type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

// not thread safe
func (rf *Raft) updateTerm(term int) {
	if term > rf.currentTerm {
		DPrintf("term is updated for server %d, currentterm %d, votedfor %d, isleader %t", rf.me, rf.currentTerm, rf.votedFor, rf.isLeader)
		rf.currentTerm = term
		rf.votedFor = -1
		rf.isLeader = false
		rf.newState = true
	}
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	DPrintf("receive request vote, %d <-%d", rf.me, args.CandidateId)
	defer rf.mu.Unlock()
	defer rf.persist()

	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		return
	}
	rf.updateTerm(args.Term)

	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		lastLogIndex, lastLogTerm := rf.lastLog()
		DPrintf("%d (lastterm:%d, lastindex: %d)<-%d(%d, %d)", rf.me, lastLogTerm, lastLogIndex, args.CandidateId, args.LastLogTerm, args.LastLogIndex)
		if args.LastLogTerm > lastLogTerm || (args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndex) {
			rf.votedFor = args.CandidateId
			rf.newState = true
			reply.VoteGranted = true
			reply.Term = rf.currentTerm
			return
		}
	}

	reply.VoteGranted = false
	reply.Term = rf.currentTerm
	return

}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	Entries      []Log
	PrevLogIndex int
	PrevLogTerm  int
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
	XTerm   int
	XIndex  int
	XLen    int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	DPrintf("appendentries %d(term:%d) <- %d(%d)", rf.me, rf.currentTerm, args.LeaderId, args.Term)
	defer rf.mu.Unlock()
	defer rf.persist()
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	rf.updateTerm(args.Term)
	rf.lastLeaderHeartbeatTime = time.Now()
	reply.Term = rf.currentTerm

	if rf.inLogs(args.PrevLogIndex) && rf.getLog(args.PrevLogIndex).Term != args.PrevLogTerm {
		reply.Success = false
		reply.XTerm = rf.getLog(args.PrevLogIndex).Term
		firstIndex := args.PrevLogIndex
		for rf.inLogs(firstIndex-1) && rf.getLog(firstIndex-1).Term == reply.XTerm { // find first index in logs for this XTerm
			firstIndex--
		}
		reply.XIndex = firstIndex
		reply.XLen = rf.lastLogIndex()
		return
	} else if rf.lastLogIndex() < args.PrevLogIndex {
		// log is too short
		reply.Success = false
		reply.XLen = rf.lastLogIndex()
		return
	} else {
		// if args.PrevLogIndex in rf's snapshot, it must match, because commited log won't be deleted from any server
		reply.Success = true
	}

	DPrintf("replicating log on %d from %d, current log index %d, new log index %d, len(args.log): %d", rf.me, args.LeaderId, rf.lastLogIndex(), args.PrevLogIndex+len(args.Entries), len(args.Entries))
	// first index in args.entries that doesn't match logs
	// if i > len(args.entries), then no log needs to be added
	// args.PrevLogIndex + i must be larger than rf.snapshotindex
	// and must be in rf.logs
	i := 1
	for i <= len(args.Entries) && (rf.inSnapshot(args.PrevLogIndex+i) || (rf.inLogs(args.PrevLogIndex+i) && rf.getLog(args.PrevLogIndex+i).Term == args.Entries[i-1].Term)) {
		i++
	}
	// logs is changed: trimmed and or appended
	if rf.lastLogIndex() > args.PrevLogIndex+i-1 || i <= len(args.Entries) {
		rf.newState = true
	}
	rf.trimLogs(args.PrevLogIndex+i-1, true)
	for ; i <= len(args.Entries); i++ {
		rf.appendLog(args.Entries[i-1])
	}

	if args.LeaderCommit > rf.commitIndex {
		DPrintf("increment commit index for %d from %d to %d", rf.me, rf.commitIndex, min(args.LeaderCommit, rf.lastLogIndex()))
		rf.commitIndex = min(args.LeaderCommit, rf.lastLogIndex())
	}
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	if !rf.isLeader {
		return 0, 0, false
	}
	// 1. add command into log
	log := Log{}
	log.Term = rf.currentTerm
	log.Command = command
	rf.appendLog(log)
	rf.newState = true
	lastLogIndex, lastLogTerm := rf.lastLog()
	rf.nextIndex[rf.me] = lastLogIndex + 1
	rf.matchIndex[rf.me] = lastLogIndex
	// 2. send log to followers by heartbeat, return early if enough reply is obtained, while keep retrying the rest, reset heartbeat timer
	// 3. commit the log
	// 4. apply the log
	// 5. return index and term
	// steps 2,3,4,5 are done separately
	DPrintf("server.start, id:%d, lastlogindex:%d", rf.me, rf.lastLogIndex())
	return lastLogIndex, lastLogTerm, true
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// not thread safe
func (rf *Raft) isElectionTimeout(timeout time.Duration) bool {
	DPrintf("checking election timeout for %d, current time %s, last heartbeat %s, duration %s, timeout: %s", rf.me, time.Now().String(), rf.lastLeaderHeartbeatTime.String(), time.Since(rf.lastLeaderHeartbeatTime).String(), timeout.String())
	return !rf.isLeader && time.Since(rf.lastLeaderHeartbeatTime) > timeout
}

func (rf *Raft) timedRequest(i int, method string, args interface{}, reply interface{}) bool {
	call := func(i int, args interface{}, reply interface{}, ch chan bool) {
		ch <- rf.peers[i].Call(method, args, reply)
	}
	ch := make(chan bool)
	go call(i, args, reply, ch)
	select {
	case val := <-ch:
		DPrintf("%s to %d from %d returns", method, i, rf.me)
		return val
	case <-time.After(100 * time.Millisecond):
		DPrintf("%s to %d from %d timedout", method, i, rf.me)
	}
	return false
}

// thread unsafe, and idempotent
// assume lock already acquired entering the function
func (rf *Raft) sendRequestVote(i int, ch chan bool) {
	rf.mu.Lock()
	args := RequestVoteArgs{}
	args.Term = rf.currentTerm
	args.CandidateId = rf.me
	lastLogIndex, lastLogTerm := rf.lastLog()
	args.LastLogIndex = lastLogIndex
	args.LastLogTerm = lastLogTerm
	reply := RequestVoteReply{}
	rf.mu.Unlock()
	ok := rf.timedRequest(i, "Raft.RequestVote", &args, &reply)
	if !ok {
		ch <- false
		return
	}
	rf.mu.Lock()
	rf.updateTerm(reply.Term)
	rf.persist()
	rf.mu.Unlock()
	ch <- reply.VoteGranted
	return
}

// thread safe
func (rf *Raft) ticker() {
	for rf.killed() == false {
		timeout := time.Duration(1000+(rand.Int63()%1000)) * time.Millisecond // old value 1000
		rf.mu.Lock()
		isTimeout := rf.isElectionTimeout(timeout)
		rf.mu.Unlock()
		if isTimeout {
			// kick out election process
			DPrintf("start election, id: %d, isleader: %t, prevterm %d, newterm: %d", rf.me, rf.isLeader, rf.currentTerm, rf.currentTerm+1)
			rf.mu.Lock()
			rf.currentTerm += 1
			rf.votedFor = rf.me
			term := rf.currentTerm
			rf.mu.Unlock()
			ch := make(chan bool, len(rf.peers)-1)
			for i, _ := range rf.peers {
				if i == rf.me {
					continue
				}
				go rf.sendRequestVote(i, ch)
			}
			c := 1
			voteGranted := false
			for _ = range len(rf.peers) - 1 {
				voteGranted = <-ch
				DPrintf("received response for request vote, %d", rf.me)
				if voteGranted {
					c += 1
					if c > len(rf.peers)/2 {
						break
					}
				}
			}
			DPrintf("number of votes received for %d: %d", rf.me, c)
			if c > len(rf.peers)/2 {
				rf.mu.Lock()
				if term == rf.currentTerm && rf.isElectionTimeout(timeout) {
					DPrintf("become leader %d", rf.me)
					rf.isLeader = true
					lastLogIndex := rf.lastLogIndex()
					for i := range len(rf.peers) {
						rf.nextIndex[i] = lastLogIndex + 1
						rf.matchIndex[i] = 0
					}
					rf.matchIndex[rf.me] = lastLogIndex
					rf.mu.Unlock()
					rf.heartbeat()
				} else {
					rf.mu.Unlock()
				}
			}
		}
		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 500 + (rand.Int63() % 500)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

// not thread safe
func (rf *Raft) applyCmd() {
	rf.mu.Lock()
	logs := make([]Log, 0)
	for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
		logs = append(logs, rf.getLog(i))
	}
	// need to release lock here before sending msg to channel,
	// because this will trigger taking snapshot which requires lock
	rf.mu.Unlock()

	for _, log := range logs {
		rf.lastApplied++ // could conflict with install snapshot
		msg := ApplyMsg{}
		msg.Command = log.Command
		msg.CommandIndex = rf.lastApplied
		msg.CommandValid = true
		rf.applyCh <- msg
	}
}

func (rf *Raft) applyCmdTicker() {
	for rf.killed() == false {
		rf.applyCmd()
		time.Sleep(15 * time.Millisecond)
	}
}
func (rf *Raft) heartbeatTicker() {
	for rf.killed() == false {
		rf.mu.Lock()
		if rf.isLeader {
			rf.mu.Unlock()
			rf.heartbeat()
		} else {
			rf.mu.Unlock()
		}
		sendFaster := false
		rf.mu.Lock()
		if rf.isLeader {
			for i, index := range rf.matchIndex {
				if i != rf.me && index < rf.lastLogIndex() {
					sendFaster = true
					break
				}
			}
		}
		rf.mu.Unlock()
		if sendFaster {
			time.Sleep(time.Duration(30) * time.Millisecond)
		} else {
			time.Sleep(time.Duration(50) * time.Millisecond)
		}
	}
}

// not thread safe
func (rf *Raft) onLogReplicated() {
	matches := make([]int, len(rf.peers))
	copy(matches, rf.matchIndex)
	sort.Sort(sort.Reverse(sort.IntSlice(matches)))

	commitIndex := matches[len(rf.peers)/2]
	if commitIndex <= rf.commitIndex || rf.getLog(commitIndex).Term < rf.currentTerm {
		return
	}
	rf.commitIndex = commitIndex
}

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	DPrintf("receive install snapshot from leader %d to follower %d, len of data: %d", args.LeaderId, rf.me, len(args.Data))
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		return
	}
	rf.updateTerm(args.Term)
	rf.lastLeaderHeartbeatTime = time.Now()
	if rf.inSnapshot(args.LastIncludedIndex) || (rf.inLogs(args.LastIncludedIndex) && rf.getLog(args.LastIncludedIndex).Term == args.LastIncludedTerm) {
		reply.Term = rf.currentTerm
		return
	}
	DPrintf("install snapshot on server %d succeed, curr snapshotindex %d, curr lastlogindex %d", rf.me, rf.snapshotIndex, rf.lastLogIndex())
	rf.snapshot = args.Data
	rf.snapshotIndex = args.LastIncludedIndex
	rf.snapshotTerm = args.LastIncludedTerm
	rf.logs = make([]Log, 0)

	rf.commitIndex = args.LastIncludedIndex
	rf.lastApplied = rf.commitIndex
	msg := ApplyMsg{}
	msg.Snapshot = rf.snapshot
	msg.SnapshotIndex = rf.snapshotIndex
	msg.SnapshotTerm = rf.snapshotTerm
	msg.SnapshotValid = true
	msg.CommandValid = false
	rf.applyCh <- msg

	rf.newState = true
	DPrintf("after install snapshot on server %d, new snapshotindex %d, new lastlogindex %d", rf.me, rf.snapshotIndex, rf.lastLogIndex())
}

// not thread safe
// assume lock is acquired entering this function
func (rf *Raft) sendInstallSnapshotRequest(i int) {
	args := InstallSnapshotArgs{}
	reply := InstallSnapshotReply{}
	args.Term = rf.currentTerm
	args.LeaderId = rf.me
	args.LastIncludedIndex = rf.snapshotIndex
	args.LastIncludedTerm = rf.snapshotTerm
	args.Data = rf.snapshot
	// take a snapshot of the state before sending request
	// for verification after request returns
	snapshotIndex := rf.snapshotIndex
	rf.mu.Unlock()
	DPrintf("calling installsnapshot follower %d, from leader %d, snapshotindex: %d", i, rf.me, snapshotIndex)
	ok := rf.timedRequest(i, "Raft.InstallSnapshot", &args, &reply)
	if !ok {
		// exit, til next heartbeat
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.updateTerm(reply.Term)
	rf.persist()
	if !rf.isLeader {
		// don't need to call followers anymore
		DPrintf("leader %d turns into follower, exit callfollower", rf.me)
		return
	}
	if rf.nextIndex[i] >= snapshotIndex+1 {
		return
	}
	rf.nextIndex[i] = snapshotIndex + 1
	rf.matchIndex[i] = rf.nextIndex[i] - 1
}

// not thread safe
// assume lock is acquired entering this function
func (rf *Raft) sendAppendEntriesRequest(i int) {
	args := AppendEntriesArgs{}
	reply := AppendEntriesReply{}
	args.Term = rf.currentTerm
	args.LeaderId = rf.me
	args.PrevLogIndex = rf.nextIndex[i] - 1
	getLogTerm := func(i int) int {
		if i == rf.snapshotIndex {
			return rf.snapshotTerm
		}
		return rf.getLog(i).Term
	}
	args.PrevLogTerm = getLogTerm(args.PrevLogIndex)
	args.Entries = make([]Log, 0)
	for i := rf.nextIndex[i]; rf.inLogs(i); i++ {
		args.Entries = append(args.Entries, rf.getLog(i))
	}
	args.LeaderCommit = rf.commitIndex

	// taking a snapshot of state to process reply
	lastLogIndex := rf.lastLogIndex()
	rf.mu.Unlock()

	DPrintf("calling follower %d, from leader %d", i, rf.me)
	ok := rf.timedRequest(i, "Raft.AppendEntries", &args, &reply)
	if !ok {
		// exit, til next heartbeat
		DPrintf("calling follower %d, from leader %d failed", i, rf.me)
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.updateTerm(reply.Term)
	rf.persist()
	if !rf.isLeader {
		// don't need to call followers anymore
		DPrintf("leader %d turns into follower, exit callfollower", rf.me)
		return
	}

	if !reply.Success {
		DPrintf("calling follower %d, from leader %d returned, but unsuccessful", i, rf.me)
		// things might change while waiting for the reply
		if rf.nextIndex[i] != args.PrevLogIndex+1 {
			return
		}
		// follower's log is too short
		if reply.XLen < args.PrevLogIndex {
			rf.nextIndex[i] = reply.XLen + 1
			return
		}
		// previndex's term is smaller than xterm, skip xterm
		// prevIndex must >= rf.snapshotIndex
		if (args.PrevLogIndex == rf.snapshotIndex && rf.snapshotTerm < reply.XTerm) || (rf.getLog(args.PrevLogIndex).Term < reply.XTerm) {
			rf.nextIndex[i] = reply.XIndex
			return
		}
		// find lastindex of Xterm in logs
		lastIndex := -1
		for i := args.PrevLogIndex - 1; i >= rf.snapshotIndex && getLogTerm(i) >= reply.XTerm; i-- {
			if getLogTerm(i) == reply.XTerm {
				lastIndex = i
				break
			}
		}
		// logs doesn't have the term, so skip it
		if lastIndex == -1 {
			rf.nextIndex[i] = reply.XIndex
			return
		}
		// rf.log has the term, jump to the last index under the xterm
		rf.nextIndex[i] = lastIndex + 1
		return
	} else {
		DPrintf("calling follower %d, from leader %d returned,  success", i, rf.me)
		// things might change while waiting for the reply
		if rf.nextIndex[i] < lastLogIndex+1 {
			rf.nextIndex[i] = lastLogIndex + 1
			rf.matchIndex[i] = rf.nextIndex[i] - 1
			rf.onLogReplicated()
		}
	}
}

// thread safe
func (rf *Raft) callFollower(i int) {
	rf.mu.Lock()
	if !rf.isLeader {
		rf.mu.Unlock()
		return
	}

	if rf.nextIndex[i]-1 < rf.snapshotIndex {
		rf.sendInstallSnapshotRequest(i)
	} else {
		rf.sendAppendEntriesRequest(i)
	}
}

// thread safe
func (rf *Raft) heartbeat() {
	DPrintf("sending heart beat from %d", rf.me)
	for i, _ := range rf.peers {
		if i == rf.me {
			continue
		}
		go rf.callFollower(i)
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (3A, 3B, 3C).
	rf.isLeader = false
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.dead = 0
	rf.lastLeaderHeartbeatTime = time.Now()

	rf.logs = make([]Log, 0)
	rf.applyCh = applyCh
	rf.newState = false
	rf.snapshot = make([]byte, 0)
	rf.snapshotIndex = 0
	rf.snapshotTerm = 0

	DPrintf("make %d, numer of peers: %d", rf.me, len(peers))

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.snapshot = persister.ReadSnapshot()
	// add comment
	rf.commitIndex = rf.snapshotIndex
	rf.lastApplied = rf.commitIndex
	rf.matchIndex = make([]int, len(rf.peers))
	rf.nextIndex = make([]int, len(rf.peers))
	for i := range len(rf.peers) {
		rf.nextIndex[i] = rf.lastLogIndex() + 1
		rf.matchIndex[i] = 0
	}
	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.heartbeatTicker()
	go rf.applyCmdTicker()
	return rf
}
