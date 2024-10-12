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
	"bytes"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
)

// ApplyMsg as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type LogEntry struct {
	Term    int
	Command interface{}
}

// Raft A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.RWMutex        // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	applyCh chan ApplyMsg

	timeOut     time.Time
	voteCount   int
	init        bool
	CurrentTerm int
	VotedFor    int
	Logs        []LogEntry // copy of each server's committed entries

	commitIndex int
	lastApplied int

	nextIndex  []int
	matchIndex []int

	lastIncludeIndex int
	lastIncludeTerm  int
	curSnapshot      []byte
	//snapshotPersist  bool

	logsToBeCommitted chan LogEntry
}

func (rf *Raft) Persister() *Persister {
	return rf.persister
}

func (rf *Raft) GetMu() {
	rf.mu.Lock()
}

func (rf *Raft) RelMu() {
	rf.mu.Unlock()
}

func (rf *Raft) GetLastInclude() int {
	return rf.lastIncludeIndex
}

func (rf *Raft) GetLastApplied() int {
	return rf.lastApplied
}

// GetState return CurrentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool
	rf.mu.RLock()
	term = rf.CurrentTerm
	// fmt.Printf("count %d\n", rf.voteCount)
	if rf.VotedFor == rf.me && rf.voteCount >= len(rf.peers)/2+1 {
		isleader = true
	}
	rf.mu.RUnlock()
	// Your code here (2A).
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	//rf.mu.Lock()

	//DPrintf("persist logs:%v\n", len(rf.Logs))
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.CurrentTerm)
	e.Encode(rf.VotedFor)
	e.Encode(rf.Logs)
	e.Encode(rf.lastIncludeIndex)
	e.Encode(rf.lastIncludeTerm)
	raftstate := w.Bytes()
	rf.persister.Save(raftstate, rf.curSnapshot)
	//rf.mu.Unlock()
	//fmt.Printf("\tpersist %v\n", len(rf.curSnapshot))
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var CurrentTerm int
	var logs []LogEntry
	var VotedFor int
	var lastIncludeIndex int
	var lastIncludeTerm int
	if d.Decode(&CurrentTerm) != nil ||
		d.Decode(&VotedFor) != nil || d.Decode(&logs) != nil ||
		d.Decode(&lastIncludeIndex) != nil || d.Decode(&lastIncludeTerm) != nil {
		fmt.Printf("decode\n")
	} else {
		rf.mu.Lock()
		rf.CurrentTerm = CurrentTerm
		rf.VotedFor = VotedFor
		rf.Logs = logs
		rf.lastIncludeIndex = lastIncludeIndex
		rf.lastIncludeTerm = lastIncludeTerm

		rf.lastApplied = rf.lastIncludeIndex
		rf.commitIndex = rf.lastIncludeIndex
		rf.mu.Unlock()
	}
}

type InstallSnapshotArgs struct {
	Term             int
	LeaderID         int
	LastIncludeIndex int
	LastIncludeTerm  int
	Offset           int
	Data             []byte
	Done             bool
}

type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

func (rf *Raft) readSnapshot(data []byte) (int, []LogEntry) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return -1, nil
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	var lastIncludeIndex int
	var logs []LogEntry
	if d.Decode(&lastIncludeIndex) != nil ||
		d.Decode(&logs) != nil {
		fmt.Errorf("decode snap\n")
		//fmt.Printf("decode snap\n")
	}
	return lastIncludeIndex, logs

}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.CurrentTerm
	if args.Term < rf.CurrentTerm {
		return
	}

	//lastInd, snapLogs := rf.readSnapshot(args.Data)
	//oriInd := 0
	//if len(rf.curSnapshot) != 0 {
	//	oriInd, _ = rf.readSnapshot(rf.curSnapshot)
	//}
	//这里没有转换ind
	var i int
	//fmt.Printf("ori %d last %d log %d rflast %d snapl %d\n", oriInd, lastInd, len(rf.Logs), rf.lastIncludeIndex, len(snapLogs))
	for i = 1; i < len(rf.Logs); i++ {
		//fmt.Printf("%d ", rf.encodeLogInd(len(rf.Logs)))
		ind := rf.encodeLogInd(i)
		if ind == args.LastIncludeIndex && rf.Logs[i].Term == args.LastIncludeTerm {
			rf.Logs = rf.Logs[i+1:]
			return
		}
		//fmt.Printf(" o %v", rf.encodeLogInd(i) == lastInd)
		//if rf.Logs[i-oriInd].Term != snapLogs[i].Term {
		//	break
		//}
	}

	rf.CurrentTerm = args.Term
	rf.VotedFor = args.LeaderID

	rf.Logs = make([]LogEntry, 1)
	rf.Logs[0].Term = args.LastIncludeTerm
	rf.curSnapshot = args.Data
	rf.lastIncludeTerm = args.LastIncludeTerm
	//每次snap的时候0会重复snap一次
	rf.lastIncludeIndex = args.LastIncludeIndex
	rf.lastApplied = args.LastIncludeIndex
	rf.commitIndex = args.LastIncludeIndex

	applMsg := ApplyMsg{Snapshot: rf.curSnapshot, SnapshotIndex: rf.lastIncludeIndex, SnapshotValid: true, SnapshotTerm: rf.lastIncludeTerm}
	rf.applyCh <- applMsg

	rf.persist()

}

//func (rf *Raft) checkSnapshot() {
//	for !rf.killed() {
//		time.Sleep(time.Duration(20) * time.Millisecond)
//		rf.mu.Lock()
//		if !rf.snapshotPersist && len(rf.curSnapshot) != 0 {
//			applMsg := ApplyMsg{Snapshot: rf.curSnapshot, SnapshotIndex: rf.lastIncludeIndex, SnapshotValid: true, SnapshotTerm: rf.lastIncludeTerm}
//			rf.applyCh <- applMsg
//			rf.snapshotPersist = true
//		}
//		rf.mu.Unlock()
//		//ind := rf.encodeLogInd(1)
//		//if ind <= rf.lastApplied {
//		//	var xlog []interface{}
//		//	for i := 0; i < rf.decodeLogInd(rf.lastApplied+1); i++ {
//		//		xlog = append(xlog, rf.Logs[i].Command)
//		//	}
//		//	//xlog = rf.Logs[0 : rf.decodeLogInd(rf.commitIndex)+1]
//		//	lastIncludeIndex := rf.lastApplied
//		//	//for i:=0;i<len(snapLogs);i++{
//		//	//	applyMsg := ApplyMsg{}
//		//	//}
//		//	w := new(bytes.Buffer)
//		//	e := labgob.NewEncoder(w)
//		//	e.Encode(lastIncludeIndex)
//		//	e.Encode(xlog)
//		//	rf.mu.Unlock()
//		//	rf.Snapshot(rf.lastApplied, w.Bytes())
//		//
//		//	//r := bytes.NewBuffer(rf.curSnapshot)
//		//	//d := labgob.NewDecoder(r)
//		//	//var lastIncludedIndex int
//		//	//var log []interface{}
//		//	//if d.Decode(&lastIncludedIndex) != nil ||
//		//	//	d.Decode(&log) != nil {
//		//	//	fmt.Printf("error\n")
//		//	//}
//		//
//		//	fmt.Printf("ck snap log %d applied %d\n", len(xlog), rf.lastApplied)
//		//	applMsg := ApplyMsg{Snapshot: rf.curSnapshot, SnapshotIndex: rf.lastIncludeIndex, SnapshotValid: true, SnapshotTerm: rf.lastIncludeTerm}
//		//	rf.applyCh <- applMsg
//		//} else {
//		//	rf.mu.Unlock()
//		//}
//	}
//}

// Snapshot the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//DPrintf("snap ind:%v len:%v\n", index, len(snapshot))

	//fmt.Printf("snapshot %d\n", index)
	rf.curSnapshot = snapshot

	rf.lastIncludeTerm = rf.Logs[rf.decodeLogInd(index)].Term
	//每次snap的时候0会重复snap一次
	rf.Logs = rf.Logs[rf.decodeLogInd(index):]
	rf.lastIncludeIndex = index
	rf.persist()

}

// 可能要返回bool来让handleAE判断追上了没有
func (rf *Raft) handleSnapshot(term int, index int) bool {

	rf.mu.Lock()
	//fmt.Printf("SP ld %d rf %d lastin %d lastTerm %d\n", rf.me, index, rf.lastIncludeIndex, rf.lastIncludeTerm)

	args := InstallSnapshotArgs{Term: term, LeaderID: rf.me, LastIncludeIndex: rf.lastIncludeIndex,
		LastIncludeTerm: rf.lastIncludeTerm, Offset: 0, Data: rf.curSnapshot, Done: true}
	reply := InstallSnapshotReply{}
	rf.mu.Unlock()
	ok := rf.sendInstallSnapshot(index, &args, &reply)
	if !ok {
		return false
	}
	if reply.Term > term {
		rf.mu.Lock()
		//一种可能是当前leader不正当，另一种是收到了过时的回复。
		if rf.CurrentTerm < reply.Term {
			rf.CurrentTerm = reply.Term
			rf.mu.Unlock()
			rf.beFollower(true)
		} else {
			rf.mu.Unlock()
		}
		return false
	}

	if rf.nextIndex[index] < rf.lastIncludeIndex {
		rf.nextIndex[index] = rf.lastIncludeIndex + 1
		rf.matchIndex[index] = rf.lastIncludeIndex
	}
	return true
}

type AppendEntriesArgs struct {
	Term         int
	LeaderID     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}
type AppendEntriesReply struct {
	Term    int
	Success bool
	Xterm   int
	Xindex  int
	Xlen    int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.CurrentTerm
	// reply.lterm = -1

	// 1. new leader fisrt send hb
	// 2. lost follower with higher term receive hb/start
	// 3. lost leader send hb/start
	if args.Term < rf.CurrentTerm {
		reply.Success = false
		//rf.mu.Unlock()
		return
	}
	rf.CurrentTerm = args.Term
	rf.voteCount = 0
	rf.VotedFor = args.LeaderID

	// rf.tmu.Lock()
	rf.timeOut = time.Now() //.Add(time.Duration(rand.Int63()%200) * time.Millisecond)
	// rf.tmu.Unlock()
	//fmt.Printf("\trf:%d logs:%d prev %d lastin %d ld %d\n", rf.me, len(rf.Logs),
	//	args.PrevLogIndex, rf.lastIncludeIndex, args.LeaderID)
	prevI := rf.decodeLogInd(args.PrevLogIndex)
	if prevI < 0 {
		reply.Success = true
		return
	}
	if len(rf.Logs)-1 < prevI {
		//fmt.Printf("len short rf:%d logs:%d prev %d lastin %d\n", rf.me, len(rf.Logs), args.PrevLogIndex, rf.lastIncludeIndex)
		reply.Success = false
		reply.Xlen = rf.encodeLogInd(len(rf.Logs))
		reply.Xterm = -1
	} else if rf.Logs[prevI].Term != args.PrevLogTerm {

		//fmt.Printf("delete log rf:%d logs:%d lt %d prevt %d\n", rf.me, len(rf.Logs), rf.Logs[prevI].Term, args.PrevLogTerm)
		reply.Success = false
		//prevI := rf.decodeLogInd(args.PrevLogIndex)
		reply.Xterm = rf.Logs[prevI].Term
		var i int
		for i = prevI; rf.Logs[i].Term == reply.Xterm && i != 0; i-- {
		}
		reply.Xindex = rf.encodeLogInd(i + 1)
		rf.Logs = rf.Logs[0:prevI]
		rf.persist()
	} else if rf.Logs[prevI].Term == args.PrevLogTerm &&
		len(args.Entries) != 0 {
		reply.Success = true
		if len(rf.Logs)-1 > prevI {
			i := 0
			for i = 0; i < len(args.Entries) && prevI+i+1 < len(rf.Logs); i++ {
				if rf.Logs[prevI+i+1] != args.Entries[i] {
					// fmt.Printf("ap logl %d rf %d prev %d\n", len(rf.Logs), rf.me, args.PrevLogIndex)
					break
				}
			}
			if i < len(args.Entries) {
				// fmt.Printf("ap logl %d rf %d prev %d\n", len(rf.Logs), rf.me, args.PrevLogIndex)
				rf.Logs = rf.Logs[:prevI+i+1]
				rf.Logs = append(rf.Logs, args.Entries[i:]...)
			}
		} else {
			// fmt.Printf("app logl %d rf %d prev %d argl %d lasta %d\n", len(rf.Logs), rf.me,
			// args.PrevLogIndex, len(args.Entries), args.Entries[len(args.Entries)-1].Command)
			rf.Logs = append(rf.Logs, args.Entries...)
		}
		rf.persist()
		//fmt.Printf("append logl %d rf %d\n", len(rf.Logs), rf.me)
	} else if len(args.Entries) == 0 { // heartbeat
		// fmt.Printf("append %d %d\n", rf.me, rf.CurrentTerm)
		reply.Success = true

		ind := rf.commitIndex
		if args.LeaderCommit > rf.commitIndex {
			if args.LeaderCommit < rf.encodeLogInd(len(rf.Logs)-1) {
				ind = args.LeaderCommit
			} else {
				ind = rf.encodeLogInd(len(rf.Logs) - 1)
			}
		}
		//fmt.Printf("rf %d comI %d newcomI %d leader %d ldcomI %d rft %d\n", rf.me, rf.commitIndex, ind, args.LeaderID, args.LeaderCommit, rf.CurrentTerm)
		rf.commitIndex = ind

	}
	// if rf.CurrentTerm != args.Term || rf.VotedFor != args.LeaderID {
	rf.CurrentTerm = args.Term
	rf.voteCount = 0
	rf.VotedFor = args.LeaderID

}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// RequestVoteArgs example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateID  int
	LastLogIndex int
	LastLogTerm  int
}

// RequestVoteReply example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

// RequestVote example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).

	rf.mu.Lock()
	defer rf.mu.Unlock()
	//rf.mu.RLock()
	reply.Term = rf.CurrentTerm
	if args.Term < rf.CurrentTerm {
		// fmt.Printf("reqVote term\n")
		//rf.mu.RUnlock()
		return
	}
	var lastLogTerm int
	lastLogTerm = rf.Logs[len(rf.Logs)-1].Term
	//fmt.Printf("reqVote %d me %d\n", rf.VotedFor, rf.me)
	if (rf.VotedFor == -1 || rf.VotedFor == args.CandidateID || rf.CurrentTerm < args.Term) &&
		(args.LastLogTerm > lastLogTerm ||
			(args.LastLogTerm == lastLogTerm && args.LastLogIndex >= rf.encodeLogInd(len(rf.Logs)-1))) {
		reply.VoteGranted = true
		//fmt.Printf("req grant rf:%d cand:%d t:%d aI:%d rfI:%d lt:%d at:%d\n",
		//	rf.me, args.CandidateID, args.Term, args.LastLogIndex, len(rf.Logs)-1, args.LastLogTerm, rf.Logs[len(rf.Logs)-1].Term)
		rf.VotedFor = args.CandidateID
		rf.CurrentTerm = args.Term
		rf.voteCount = 0

		rf.timeOut = time.Now() //.Add(time.Duration(rand.Int63()%200) * time.Millisecond)
		rf.persist()
		// fmt.Printf("grant rf:%d cand:%d t:%d aI:%d rfI:%d\n",
		// rf.me, args.CandidateID, args.Term, args.LastLogIndex, len(rf.Logs)-1)

	} else {
		reply.VoteGranted = false
	}

}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) encodeLogInd(ind int) int {
	//rf.mu.Lock()
	//defer rf.mu.Unlock()
	return ind + rf.lastIncludeIndex
}
func (rf *Raft) decodeLogInd(ind int) int {
	//rf.mu.Lock()
	//defer rf.mu.Unlock()
	return ind - rf.lastIncludeIndex
}

func (rf *Raft) handleAE(reply *AppendEntriesReply, term int, index int, comI int) {

	if !reply.Success {
		if reply.Term > term {
			//fmt.Printf("term ld:%d\n", rf.me)
			rf.mu.Lock()
			//一种可能是当前leader不正当，另一种是收到了过时的回复。
			if rf.CurrentTerm < reply.Term {
				rf.CurrentTerm = reply.Term
				rf.mu.Unlock()
				rf.beFollower(true)
			} else {
				rf.mu.Unlock()
			}
			return
		} else {
			for !rf.killed() {
				_, isLeader := rf.GetState()
				if !isLeader {
					return
				}

				rf.mu.Lock()
				if term < rf.CurrentTerm {
					rf.mu.Unlock()
					return
				}
				if rf.nextIndex[index] > comI {
					rf.mu.Unlock()
					return
				}

				if comI < rf.encodeLogInd(len(rf.Logs)-1) {
					rf.mu.Unlock()
					return
				}
				realInd := rf.decodeLogInd(comI)
				//fmt.Printf("\tXlen %d xt %d xi %d\n", reply.Xlen, reply.Xterm, reply.Xindex)
				// fmt.Printf("log false next: %d rf:%d ld:%d\n", rf.nextIndex[index], index, rf.me)
				if reply.Xterm == -1 {
					rf.nextIndex[index] = reply.Xlen
				} else {
					if rf.Logs[realInd].Term < reply.Xterm {
						rf.nextIndex[index] = reply.Xindex
					} else {
						//可能不在这里，因为被snapshot了
						var i int
						for i = realInd; rf.Logs[i].Term != reply.Xterm && i > 1; i-- {
						}
						rf.nextIndex[index] = rf.encodeLogInd(i)
					}
				}
				prevInd := rf.decodeLogInd(rf.nextIndex[index] - 1)
				for prevInd < 0 {
					if term < rf.CurrentTerm || rf.nextIndex[index] > comI || comI < rf.encodeLogInd(len(rf.Logs)-1) {
						rf.mu.Unlock()
						return
					}

					rf.mu.Unlock()
					suc := rf.handleSnapshot(term, index)
					if !suc {
						return
					}
					//return
					rf.mu.Lock()
					prevInd = rf.decodeLogInd(rf.nextIndex[index] - 1)
					if comI < rf.encodeLogInd(len(rf.Logs)-1) {
						rf.mu.Unlock()
						return
					}
					//return
				}
				realInd = rf.decodeLogInd(comI)
				//rf.mu.Lock()

				//fmt.Printf("leader:%d term:%d prevInd:%d rf %d log %d lastin %d\n", rf.me, term, rf.nextIndex[index]-1, index, len(rf.Logs), rf.lastIncludeIndex)
				args := AppendEntriesArgs{
					Term: term, PrevLogIndex: rf.nextIndex[index] - 1, PrevLogTerm: rf.Logs[prevInd].Term,
					LeaderCommit: rf.commitIndex, LeaderID: rf.me, Entries: rf.Logs[prevInd+1 : realInd+1],
				}
				*reply = AppendEntriesReply{}
				rf.mu.Unlock()
				ok := false
				// for i := 0; i < 3 && !ok; i++ {
				ok = rf.sendAppendEntries(index, &args, reply)

				if !ok {
					return
				}
				// fmt.Printf("SAE ok:%v reply:%v rf:%d prevInd:%d ld:%d comI %d\n", ok, reply.Success, index,
				// 	args.PrevLogIndex, rf.me, comI)
				if ok && reply.Success {
					// fmt.Printf("break\n")
					break
				}
				if !reply.Success && reply.Term > rf.CurrentTerm {
					fmt.Printf("handAE term\n")
				}
				time.Sleep(10 * time.Millisecond)
			}
		}
	}

	// fmt.Printf("\tld %d comI %d c %d rf %d isSt %v\n", rf.me, comI, rf.commitCount[comI], index, isStart)

	if rf.killed() {
		return
	}
	rf.mu.Lock()
	if rf.nextIndex[index] < comI+1 {
		rf.nextIndex[index] = comI + 1
	}
	rf.matchIndex[index] = rf.nextIndex[index] - 1
	//fmt.Printf("\t\trf:%d match:%d ld:%d\n", index, rf.matchIndex[index], rf.me)
	rf.mu.Unlock()
	return

}

func (rf *Raft) CommitLog(index int, term int, comI int) {
	rf.mu.RLock()
	if rf.VotedFor != rf.me || rf.voteCount < len(rf.peers)/2+1 ||
		rf.nextIndex[index] > comI {
		rf.mu.RUnlock()
		return
	}
	prevInd := rf.decodeLogInd(rf.nextIndex[index] - 1)
	if prevInd < 0 {
		rf.mu.RUnlock()
		//rf.handleSnapshot(term, index)
		return
	}
	realInd := rf.decodeLogInd(comI)
	// fmt.Printf("commitlog len:%d next:%d rf:%d leader:%d term:%d %d\n", len(rf.Logs), rf.nextIndex[index], index,
	// 	rf.me, term, rf.CurrentTerm)
	args := AppendEntriesArgs{
		Term: term, PrevLogIndex: rf.nextIndex[index] - 1, PrevLogTerm: rf.Logs[rf.decodeLogInd(rf.nextIndex[index]-1)].Term,
		LeaderCommit: rf.commitIndex, LeaderID: rf.me, Entries: rf.Logs[rf.decodeLogInd(rf.nextIndex[index]) : realInd+1],
	}
	// for i := 0; i < len(args.Entries); i++ {
	// 	fmt.Printf("entry %d %d %v\n", index, args.Entries[i].Term, args.Entries[i].Command)
	// }
	reply := AppendEntriesReply{}
	rf.mu.RUnlock()
	ok := rf.sendAppendEntries(index, &args, &reply)
	if !ok {
		return
	}
	if ok && reply.Term > term {
		// fmt.Printf("term ld:%d\n", rf.me)
		rf.mu.Lock()
		if rf.CurrentTerm < reply.Term {
			rf.CurrentTerm = reply.Term
			rf.mu.Unlock()
			rf.beFollower(true)
		} else {
			rf.mu.Unlock()
		}

		return
	}

	rf.handleAE(&reply, term, index, comI)
}

func (rf *Raft) checkCommit() {
	// fmt.Printf("checkCommit %v comInd:%d leader:%d\n", rf.Logs[comI].Command, comI, rf.me)

	time.Sleep(time.Duration(100) * time.Millisecond)
	for !rf.killed() {
		time.Sleep(time.Duration(20) * time.Millisecond)
		_, isLeader := rf.GetState()
		rf.mu.Lock()
		if isLeader {
			num := 0
			minI := 1<<31 - 1
			for i := 0; i < len(rf.matchIndex); i++ {
				if rf.matchIndex[i] > rf.commitIndex {
					num++
					if rf.matchIndex[i] < minI {
						minI = rf.matchIndex[i]
					}
				}
			}
			//ori := rf.commitIndex
			//也有可能不在这里面
			realMin := rf.decodeLogInd(minI)
			if num >= len(rf.matchIndex)/2 && rf.commitIndex < minI && rf.Logs[realMin].Term == rf.CurrentTerm {
				rf.commitIndex = minI
			}
			//DPrintf("rf %d lastapply %d comm %d\n", rf.me, rf.lastApplied, rf.commitIndex)
			//fmt.Printf("\trf %d ori %d new %d log %d t %d\n", rf.me, ori, rf.commitIndex, len(rf.Logs), rf.CurrentTerm)
		}
		if rf.lastApplied == rf.commitIndex {
			rf.mu.Unlock()
			continue
		}
		//fmt.Printf("rf %d lastapply %d comm %d\n", rf.me, rf.lastApplied, rf.commitIndex)
		realApplied := rf.decodeLogInd(rf.lastApplied)
		realCom := rf.decodeLogInd(rf.commitIndex)
		logs := rf.Logs[realApplied+1 : realCom+1]

		last := rf.lastApplied
		com := rf.commitIndex
		rf.mu.Unlock()
		for i := 0; i < len(logs); i++ {
			if isLeader {
				//fmt.Printf(" apply %v comInd:%d me:%d\n", logs[i].Command, i+last+1, rf.me)
			}
			c := ApplyMsg{Command: logs[i].Command, CommandValid: true, CommandIndex: i + last + 1}
			rf.applyCh <- c
		}
		rf.mu.Lock()
		if rf.lastApplied < com {
			rf.lastApplied = com
		}
		//fmt.Printf("committed\n")
		rf.mu.Unlock()
	}
}

// Start the service using Raft (e.g. a k/v server) wants to start
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

// 返回的index是真实的还是没有snapshot的
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
	term, isLeader = rf.GetState()
	rf.mu.Lock()

	index = rf.encodeLogInd(len(rf.Logs))

	if !isLeader {
		rf.mu.Unlock()
		return index, term, isLeader
	}
	e := LogEntry{Term: rf.CurrentTerm, Command: command}

	//rf.logsToBeCommitted <- e

	rf.Logs = append(rf.Logs, e)
	comI := len(rf.Logs) - 1 + rf.lastIncludeIndex
	//DPrintf("start ld:%v logs:%v command:%v\n", rf.me, comI, command)

	//fmt.Printf("start ld %d logs %d lastin %d\n", rf.me, len(rf.Logs), rf.lastIncludeIndex)
	rf.persist()
	rf.mu.Unlock()

	for i := 0; i < len(rf.peers) && !rf.killed(); i++ {
		if i == rf.me {
			continue
		}
		go rf.CommitLog(i, term, comI)
	}
	time.Sleep(time.Duration(20) * time.Millisecond)

	rf.mu.Lock()
	isLeader = rf.VotedFor == rf.me && rf.voteCount >= len(rf.peers)/2+1
	rf.mu.Unlock()

	return comI, term, isLeader
}

// Kill the tester doesn't halt goroutines created by Raft after each test,
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

func (rf *Raft) leaderAct(index int, term int, comI int) {
	rf.mu.RLock()
	//comI = comI - rf.lastIncludeIndex
	realInd := rf.decodeLogInd(comI)
	if rf.VotedFor != rf.me || rf.voteCount < len(rf.peers)/2+1 || term < rf.CurrentTerm {
		rf.mu.RUnlock()
		return
	}
	args := AppendEntriesArgs{
		Term: term, PrevLogIndex: comI, PrevLogTerm: rf.Logs[realInd].Term,
		LeaderCommit: rf.commitIndex, LeaderID: rf.me, Entries: []LogEntry{},
	}
	//fmt.Printf("hb rf:%d leaderTerm:%d leader:%d next:%d comI %d lastin %d prevT %d\n", index,
	//	rf.CurrentTerm, rf.me, rf.nextIndex[index], comI, rf.lastIncludeIndex, rf.Logs[realInd].Term)

	reply := AppendEntriesReply{}
	rf.mu.RUnlock()
	ok := rf.sendAppendEntries(index, &args, &reply)
	//fmt.Printf("\thb ok: %v rf:%d leaderTerm:%d leader:%d\n", ok, index, rf.CurrentTerm, rf.me)
	if !ok {
		return
	}
	// fmt.Printf("hb rf:%d leaderTerm:%d leader:%d next:%d comI %d\n", index,
	// 	rf.CurrentTerm, rf.me, rf.nextIndex[index], comI)
	rf.mu.Lock()
	if term < rf.CurrentTerm {
		rf.mu.Unlock()
		return
	}
	if reply.Term > term {
		// fmt.Printf("term ld:%d\n", rf.me)
		rf.CurrentTerm = reply.Term
		rf.mu.Unlock()
		rf.beFollower(true)

		return

	}
	rf.mu.Unlock()
	// 只有start会增加log len
	rf.handleAE(&reply, term, index, comI)
}

// 在过时的leader恢复时，发送hb发现自己过时，转变为follower时需要重置超时时间
func (rf *Raft) beFollower(setTime bool) {
	// fmt.Printf("beF rf %d term %d\n", rf.me, rf.CurrentTerm)
	rf.mu.Lock()
	rf.voteCount = 0
	rf.VotedFor = -1
	if setTime {
		rf.timeOut = time.Now()
	}
	rf.persist()
	rf.mu.Unlock()
}

func (rf *Raft) followerAct(index int, term int, comI int) {
	rf.mu.RLock()

	realInd := rf.decodeLogInd(comI)
	//可能在发的过程中发现自己已经被改成follower了，log被改了
	if comI > rf.encodeLogInd(len(rf.Logs)-1) || term < rf.CurrentTerm {
		rf.mu.RUnlock()
		return
	}
	args := RequestVoteArgs{
		Term: term, CandidateID: rf.me,
		LastLogIndex: comI, LastLogTerm: rf.Logs[realInd].Term,
	}
	reply := RequestVoteReply{}
	rf.mu.RUnlock()
	ok := rf.sendRequestVote(index, &args, &reply)
	if !ok {
		// fmt.Printf("vote !ok %d\n", index)
		return
	}
	// fmt.Printf("vote rf %d reply %v isme %d term %d ok %v\n", index, reply.VoteGranted, rf.VotedFor, term, ok)
	rf.mu.Lock()
	if rf.VotedFor != rf.me {
		rf.mu.Unlock()
		// fmt.Printf("vote !ld %d\n", rf.me)
		return
	}
	if term < rf.CurrentTerm {
		rf.mu.Unlock()
		return
	}
	if reply.Term > term {
		// fmt.Printf("vote term %d\n", rf.me)
		rf.CurrentTerm = reply.Term
		rf.mu.Unlock()
		rf.beFollower(false)
		return
	}
	if reply.VoteGranted && rf.VotedFor == rf.me {
		rf.voteCount++
	}
	if !rf.init && rf.voteCount >= len(rf.peers)/2+1 {
		fmt.Printf("sel %d %d %d\n", len(rf.Logs), rf.CurrentTerm, rf.me)
		for i := 0; i < len(rf.peers); i++ {
			rf.nextIndex[i] = rf.encodeLogInd(len(rf.Logs))
			rf.matchIndex[i] = 0
		}
		rf.init = true
		rf.persist()
	}
	rf.mu.Unlock()
	return
}

func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here (2A)
		// Check if a leader election should be started.

		term, isLeader := rf.GetState()
		// fmt.Printf("isleader %d %d %d\n", rf.me, rf.voteCount, len(rf.peers))
		if isLeader {
			rf.mu.Lock()
			comI := rf.encodeLogInd(len(rf.Logs) - 1)
			//comI := len(rf.Logs) - 1 + rf.lastIncludeIndex
			rf.mu.Unlock()
			for i := 0; i < len(rf.peers) && !rf.killed(); i++ {
				if i == rf.me {
					continue
				}
				go rf.leaderAct(i, term, comI)
			}
			time.Sleep(50 * time.Millisecond)
		} else {
			outMs := 300 + (rand.Int63() % 150)
			rf.mu.Lock()
			isTimeOut := time.Since(rf.timeOut).Milliseconds() > outMs
			if isTimeOut {
				//fmt.Printf("election %d term %d\n", rf.me, rf.CurrentTerm+1)
				rf.CurrentTerm++
				term := rf.CurrentTerm
				rf.VotedFor = rf.me
				rf.voteCount = 1
				rf.init = false

				rf.persist()
				comI := rf.encodeLogInd(len(rf.Logs) - 1)
				//rf.timeOut = time.Now().Add(time.Duration(rand.Int63()%200) * time.Millisecond)
				rf.mu.Unlock()
				for i := 0; i < len(rf.peers) && !rf.killed(); i++ {
					if i == rf.me {
						continue
					}
					go rf.followerAct(i, term, comI)
				}
				time.Sleep(time.Duration(20) * time.Millisecond)

				rf.mu.Lock()

				if rf.VotedFor == rf.me && rf.voteCount < len(rf.peers)/2+1 /*) || rf.voteCount == 0*/ {
					// fmt.Printf("fail %d %d\n", rf.me, rf.CurrentTerm)
					rf.mu.Unlock()
					rf.beFollower(false)
					ms := 50 + (rand.Int63() % 100)
					time.Sleep(time.Duration(ms) * time.Millisecond)
					// }
				} else {
					rf.mu.Unlock()
				}

				// pause for a random amount of time between 50 and 350
				// milliseconds.
			} else {
				rf.mu.Unlock()
				time.Sleep(time.Duration(20) * time.Millisecond)
			} // timeout
		}
	}
}

func (rf *Raft) GetLogTerm(index int) int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	realInd := rf.decodeLogInd(index)
	if realInd < len(rf.Logs) {
		return rf.Logs[realInd].Term
	}
	return -1
}

// Make
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
	persister *Persister, applyCh chan ApplyMsg,
) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	//fmt.Printf("make %d\n", me)
	// Your initialization code here (2A, 2B, 2C).
	rf.VotedFor = -1
	rf.voteCount = 0
	rf.CurrentTerm = 0

	rf.Logs = make([]LogEntry, 1)
	//rf.commitCount = make([]int, 1)
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	for i := 0; i < len(rf.peers); i++ {
		rf.nextIndex[i] = 1
		rf.matchIndex[i] = 0
	}

	rf.commitIndex = 0
	rf.lastIncludeTerm = 0
	rf.lastIncludeIndex = 0

	rf.timeOut = time.Now() //.Add(time.Duration(-350) * time.Millisecond)
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.curSnapshot = persister.ReadSnapshot()
	//fmt.Printf("log %d\n", len(rf.Logs))
	rf.applyCh = applyCh
	// start ticker goroutine to start elections

	//rf.logsToBeCommitted = make(chan LogEntry, 1000)

	go rf.ticker()

	go rf.checkCommit()

	if _, isLeader := rf.GetState(); isLeader {
		rf.Start(nil)
	}

	//go rf.checkStart()
	//go rf.checkSnapshot()

	return rf
}
