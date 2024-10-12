package kvraft

import (
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
	"bytes"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

const Debug = true

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Type     int //GET:0, PUT:1, APPEND:2
	Seq      int64
	Key      string
	Value    string
	ClientID int64

	//done bool
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	DB        map[string]string
	waitingOP map[int]chan Op
	CliSeq    map[int64]int64

	//curSnapshot []byte

	lastLD  int64
	timeOut time.Duration
}

func (kv *KVServer) checkOP() {
	for o := range kv.applyCh {
		if kv.killed() {
			return
		}
		//o := <-kv.applyCh
		if o.CommandValid {
			command, _ := o.Command.(Op)
			kv.mu.Lock()

			key := command.Key
			val := command.Value

			if command.Seq > kv.CliSeq[command.ClientID] {
				kv.CliSeq[command.ClientID] = command.Seq
				switch command.Type {
				case 0:
					//DPrintf("get key:%v val:%v", key, val)
					break
				case 1:
					//DPrintf("put key:%v val:%v", key, val)
					kv.DB[key] = val
					break
				case 2:
					//DPrintf("append key:%v val:%v", key, val)
					_, ok := kv.DB[key]
					if ok {
						kv.DB[key] += val
					} else {
						kv.DB[key] = val
					}
					break
				}
			}
			if command.Type == 0 {
				val = kv.DB[key]
				command.Value = val
			}

			kv.mu.Unlock()
			term, isLeader := kv.rf.GetState()
			logTerm := kv.rf.GetLogTerm(o.CommandIndex)
			if isLeader && term == logTerm {
				//DPrintf("ME:%v seq:%v\n", kv.me, command.Seq)
				kv.getChan(o.CommandIndex) <- command
				//DPrintf("\tME:%v seq:%v\n", kv.me, command.Seq)
			}
			//if isLeader {
			//	DPrintf("me:%v\n", kv.me)
			//	for key, val := range kv.DB {
			//		DPrintf("\tkey:%v val:%v\n", key, val)
			//	}
			//}
			//go func() {
			persister := kv.rf.Persister()
			size := persister.RaftStateSize()
			if size >= kv.maxraftstate && kv.maxraftstate != -1 {
				w := new(bytes.Buffer)
				e := labgob.NewEncoder(w)
				kv.mu.Lock()
				e.Encode(kv.DB)
				e.Encode(kv.CliSeq)

				//DPrintf("server:%v snap:%v\n", kv.me, o.CommandIndex)

				kv.mu.Unlock()
				kv.rf.Snapshot(o.CommandIndex, w.Bytes())
			}
			//}()

		} else {
			DPrintf("\nsnap\n\n")
			kv.mu.Lock()
			kv.installSnap(o.Snapshot)
			kv.mu.Unlock()
		}
	}

}

func (kv *KVServer) checkSnap() {
	for !kv.killed() {
		persister := kv.rf.Persister()
		size := persister.RaftStateSize()
		if size >= kv.maxraftstate && kv.maxraftstate != -1 {
			w := new(bytes.Buffer)
			e := labgob.NewEncoder(w)
			kv.mu.Lock()
			e.Encode(kv.DB)
			e.Encode(kv.CliSeq)
			kv.rf.GetMu()
			ind := kv.rf.GetLastApplied() //如果0是lastInclude
			kv.rf.RelMu()

			//DPrintf("server:%v snap:%v\n", kv.me, ind)

			kv.mu.Unlock()
			kv.rf.Snapshot(ind, w.Bytes())
		}
		time.Sleep(time.Duration(50) * time.Millisecond)
	}
}

func (kv *KVServer) getChan(logInd int) chan Op {
	kv.mu.Lock()
	ch, hasChan := kv.waitingOP[logInd]
	if !hasChan {
		kv.waitingOP[logInd] = make(chan Op)
		ch = kv.waitingOP[logInd]
	}

	kv.mu.Unlock()
	return ch
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {

	// Your code here.
	op := Op{
		Type:     0,
		Seq:      args.Seq,
		Key:      args.Key,
		Value:    "",
		ClientID: args.Me,
	}

	kv.mu.Lock()
	ind, _, ok := kv.rf.Start(op)
	//kv.mu.Lock()
	//defer kv.mu.Unlock()
	if !ok {
		kv.mu.Unlock()
		reply.Err = ErrWrongLeader
		//DPrintf("WrongLD me:%v\n", kv.me)
		return
	}
	kv.mu.Unlock()

	//DPrintf("get me:%v seq:%v\n", kv.me, op.Seq)
	select {
	case command := <-kv.getChan(ind):
		//DPrintf("get chan op:%v\n", command)
		//command, _ := o.Command.(Op)
		if command.Seq == op.Seq && command.ClientID == op.ClientID {
			reply.Value = command.Value
			if reply.Value == "" {
				reply.Err = ErrNoKey
			} else {
				reply.Err = OK
			}

		}
	case <-time.After(kv.timeOut):
		DPrintf("get timeOut op:%v \n", op)
		reply.Err = ErrWrongLeader
	}

	go func() {
		kv.mu.Lock()
		delete(kv.waitingOP, ind)
		kv.mu.Unlock()
	}()

}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	var OpType int
	if args.Op == "Put" {
		OpType = 1
	} else {
		OpType = 2
	}
	//DPrintf("type:%v\n", OpType)
	op := Op{
		Type:     OpType,
		Seq:      args.Seq,
		Key:      args.Key,
		Value:    args.Value,
		ClientID: args.Me,
	}

	kv.mu.Lock()
	lastSeq := kv.CliSeq[op.ClientID]
	if op.Seq <= lastSeq {
		kv.mu.Unlock()
		reply.Err = OK
		return
	}
	//kv.mu.Unlock()

	ind, _, ok := kv.rf.Start(op)
	//kv.mu.Lock()
	//defer kv.mu.Unlock()
	if !ok {
		kv.mu.Unlock()
		//DPrintf("not ld %v\n", kv.me)
		reply.Err = ErrWrongLeader
		//DPrintf("WrongLD me:%v\n", kv.me)
		return
	}
	kv.mu.Unlock()

	//DPrintf("LD me:%v\n", kv.me)
	select {
	case command := <-kv.getChan(ind):

		if command.Seq == op.Seq && command.ClientID == op.ClientID {
			reply.Err = OK
		}
	case <-time.After(kv.timeOut):
		DPrintf("put timeOut op:%v \n", op)
		reply.Err = ErrWrongLeader
	}
	go func() {
		kv.mu.Lock()
		delete(kv.waitingOP, ind)
		kv.mu.Unlock()
	}()
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

// StartKVServer servers[] contains the ports of the set of
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

	//buf := bytes.NewBuffer(snapData)
	//d := labgob.NewDecoder(buf)
	//if d.Decode(kv.DB) != nil {
	//	fmt.Errorf("kvserver decode snap\n")
	//}
	// You may need initialization code here.

	//DPrintf("make server %v\n", kv.me)

	kv.waitingOP = make(map[int]chan Op)
	kv.DB = make(map[string]string)
	kv.CliSeq = make(map[int64]int64)
	kv.timeOut = time.Duration(1000) * time.Millisecond
	snapData := persister.ReadSnapshot()
	kv.mu.Lock()
	kv.installSnap(snapData)
	kv.mu.Unlock()
	go kv.checkOP()
	//go kv.checkSnap()
	return kv
}

func (kv *KVServer) installSnap(snapData []byte) {

	buf := bytes.NewBuffer(snapData)
	d := labgob.NewDecoder(buf)
	var db map[string]string
	var cliseq map[int64]int64
	if d.Decode(&db) != nil || d.Decode(&cliseq) != nil {
		fmt.Errorf("kvserver decode snap\n")
	}
	if len(db) == 0 || len(cliseq) == 0 {
		//DPrintf("db0 server:%v\n", kv.me)
		return
	}
	kv.DB = db
	kv.CliSeq = cliseq
	//DPrintf("install snap\n")
	//for key, val := range kv.DB {
	//	DPrintf("\tkey:%v val:%v\n", key, val)
	//}
}
