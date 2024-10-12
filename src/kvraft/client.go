package kvraft

import (
	"6.5840/labrpc"
	"sync"
)
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.

	mu         sync.Mutex
	me         int64
	seq        int64
	prevLeader int
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
	ck.seq = 1
	ck.me = nrand()
	ck.prevLeader = int(nrand()) % len(ck.servers)

	//time.Sleep(time.Duration(500) * time.Millisecond)
	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.
	//DPrintf("cli get:%v\n", key)
	ck.mu.Lock()
	args := GetArgs{Key: key, Seq: ck.seq, Me: ck.me}
	ck.seq++
	ck.mu.Unlock()
	for i := ck.prevLeader; ; i++ {
		reply := GetReply{}
		j := i % len(ck.servers)
		ok := ck.servers[j].Call("KVServer.Get", &args, &reply)
		if !ok || reply.Err == ErrWrongLeader {
			//DPrintf("\t\tget ok:%v err:%v i:%v prevld:%v", ok, reply.Err, j, ck.prevLeader)
			continue
		}

		if ok {
			ck.prevLeader = j
			if reply.Err == OK {
				return reply.Value
			}
			if reply.Err == ErrNoKey {
				return ""
			}
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
	//DPrintf("%v key:%v value:%v\n", op, key, value)
	ck.mu.Lock()
	args := PutAppendArgs{
		Key:   key,
		Value: value,
		Op:    op,
		Seq:   ck.seq,
		Me:    ck.me,
	}
	ck.seq++
	ck.mu.Unlock()
	for i := ck.prevLeader; ; i++ {
		reply := PutAppendReply{}
		j := i % len(ck.servers)
		ok := ck.servers[j].Call("KVServer.PutAppend", &args, &reply)
		if !ok || reply.Err == ErrWrongLeader {
			//DPrintf("\t\tput ok:%v err:%v i:%v", ok, reply.Err, j)
			continue
		}
		if reply.Err == OK {
			//DPrintf("\tp/a ok\n")
			ck.prevLeader = j
			return
		} else if reply.Err == ErrTimeOut {
			DPrintf("timeOut me:%v seq:%v\n", ck.me, ck.seq)
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
