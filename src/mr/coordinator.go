package mr

import (
	// "fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Coordinator struct {
	// Your definitions here.
	Rlock   sync.Mutex //
	Rtasks  []int      // len = NReduce, 0: not allocated 1: executing 2:reduced
	NReduce int

	Mlock  sync.Mutex
	Files  []string
	NMap   int
	Mtasks []int // len = NMap, 0: not mtasks, 1: mapping, 2: mapped

	timeM []time.Time // len = NMap
	timeR []time.Time // len = NReduce

	Dlock      sync.RWMutex
	MapDone    bool
	ReduceDone bool
}

func (c *Coordinator) nameToIndex(fileName string) int {
	for i := 0; i < c.NMap; i++ {
		if c.Files[i] == fileName {
			return i
		}
	}
	return -1
}

func (c *Coordinator) SearchMap() int {
	c.Mlock.Lock()
	for i := 0; i < c.NMap; i++ {
		if c.Mtasks[i] == 0 || (c.Mtasks[i] == 1 && time.Since(c.timeM[i]).Seconds() > 10) {
			c.Mtasks[i] = 1
			c.timeM[i] = time.Now()
			c.Mlock.Unlock()
			return i
		}
	}
	c.Mlock.Unlock()
	return -1
}

func (c *Coordinator) CheckMap() int {
	c.Dlock.RLock()
	if c.MapDone {
		c.Dlock.RUnlock()
		return 1
	}
	c.Dlock.RUnlock()
	c.Mlock.Lock()
	for i := 0; i < c.NMap; i++ {
		if c.Mtasks[i] != 2 {
			c.Mlock.Unlock()
			return 0
		}
	}
	c.Mlock.Unlock()
	c.Dlock.Lock()
	c.MapDone = true
	c.Dlock.Unlock()

	return 1
}

func (c *Coordinator) SearchReduce() int {
	c.Rlock.Lock()
	for i := 0; i < c.NReduce; i++ {
		if c.Rtasks[i] == 0 || (c.Rtasks[i] == 1 && time.Since(c.timeR[i]).Seconds() > 10) {
			c.Rtasks[i] = 1
			c.timeR[i] = time.Now()
			c.Rlock.Unlock()
			return i
		}
	}
	c.Rlock.Unlock()
	return -1
}

func (c *Coordinator) CheckReduce() int {
	c.Dlock.RLock()
	if c.ReduceDone == true {
		c.Dlock.RUnlock()
		return 1
	}
	c.Dlock.RUnlock()
	c.Rlock.Lock()
	for i := 0; i < c.NReduce; i++ {
		if c.Rtasks[i] != 2 {
			c.Rlock.Unlock()
			return 0
		}
	}
	c.Rlock.Unlock()

	c.Dlock.Lock()
	c.ReduceDone = true
	c.Dlock.Unlock()
	return 1
}

// Your code here -- RPC handlers for the worker to call.

func (c *Coordinator) RPChandler(args *Args, reply *Reply) error {
	// fmt.Printf("rpc\n")
	// if args.Hello == 1 { // to confirm workers' number
	// 	// reply.ReduceN = c.NReduce
	// 	c.Wlock.Lock()
	// 	c.Nworkers++
	// 	c.Wlock.Unlock()
	// 	return nil
	// }

	if args.TaskType == 1 {
		// fmt.Printf("rpc:map %d\n", args.TaskI)
		c.Mlock.Lock()
		c.Mtasks[args.TaskI] = 2
		c.Mlock.Unlock()
	} else if args.TaskType == 2 {
		// fmt.Printf("rpc:reduce %d\n", args.TaskI)
		c.Rlock.Lock()
		c.Rtasks[args.TaskI] = 2
		c.Rlock.Unlock()
	}
	// fmt.Printf("map\n")
	allMapped := c.CheckMap()
	if allMapped == 0 {
		i := c.SearchMap()
		if i != -1 {

			reply.TaskType = 1
			reply.TaskI = i
			reply.FileName = c.Files[i]
			reply.ReduceN = c.NReduce

			c.Mlock.Lock()
			c.Mtasks[i] = 1
			c.Mlock.Unlock()

			return nil
		} else { // mapping
			reply.Waiting = 1
			// fmt.Printf("allMapped:waiting\n")
			return nil
		}
	}

	// fmt.Printf("reduce %d\n", c.Nworkers)
	allReduced := c.CheckReduce()
	if allReduced == 0 {
		j := c.SearchReduce()
		if j != -1 {

			reply.TaskType = 2
			reply.TaskI = j
			reply.MapN = c.NMap

			c.Rlock.Lock()
			c.Rtasks[j] = 1
			c.Rlock.Unlock()

			return nil
		} else {
			reply.Waiting = 1
			// fmt.Printf("allReduced:waiting\n")
			return nil
		}
	}
	// fmt.Printf("exit\n")
	reply.Exit = 1
	return nil
}

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	// l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	// Your code here.
	return c.ReduceDone
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// NReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	for i := 0; i < len(files); i++ {
		c.Files = append(c.Files, files[i])
		c.Mtasks = append(c.Mtasks, 0)
		c.timeM = append(c.timeM, time.Now())
	}
	for i := 0; i < nReduce; i++ {
		c.Rtasks = append(c.Rtasks, 0)
		c.timeR = append(c.timeR, time.Now())
	}
	c.NReduce = nReduce
	c.NMap = len(files)

	// fmt.Printf("MakeCoordinator\n")
	c.server()
	return &c
}
