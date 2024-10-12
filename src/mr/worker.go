package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string,
) {
	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

	// fmt.Printf("worker\n")
	reply := Reply{}
	args := Args{}

	// args.Hello = 1
	// CallRPC(&reply, &args)
	// args.Hello = 0

	for {
		reply = Reply{}
		CallRPC(&reply, &args)
		if reply.Exit == 1 {
			// fmt.Printf("worker exit\n")
			break
		}
		Working(&reply, &args, mapf, reducef)
	}
	return
}

func CallRPC(r *Reply, a *Args) bool {
	return call("Coordinator.RPChandler", a, r)
}

func Working(r *Reply, a *Args, mapf func(string, string) []KeyValue,
	reducef func(string, []string) string,
) {
	if r.Waiting == 1 {
		a.TaskType = 0
		time.Sleep(1 * time.Second)
	} else {
		if r.TaskType == 1 {
			// do map and sort
			file, err := os.Open(r.FileName)
			if err != nil {
				log.Fatalf("cannot open %v", r.FileName)
			}
			content, err := io.ReadAll(file)
			if err != nil {
				log.Fatalf("cannot read %v", r.FileName)
			}
			file.Close()
			kva := mapf(r.FileName, string(content))

			iname := "mr-"
			iname += strconv.Itoa(r.TaskI)
			for i := 0; i < len(kva); i++ {
				k := kva[i].Key
				n := ihash(k) % r.ReduceN
				intname := iname + "-" + strconv.Itoa(n)
				ifile, _ := os.OpenFile(intname, os.O_CREATE|os.O_APPEND|os.O_RDWR, 0666)
				enc := json.NewEncoder(ifile)
				err := enc.Encode(&kva[i])
				if err != nil {
					log.Fatalf("cannot write json %v", intname)
				}
			}

			a.TaskType = 1
			a.TaskI = r.TaskI
		} else if r.TaskType == 2 {
			// do reduce
			outname := "mr-out-" + strconv.Itoa(r.TaskI)
			var kvo []KeyValue
			outfile, _ := os.OpenFile(outname, os.O_CREATE|os.O_APPEND|os.O_RDWR, 0666)
			for i := 0; i < r.MapN; i++ {
				intname := "mr-" + strconv.Itoa(i) + "-" + strconv.Itoa(r.TaskI)
				ofile, _ := os.Open(intname)
				dec := json.NewDecoder(ofile)
				for {
					var kv KeyValue
					if err := dec.Decode(&kv); err != nil {
						break
					}
					kvo = append(kvo, kv)
				}
				ofile.Close()
			}
			sort.Sort(ByKey(kvo))

			i := 0
			for i < len(kvo) {
				j := i + 1
				for j < len(kvo) && kvo[j].Key == kvo[i].Key {
					j++
				}
				values := []string{}
				for k := i; k < j; k++ {
					values = append(values, kvo[k].Value)
				}
				output := reducef(kvo[i].Key, values)

				// this is the correct format for each line of Reduce output.
				fmt.Fprintf(outfile, "%v %v\n", kvo[i].Key, output)

				i = j
			}

			outfile.Close()
			a.TaskType = 2
			a.TaskI = r.TaskI
		}
	}
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {
	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
