package mr

import (
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"sort"
	"json"
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
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {

	log.Println("Worker:: worker called")
	filename := CallForMapTask()

	// Your worker implementation here.
	// uncomment to send the Example RPC to the coordinator.
	intermediate := collectIntermediate(mapf, filename)
	
	interName := "map-inter-" + filename

	// enc := json.NewEncoder(intermediate)
	// for _, kv := range intermediate {
	//   err := enc.Encode(&kv)
	//   if err {
	// 	log.Fatal(err);
	// 	panic("Can't encode temp file, worker paniced")
	//   }
	// }

	interFile, _ := os.Create(interName)
	
	for _, kv := range intermediate {
		fmt.Fprintf(interFile, "%v %v\n", kv.Key, kv.Value)

	}


	//
	// a big difference from real MapReduce is that all the
	// intermediate data is in one place, intermediate[],
	// rather than being partitioned into NxM buckets.
	//
	sort.Sort(ByKey(intermediate))

	oname := "mr-out-"+filename
	ofile, _ := os.Create(oname)

	produceReducedOutput(intermediate, reducef, ofile)

	ofile.Close()

}

func collectIntermediate(mapf func(string, string) []KeyValue, filename string) []KeyValue {
	
	intermediate := []KeyValue{}
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := io.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	kva := mapf(filename, string(content))
	intermediate = append(intermediate, kva...)

	return intermediate
}

func produceReducedOutput(intermediate []KeyValue, reducef func(string, []string) string, ofile *os.File) {
	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)

		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}
}

func CallForMapTask() string {
	log.Println("Worker::callMap initialized")
	args := TaskRequest{}
	reply := TaskReply{}
	handlerName := "GiveMeAMapTask"
	ok := call("Coordinator." + handlerName, &args, &reply)
	if ok {
		log.Printf("Worker::coordinator replied with %s\n", reply)
	} else {
		fmt.Println("MyCall failed!")
	}

	return reply.Filename;
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {
	log.Println()
	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99
	args.Hello = "Hello"

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
		fmt.Printf("coordinator replyed with %s\n", reply.World)
	} else {
		fmt.Printf("Example call failed!\n")
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
