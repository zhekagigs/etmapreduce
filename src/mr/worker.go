package mr

import (
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
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
// The map phase should divide the intermediate keys into buckets for nReduce reduce tasks, where nReduce is the number of reduce tasks -- the argument that main/mrcoordinator.go passes to MakeCoordinator(). Each mapper should create nReduce intermediate files for consumption by the reduce tasks.
// The worker implementation should put the output of the X'th reduce task in the file mr-out-X.
// A mr-out-X file should contain one line per Reduce function output. The line should be generated with the Go "%v %v" format, called with the key and value.
// When the job is completely finished, the worker processes should exit. A simple way to implement this is to use the return value from call(): if the worker fails to contact the coordinator, it can assume that the coordinator has exited because the job is done, so the worker can terminate too. Depending on your design, you might also find it helpful to have a "please exit" pseudo-task that the coordinator can give to workers.
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {

	log.Println("Worker:: worker started")
	replied := CallForMapTask()
	// Optional json encoding for temp files
	// enc := json.NewEncoder(intermediate)
	// for _, kv := range intermediate {
	//   err := enc.Encode(&kv)
	//   if err {
	// 	log.Fatal(err);
	// 	panic("Can't encode temp file, worker paniced")
	//   }
	// }
	//use os to write temp file
	intermediate := doMap(mapf, replied)
	CallMapTaskDone(intermediate)
	// doReduce(replied, intermediate, reducef)

}

func CallMapTaskDone(intermediate string) TaskReply {
	log.Println("Worker::callMap initialized")
	args := TaskRequest{Completed}
	reply := TaskReply{}
	handlerName := "GiveMeAMapTask"
	ok := call("Coordinator."+handlerName, &args, &reply)
	if ok {
		log.Printf("Worker::coordinator replied with %s\n", reply)
	} else {
		fmt.Println("MyCall failed!")
	}

	return reply
}

func doReduce(replied TaskReply, intermediate string, reducef func(string, []string) string) {
	oname := "mr-out-" + strconv.Itoa(replied.TaskNumber)
	ofile, _ := os.Create(oname)
	// produceReducedOutput(intermediate, reducef, ofile)
	log.Println("Worker::finished printing out file " + oname)
	ofile.Close()
}

func doMap(mapf func(string, string) []KeyValue, replied TaskReply) string {

	intermediate := collectIntermediate(mapf, replied.Filename, replied.NumReducers)
	// sort.Sort(ByKey(intermediate))

	interName := "map-inter-" + replied.Filename

	
	for key, kvpairs := range intermediate {
		interFile, _ := os.Create(interName + strconv.Itoa(key))
		fmt.Fprintf(interFile, "%v %v\n", kvpairs.Key, kvpairs.Value)
	}
	log.Println("Worker::finished printing intermeditea file " + interName)
	interFile.Close()
	return interName
}

func collectIntermediate(mapf func(string, string) []KeyValue, filename string, nReduce int)  map[int][]KeyValue {
	
	m := make(map[int][]KeyValue)

	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := io.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	keyValuePairs := mapf(filename, string(content))
	for _, kv := range keyValuePairs {
		partitionKey := ihash(kv.Key) % nReduce

		if _, ok := m[partitionKey]; !ok {
			m[partitionKey] = []KeyValue{kv}
		} else {
			m[partitionKey] = append(m[partitionKey], kv)
		}
		if (len(m)) > nReduce {
			panic("Worken:: partition failed")
		} 
	}

	return m
}

// func produceReducedOutput(intermediateFile string, reducef func(string, []string) string, ofile *os.File) {
// 	i := 0
// 	file := os.Open(intermediateFile)

// 	for i < len(intermediate) {
// 		j := i + 1
// 		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
// 			j++
// 		}
// 		values := []string{}
// 		for k := i; k < j; k++ {
// 			values = append(values, intermediate[k].Value)
// 		}
// 		output := reducef(intermediate[i].Key, values)

// 		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

// 		i = j
// 	}
// }

func CallForMapTask() TaskReply {
	log.Println("Worker::callMap initialized")
	args := TaskRequest{}
	reply := TaskReply{}
	handlerName := "GiveMeAMapTask"
	ok := call("Coordinator."+handlerName, &args, &reply)
	if ok {
		log.Printf("Worker::coordinator replied with %s\n", reply)
	} else {
		fmt.Println("MyCall failed!")
	}

	return reply
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

func SplitSlice(array []int, numberOfChunks int) [][]int {
	if len(array) == 0 {
		return nil
	}
	if numberOfChunks <= 0 {
		return nil
	}
	if numberOfChunks == 1 {
		return [][]int{array}
	}
	result := make([][]int, numberOfChunks)
	// we have more splits than elements in the input array.
	if numberOfChunks > len(array) {
		for i := 0; i < len(array); i++ {
			result[i] = []int{array[i]}
		}
		return result
	}
	for i := 0; i < numberOfChunks; i++ {
		min := (i * len(array) / numberOfChunks)
		max := ((i + 1) * len(array)) / numberOfChunks
		result[i] = array[min:max]
	}
	return result
}
