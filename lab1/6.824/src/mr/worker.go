package mr

import (
	"regexp"
	"path/filepath"
	"time"
	"strconv"
	"encoding/json"
	"fmt"
	"log"
	"net/rpc"
	"hash/fnv"
	"os"
	"io/ioutil"
	"sort"
)
//
// Map functions return a slice of KeyValue.
//
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

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	// Your worker implementation here.
	WAITING_TIME := time.Second
	machineId := 0
	for true {
		reply := getTask(machineId)
		fileName := reply.Filename
		id := reply.TaskId
		nReduce := reply.NReduce
		machineId = reply.MachineId
		if reply.Finish{
			break
		}
		if reply.IsMapTask {
			// if the assigned task is map
			// count and sort
			kva := processFile(fileName, mapf)
			storeIntermediate(kva, id, nReduce, machineId)
			sendFinishMessage(true, id)
		}else if reply.Waiting{
			// if master not done yet, but no more tasks are needed to do
			// just sleep for 10 seconds, in case some other processor failed
			//fmt.Println("Tasks are not finished but are processing by others, waiting for 10 sec")
			time.Sleep(WAITING_TIME)
		}else {
			// do the reduce logic
			processJsonFile(fileName, id, reducef)
			sendFinishMessage(false, id)
		}
		time.Sleep(WAITING_TIME)
	}
	//fmt.Println("Worker finish all the task, exiting now......")
}

// workers periodically ask the master for work
// sleeping with time.Sleep() between each request.

func storeIntermediate(kva []KeyValue, mapId int, nReduce int, machineId int){
	intermediate := make(map[int][]KeyValue)
	for _, kv := range kva {
		k := kv.Key
		index := ihash(k)%nReduce
		intermediate[index] = append(intermediate[index], kv)
	}

	for i:=0;i<nReduce;i++ {
		kvp := intermediate[i]
		fileName := "mr-"+strconv.Itoa(machineId)+"-"+strconv.Itoa(i)+".json"
		file, err :=  os.OpenFile(fileName, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			fmt.Println(err)
			return
		}
		enc := json.NewEncoder(file)
		for _, kv := range kvp {
			//fmt.Println(ihash(kv.Key))
			err := enc.Encode(&kv)
			if err != nil {
				fmt.Println(err)
				return
			}
		}
		file.Close()
	}
}

func sendFinishMessage(isMap bool, index int) {
	args := Application{}
	args.IsMap = isMap
	args.Index = index
	reply := Response{}
	call("Master.GetTaskConfirmation", &args, &reply)

	//fmt.Println("Master has confirmed that task has been finished "+reply.Filename)
}

/**
asking the master for task
*/
func getTask(machineId int) Response{
	args := Application{}
	args.MachineId = machineId
	reply := Response{}
	call("Master.SendFileName", &args, &reply)
	//fmt.Println("Master reply with the file name "+reply.Filename)
	return reply
}

/**
do the map logic
return key value pair array
*/
func processFile(filename string, mapf func(string, string) []KeyValue) []KeyValue {
	//fmt.Println("Start processing file "+filename)

	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	// count the words using the plugin
	kva := mapf(filename, string(content))

	// sort the array by key value
	sort.Sort(ByKey(kva))
	return kva
}

func readJsonFile(fileName string) []KeyValue{
	file, err := os.Open(fileName)
	if err != nil {
		fmt.Println(err)
		return nil
	}
	dec := json.NewDecoder(file)
	kva := []KeyValue{}
	for {
	  var kv KeyValue
	  if err := dec.Decode(&kv); err != nil {
		break
	  }
	  kva = append(kva, kv)
	}
	return kva
}


func GetAllFiles(ext string) []string {
	pathS, err := os.Getwd()
	if err != nil {
		panic(err)
	}
	var files []string
	filepath.Walk(pathS, func(path string, f os.FileInfo, _ error) error {
		if !f.IsDir() {
			r, err := regexp.MatchString(ext, f.Name())
			if err == nil && r {
				files = append(files, f.Name())
			}
		}
		return nil
	})
	return files
}
func processJsonFile(filename string, index int, reducef func(string, []string) string) {
	kva := []KeyValue{}
	pattern := "mr-[1-9]-"+strconv.Itoa(index)
	files := GetAllFiles(pattern)
	fmt.Println("Processing "+strconv.Itoa(index)+" reduce task")
	for _, file:= range files {
		kvp := readJsonFile(file)
		//fmt.Println(file)
		kva = append(kva, kvp...)
	}
	sort.Sort(ByKey(kva))
	// sort the key value pair again and then accumulate
	oname := "mr-out-"+strconv.Itoa(index)
	ofile, _ := os.Create(oname)

	// call Reduce on each distinct key in intermediate[],
	// and print the result to mr-out-0.
	i := 0
	for i < len(kva) {
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}
		output := reducef(kva[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", kva[i].Key, output)

		i = j
	}
	for _, file := range files{
		os.Remove(file)
	}

	ofile.Close()
}



//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
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
