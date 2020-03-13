package mr

import (
	"strconv"
	"sync"
	"log"
	"net"
	"os"
	"net/rpc"
	"net/http"
	"time"
)


type Master struct {
	// Your definitions here.
	files []string
	nReduce int

	mapFinished []bool
	mapProcessing [] bool
	numOfFinishedMap int

	reduceFinished []bool
	reduceProcessing [] bool
	numOfFinishedReduce int
	
	lock sync.Mutex
	exit bool

	ids int
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
// Example
// the RPC argument and reply types are defined in rpc.go.

func (m *Master) SendFileName(args *Application, reply *Response) error {
	// lock this assign function such that one thread allowed each time
	m.lock.Lock()
	defer m.lock.Unlock()
	assigned := false
	if m.numOfFinishedMap < len(m.files) {
		for index, file := range m.files{
			if m.mapFinished[index] || m.mapProcessing[index] {
				continue
			}
			reply.Filename = file
			reply.TaskId = index
			reply.IsMapTask = true	
			reply.NReduce = m.nReduce
			m.mapProcessing[index] = true	
			// set the timer such that we will set the task to not processing
			// if we do not receive any confirmation from the worker
			go setTimer(index, m.mapProcessing, m.mapFinished)

			if args.MachineId == 0 {
				// this is used for storing the intermediate as mr-machineId-reduceId
				m.ids ++
				reply.MachineId = m.ids
			}else{
				reply.MachineId = args.MachineId
			}
			assigned = true
			break	
		}
	}else{
		// here to implement the reduce logic
		for i := 0; i < m.nReduce; i++ {
			if m.reduceFinished[i] || m.reduceProcessing[i] {
				continue
			}
			reply.Filename = "reduce-"+ strconv.Itoa(i)+".json"
			reply.TaskId = i
			reply.IsMapTask = false	
			m.reduceProcessing[i] = true
			go setTimer(i, m.reduceProcessing, m.reduceFinished)
			assigned = true
			break		
		}
	}
	if !assigned {
		if m.numOfFinishedMap == len(m.files) && m.numOfFinishedReduce == m.nReduce {
			// everything is done, worker could exit now
			reply.Finish = true
			m.exit = true
		}else {
			// there are still work ongoing, may be reassign the work 
			reply.Waiting = true			
		}
	}
	return nil


}

func setTimer(index int, processing []bool, finished []bool) {
	time.Sleep(10*time.Second)
	// we actually could add the lock here
	if !finished[index] {
		processing[index] = false
	}
}

func (m *Master) setTaskFinished(finished []bool, index int, number *int){
	m.lock.Lock()
	defer m.lock.Unlock()
	if !finished[index] {
		finished[index] = true
		*number++
	}
}


func (m *Master) GetTaskConfirmation(args *Application, reply *Response) error{
	isMap := args.IsMap
	index := args.Index
	if isMap {
		m.setTaskFinished(m.mapFinished, index, &m.numOfFinishedMap)
	} else {
		m.setTaskFinished(m.reduceFinished, index, &m.numOfFinishedReduce)
	}
	return nil
}

// start a thread that listens for RPCs from worker.go
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	ret := m.exit
	// Your code here.
	if ret {
		files := GetAllFiles("[*].json")
		for _, file := range files {
			os.Remove(file)
		}
	}

	return ret
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}
	// Your code here.
	m.files = files
	m.nReduce = nReduce
	m.mapFinished = make([]bool, len(files))
	m.mapProcessing = make([]bool, len(files))
	m.numOfFinishedMap = 0;

	m.reduceFinished = make([]bool,nReduce)
	m.reduceProcessing = make([]bool, nReduce)
	m.numOfFinishedReduce = 0;
	m.lock = sync.Mutex{}
	m.server()
	return &m
}
