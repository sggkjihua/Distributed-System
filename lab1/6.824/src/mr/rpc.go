package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"


// Add your RPC definitions here.

type Application struct {
	Index int
	IsMap bool
	MachineId int
}

type Response struct {
	Filename string
	IsMapTask bool
	TaskId int
	Finish bool
	Waiting bool
	NReduce int
	MachineId int
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
