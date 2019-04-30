package main

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
)

var ch = make(chan int)

var Server = make(map[string]string) // {key:"IP", value: "PORT"} IP is hard coded

var ServerName = make(map[string]string) //{key: "Name", value: "IP"}

var Client = make(map[string]string) // key:"IP", value: "PORT"} to store the current client's address

var StoredVal = make(map[string]int) // {key:"obeject", value: "value"} value stored in current server

var SavedAddr = make(map[string]string) // {key: "object", value: "map address"} to store different map address of each client

//var SavedOp = make(map[string]string) // {key:"obeject", value: "value"} Uncommited transactions in current server

var ClientSaveOP = make(map[string]int) //{key:"A.x", value: "value"} Uncommited

var CSConn = make(map[string]*net.TCPConn) // current client and server connection

var CommitMap = make(map[string]int) // store the current unresponsive server

var valMutex = sync.RWMutex{} //to lock the value when check and write

var mutexMap = make(map[string]*sync.RWMutex) // the map contains current mutex that the client holds
var mutexLockStatus = make(map[string]int)

var whoHoldsLock = make(map[string]*net.TCPConn)


var shouldGetWait = true//to lock the get operation

var shouldSetWait = true//to lock the set operation

var forceAbort = false // to check whether detection forces abort

var ClientState = 0 // The client state {0: Not in a transaction, 1: in the uncommited transaction, wait for commit or abort}

var TargetQ []string // The queue that stores the destination of each set operation

var TargeLog = make(map[string]int)

//the next part are all about the coordinator

var nodeMap = make(map[string]string) // the node connects to the next node


//to log the coordinator address and port
var CoordAddr = "10.195.3.50"
//var CoordAddr = "192.168.1.6"
var CoordPort = "6060"

var LOCALNAME = ""


func setPort(addr []string, port string) string {
	for a := range addr {
		for s := range Server {
			if (addr[a] + ":" + port) == s {
				Server[s] = port
				return s
			}
		}
	}
	return "NULL"
}

func getIPAddr() []string {
	var res []string
	adds, err := net.InterfaceAddrs()
	if err != nil {
		os.Stderr.WriteString("Oops:" + err.Error())
		os.Exit(1)
	}
	for _, a := range adds {
		if ipnet, ok := a.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
			if ipnet.IP.To4() != nil {
				res = append(res, ipnet.IP.String())
			}
		}
	}
	return res
}

// start the Coordinator
func startCoordinator(port string, name string){
	myAddr := getIPAddr()
	fmt.Println("myAddr", myAddr)
	fmt.Println("port", port)
	index := CoordAddr

	if index == "NULL" {
		fmt.Println(myAddr)
		fmt.Println("Cannot find address!")
		return
	}
	tcpAddr, _ := net.ResolveTCPAddr("tcp", index+":"+port)
	tcpListener, _ := net.ListenTCP("tcp", tcpAddr)
	for {
		tcpConn, err := tcpListener.AcceptTCP()
		defer tcpConn.Close()
		if err != nil {
			fmt.Println(err.Error())
			os.Exit(0)
		}
		strRemoteAddr := tcpConn.RemoteAddr().String()
		fmt.Println("connecting with: " + strRemoteAddr)
		//var SavedOp = make(map[string]string)
		//var checkLockStatus = make(map[string]int) // check wether the lock has been hold now
		go handleDeadLock(tcpConn,  port, name)

	}


}

func handleDeadLock(tcpConn *net.TCPConn, port string, name string){
	buff := make([]byte, 128)
	for{
		j, err := tcpConn.Read(buff)
		if err != nil && err.Error() != "EOF" {
			fmt.Println("Wrong to read the buffer ! ", err)
			ch <- 1
			break

		}
		if err == nil{
			recvMsg := string(buff[0:j])
			// show the current received message
			fmt.Println(recvMsg)
			recvMsgSplit := strings.Split(recvMsg, "\n")
			recvLen := len(recvMsgSplit)
			//fmt.Println("length: ", len(recvMsgSplit))
			cnt := 0
			for recvLen >= 2{
				msgSplit := strings.Fields(recvMsgSplit[cnt])
				OpName:= msgSplit[0]
				fmt.Println("Opname: ", OpName)
				nodeStart := msgSplit[1]
				nodeEnd := msgSplit[2]

				if OpName == "Hold"{
					nodeMap[nodeStart] = nodeEnd
				}else if OpName == "Release"{
					delete(nodeMap, nodeStart)
				}
				recvLen -= 1
				cnt += 1

			}
			fmt.Println("current nodeMap: ", nodeMap)

			if checkDAG() == false{
				fmt.Println("Deadlock Detected!")
				b := []byte("Deadlock!")
				tcpConn.Write(b)
			}else{
				fmt.Println("No Deadlock!")
				b := []byte("OK!")
				tcpConn.Write(b)
			}

		}
	}
}

func checkDAG() bool{
	for key := range nodeMap{
		var visited = make(map[string]int)
		for keyM:= range nodeMap{
			visited[keyM] = 0
		}
		currentKey := key
		for{
			if visited[currentKey] == 1{
				return false
			}

			visited[currentKey] = 1
			if val, ok := nodeMap[currentKey]; ok{
				currentKey = val
			}else{
				break
			}

		}
	}

	return true
}

// start the server
func startServer(port string, name string) {
	// connect to the coordinator first
	fmt.Println("Current Addr: " + CoordAddr + ":" + CoordPort)
	tcpAddrC, _ := net.ResolveTCPAddr("tcp", CoordAddr + ":" + CoordPort)
	connC, err := net.DialTCP("tcp", nil, tcpAddrC)
	if err != nil {
		fmt.Println("Coordinator is not starting")
		os.Exit(0)
	}
	CSConn[CoordAddr + ":" + CoordPort] = connC

	myAddr := getIPAddr()
	index := setPort(myAddr, port)

	if index == "NULL" {
		fmt.Println(myAddr)
		fmt.Println("Cannot find address!")
		return
	}

	//fmt.Println(index)
	tcpAddr, _ := net.ResolveTCPAddr("tcp", index)
	tcpListener, _ := net.ListenTCP("tcp", tcpAddr)
	for {
		tcpConn, err := tcpListener.AcceptTCP()
		defer tcpConn.Close()
		if err != nil {
			fmt.Println(err.Error())
			os.Exit(0)
		}
		strRemoteAddr := tcpConn.RemoteAddr().String()
		fmt.Println("connecting with: " + strRemoteAddr)

		// start to handle request together with uncommited operation map
		var SavedOp = make(map[string]string)
		var checkLockStatus = make(map[string]int) // check wether the lock has been hold now
		go handleRequest(tcpConn, SavedOp, port, name, checkLockStatus, connC,strRemoteAddr)

	}

}

// handle the request from the client
func handleRequest(tcpConn *net.TCPConn, SavedOp map[string]string, port string, name string , checkLockStatus map[string]int, tcpConnC *net.TCPConn, clientName string) {
	buff := make([]byte, 128)
	for{
		j, err := tcpConn.Read(buff)
		if err != nil && err.Error() != "EOF" {
			fmt.Println("Wrong to read the buffer ! ", err)
			ch <- 1
			break

		}
		//fmt.Println("I am here")

		if err == nil{
			recvMsg := dewrapMessage(string(buff[0:j]))
			// show the current received message
			fmt.Println(recvMsg)
			clientName = recvMsg[0]
			clientInstruction := recvMsg[1]

			msgSplit := strings.Split(clientInstruction, " ")
			//fmt.Println(msgSplit)

			switch msgSplit[0]{
			case "GET":
				go handleGet(tcpConn, msgSplit, recvMsg, name, checkLockStatus, SavedOp)

			case "SET":
				go handleSet(tcpConn, SavedOp, recvMsg, clientName, checkLockStatus , name, tcpConnC)


			case "COMMIT":
				go handleCommit(tcpConn, SavedOp, recvMsg, clientName, checkLockStatus , name, tcpConnC)
				//fmt.Println("recvMsg[2]", recvMsg[2])
				//for{
				//	if len(checkLockStatus) <= 0{
				//		break
				//	}
				//}
				//recvMsgSplit := strings.Split(recvMsg[2],".")
				//fmt.Println("savedop:", SavedOp)
				////fmt.Println("SavedOP length", len(SavedOp))
				////fmt.Println("Mutex Length", len(mutexMap))
				////for k, v := range SavedOp{
				////fmt.Println("recvMsgSplit", recvMsgSplit)
				//target := recvMsgSplit[1]
				//val := SavedOp[target]
				//StoredVal[target], _ = strconv.Atoi(val)
				////fmt.Println("Current val:", StoredVal[target])
				//targetSplit := strings.Fields(target)
				////fmt.Println("Current target:", target)
				////fmt.Println("Current value:", val)
				////fmt.Println("Current target[0]:", targetSplit[0])
				////fmt.Println("Current target[0] length:", len(targetSplit[0]))
				//var tmpMutex  = mutexMap[targetSplit[0]]
				//fmt.Println("Unlock Mutex", tmpMutex)
				//bC := []byte("Release "+ name +"." +  targetSplit[0] + " " + clientName + "\n")
				//tcpConnC.Write(bC)
				//(*tmpMutex).Unlock()
				//delete(whoHoldsLock, target)
				//
				//
				//fmt.Println("Commit msg: ","COMMIT OK!" + ":" + name)
				//b := []byte("COMMIT OK!" + ":" + name + ":")
				//tcpConn.Write(b)

			case "ABORT":
				fmt.Println("whoHoldsLock: ",whoHoldsLock )
				fmt.Println("mutexMap: ", mutexMap)
				fmt.Println("ABORT Savedop:", SavedOp)

				for k, v := range whoHoldsLock{
					if v == tcpConn{
						fmt.Println("key", k)
						var tmpMutex  = mutexMap[k]
						fmt.Println("Unlock Mutex", tmpMutex)
						bC := []byte("Release "+ name +"." +  k + " " + clientName + "\n")

						tcpConnC.Write(bC)
						if mutexLockStatus[k] == 1{
							mutexLockStatus[k] = 0
							(*tmpMutex).Unlock()
						}


						delete(whoHoldsLock, k)
					}

				}
				for k := range SavedOp {
					delete(SavedOp, k)
				}
				b := []byte("ABORTED!"+ ":" + name)
				tcpConn.Write(b)


			}
		}
	}
}

//handle the operation of Commit
func handleCommit(tcpConn *net.TCPConn, SavedOp map[string]string, recvMsg []string, clientName string, checkLockStatus map[string]int, name string, tcpConnC *net.TCPConn){
	for{
		if len(checkLockStatus) <= 0{
			break
		}
	}
	//recvMsgSplit := strings.Split(recvMsg[2],".")
	//fmt.Println("savedop:", SavedOp)
	for target, val := range SavedOp{
		//target := recvMsgSplit[1]
		//val := SavedOp[target]
		StoredVal[target], _ = strconv.Atoi(val)
		//fmt.Println("Current val:", StoredVal[target])
		targetSplit := strings.Fields(target)
		var tmpMutex  = mutexMap[targetSplit[0]]
		fmt.Println("Unlock Mutex", tmpMutex)
		bC := []byte("Release "+ name +"." +  targetSplit[0] + " " + clientName + "\n")
		tcpConnC.Write(bC)
		delete(SavedOp, target)
		if mutexLockStatus[targetSplit[0]] == 1{
			mutexLockStatus[targetSplit[0]] = 0
			(*tmpMutex).Unlock()

		}

		delete(checkLockStatus, targetSplit[0])
		delete(whoHoldsLock, target)


		fmt.Println("Commit msg: ","COMMIT OK!" + ":" + targetSplit[0])
		b := []byte("COMMIT OK!" + ":" + name + ":")
		tcpConn.Write(b)
	}

}

//hanlde the operation of GET
func handleGet(tcpConn *net.TCPConn, msgSplit []string, recvMsg []string, name string , checkLockStatus map[string]int,  SavedOp map[string]string){
	target := recvMsg[2] //

	if _, ok := StoredVal[target]; ok {
		//return the search results
		checkLockStatus[msgSplit[0]] = 1
		mutexMap[msgSplit[0]] = &sync.RWMutex{}
		var tmpMutex  = mutexMap[target]
		(*tmpMutex).RLock()
		delete(checkLockStatus,msgSplit[0])
		//whoHoldsLock[msgSplit[0]] = tcpConn
		retMsg := wrapMessage(msgSplit[0], strconv.Itoa(StoredVal[target]) + ":" + name + "." + target)
		b := []byte(retMsg)
		tcpConn.Write(b)
		delete(whoHoldsLock, target)
		(*tmpMutex).RUnlock()


	}else if val, ok := SavedOp[target];ok{
		checkLockStatus[msgSplit[0]] = 1
		var tmpMutex  = mutexMap[msgSplit[0]]
		fmt.Println("Locked")
		(*tmpMutex).RLock()
		delete(checkLockStatus,msgSplit[0])
		//whoHoldsLock[msgSplit[0]] = tcpConn
		retMsg := wrapMessage(msgSplit[0], val + ":" + name + "." + target)
		b := []byte(retMsg)
		tcpConn.Write(b)
		delete(whoHoldsLock, target)
		(*tmpMutex).RUnlock()


	}else{
		//fmt.Println("target", target)
		checkLockStatus[target] = 1
		var tmpMutex  = mutexMap[target]
		fmt.Println("LockName", mutexMap[target])
		fmt.Println("Locked")

		if tmpMutex != nil {
			fmt.Println("Here!")
			(*tmpMutex).RLock()
			delete(checkLockStatus, target)
			if _,ok := StoredVal[target]; ok{
				retMsg := wrapMessage(msgSplit[0], strconv.Itoa(StoredVal[target]) + ":" + name + "." + target)
				b := []byte(retMsg)
				tcpConn.Write(b)
			}else{
				retMsg := wrapMessage(msgSplit[0], "NOT FOUND" + ":" + name + "." + target)
				b := []byte(retMsg)
				tcpConn.Write(b)
			}

			delete(whoHoldsLock, target)
			(*tmpMutex).RUnlock()
		}else{
			delete(checkLockStatus, target)
			retMsg := wrapMessage(msgSplit[0], "NOT FOUND" + ":" + name + "." + target)
			b := []byte(retMsg)
			tcpConn.Write(b)
		}



	}
}


//handle the operation of SET
func handleSet(tcpConn *net.TCPConn, SavedOp map[string]string, recvMsg []string, clientName string, checkLockStatus map[string]int, name string, tcpConnC *net.TCPConn){
	msgSplit := strings.Fields(recvMsg[2])
	if _, ok := mutexMap[msgSplit[0]]; !ok{
		//var tmpMutex  = sync.RWMutex{}
		checkLockStatus[msgSplit[0]] = 1
		mutexMap[msgSplit[0]] = &sync.RWMutex{}
		var tmpMutex  = mutexMap[msgSplit[0]]
		(*tmpMutex).Lock()
		mutexLockStatus[msgSplit[0]] = 1
		//fmt.Println("name:", name)
		//fmt.Println("recvMsg",   recvMsg)
		//fmt.Println("Hold "+ name +"." +  msgSplit[0] + " " + clientName)
		bC := []byte("Hold "+ name +"." +  msgSplit[0] + " " + clientName + "\n")
		tcpConnC.Write(bC)
		var buffC = make([]byte, 128)
		for{
			j, err := tcpConnC.Read(buffC)
			if err != nil && err.Error() != "EOF" {
				fmt.Println("Wrong to read the buffer ! ", err)
				ch <- 1
				break

			}
			if err == nil{
				recvMsgC := string(buffC[0:j])
				fmt.Println("recvMsgC", recvMsgC)
				break
			}
		}

		delete(checkLockStatus,msgSplit[0])
		whoHoldsLock[msgSplit[0]] = tcpConn
		fmt.Println("clientName", clientName)

		fmt.Println("msgSplit[0]: ", msgSplit[0])
		fmt.Println("Lock", mutexMap[msgSplit[0]])

	}else {
		msgSplit := strings.Fields(recvMsg[2])
		//fmt.Println("tcpConn: ", tcpConn)
		//fmt.Println("whoHoldsLock: ", whoHoldsLock)
		if tcpConn == whoHoldsLock[msgSplit[0]]{
			fmt.Println("Same holds the lock")
		}else{
			//fmt.Println("checkLockStatus", checkLockStatus)
			checkLockStatus[msgSplit[0]] = 1
			var tmpMutex  = mutexMap[msgSplit[0]]

			fmt.Println("Locked")
			bC := []byte("Hold "+  clientName + " " + name +"." +  msgSplit[0]+ "\n")
			// to judge whether can acquire the lock without deadlock

			tcpConnC.Write(bC)
			var buffC = make([]byte, 128)
			for{
				j, err := tcpConnC.Read(buffC)
				if err != nil && err.Error() != "EOF" {
					fmt.Println("Wrong to read the buffer ! ", err)
					ch <- 1
					break

				}
				if err == nil{
					recvMsgC := string(buffC[0:j])
					fmt.Println("recvMsgC", recvMsgC)
					if recvMsgC == "Deadlock!"{
						fmt.Println("here in deadlock ", whoHoldsLock)
						bC := []byte("Release "+ name +"." +  msgSplit[0] + " " + clientName + "\n")
						tcpConnC.Write(bC)
						//for k, v := range whoHoldsLock{
						//	if v == tcpConn{
						//		fmt.Println("key", k)
						//		var tmpMutex  = mutexMap[k]
						//		fmt.Println("Unlock Mutex", tmpMutex)
						//		bC := []byte("Release "+ name +"." +  k + " " + clientName + "\n")
						//		tcpConnC.Write(bC)
						//		(*tmpMutex).Unlock()
						//
						//		delete(whoHoldsLock, k)
						//	}
						//
						//}
						//for k := range SavedOp {
						//	delete(SavedOp, k)
						//}


						b := []byte("ABORTED"+ ":" + name)
						tcpConn.Write(b)
						return

					} else if recvMsgC == "OK!"{
						break
					}
				}
			}
			(*tmpMutex).Lock()
			mutexLockStatus[msgSplit[0]] = 1
			bC = []byte("Release "+  clientName + " " + name +"." +  msgSplit[0]+ "\n")
			tcpConnC.Write(bC)
			bC = []byte("Hold "+  name +"." +  msgSplit[0] + " " + clientName + "\n")
			tcpConnC.Write(bC)
			delete(checkLockStatus,msgSplit[0])
			whoHoldsLock[msgSplit[0]] = tcpConn
		}

		//fmt.Println("msgSplit[0]: ", msgSplit[0])
		//fmt.Println("Lock", tmpMutex)
	}
	retMsg := wrapMessage("SET", "SUCCESSFUL")
	b := []byte(retMsg)
	tcpConn.Write(b)

	fmt.Println("recvMsg[2]: ", recvMsg[2])
	fmt.Println("msgSplit:", msgSplit)
	//fmt.Println("I am here, the SaveOp length", len(SavedOp))

	//SavedOp[clientName] += recvMsg[2] + "+"
	SavedOp[msgSplit[0]] = msgSplit[1]
}

func startClient(port string, name string) {
	// connect to the coordinator first
	fmt.Println("Current Addr: " + CoordAddr + ":" + CoordPort)
	tcpAddr, _ := net.ResolveTCPAddr("tcp", CoordAddr + ":" + CoordPort)
	connC, err := net.DialTCP("tcp", nil, tcpAddr)
	if err != nil {
		fmt.Println("Coordinator is not starting")
		os.Exit(0)
	}
	CSConn[CoordAddr + ":" + CoordPort] = connC
	// then connect to the server second
	fmt.Println("Server: ", len(Server))
	for ADDR := range Server {
		fmt.Println("Current Addr: " + ADDR)

		//ADDRSplit := strings.Split(ADDR, ":")
		//fmt.Println(ADDRSplit[0] + ":"+ port)
		//tcpAddr, _ := net.ResolveTCPAddr("tcp", ADDR+":"+port)
		tcpAddr, _ := net.ResolveTCPAddr("tcp", ADDR)
		conn, err := net.DialTCP("tcp", nil, tcpAddr)
		if err != nil {
			fmt.Println("Server is not starting")
			os.Exit(0)
		}
		CSConn[ADDR] = conn
		go handleFeedback(conn)

	}
	fmt.Println("CsConn: ", CSConn)
	// go doTask()
	doTask()

}

func handleFeedback(tcpConn *net.TCPConn){
	buff := make([]byte, 128)
	for{
		j, err := tcpConn.Read(buff)
		if err != nil && err.Error() != "EOF" {
			fmt.Println("Wrong to read the buffer ! ", err)
			ch <- 1
			break

		}
		if err == nil{
			recvMsg := string(buff[0:j])
			//fmt.Println("recvMsg",recvMsg)
			msgSplit := strings.Split(recvMsg, ":")
			if len(msgSplit) > 1 && msgSplit[1] == "GET"{
				port := msgSplit[3]
				value := msgSplit[2]
				if value == "NOT FOUND" {
					// need to abort the transaction
					fmt.Println(value)
					for len(TargetQ) > 0{
						currentTarget := TargetQ[0]
						TargetQ = TargetQ[1:]
						targetSplit := strings.Split(currentTarget, ".")
						dest := targetSplit[0]
						conn := CSConn[ServerName[dest]]
						sendMsg := wrapMessage(msgSplit[0], currentTarget)
						//sendMsg := "ABORT"
						b := []byte(sendMsg)
						conn.Write(b)
					}

					for k := range ClientSaveOP {
						delete(ClientSaveOP, k)
					}
					ClientState = 0
					shouldGetWait = false
				}else{
					fmt.Println(port + " = " + value)
					shouldGetWait = false
				}
			}else if msgSplit[0] == "ABORTED"{
				fmt.Println("here msg: ", msgSplit)
				forceAbort = true
			} else if len(msgSplit) > 1 && msgSplit[1] == "SET"{
				//fmt.Println("Successful Set", msgSplit[2])
				shouldSetWait = false
				fmt.Println("OK")
			} else{
				//fmt.Println("recvMsg",recvMsg)
				//fmt.Println("msgSplit[1]", msgSplit[1])
				//fmt.Println("Current length:", len(CommitMap))
				prevLen := len(CommitMap)
				delete(CommitMap, msgSplit[1])
				if prevLen!= len(CommitMap) && len(CommitMap) <= 0 {
					fmt.Println(msgSplit[0])
				}

			}

		}
	}
}

//to deal with the user's input and instructions
func doTask() {
	//var currentTarget = ""// The target of last set

	for {
		var msg string
		in := bufio.NewReader(os.Stdin)
		msg, err := in.ReadString('\n')

		if err != nil {
			fmt.Println("Input reading error!")
			os.Exit(0)
		}

		msgSplit := strings.Fields(msg)
		//fmt.Println(msgSplit)
		switch msgSplit[0] {
		case "BEGIN\n":
			if ClientState!= 0{
				continue
			}
			fmt.Println("OK")
			ClientState = 1
		case "BEGIN":
			if ClientState!= 0{
				continue
			}
			fmt.Println("OK")
			ClientState = 1
		case "SET":
			if ClientState != 1{
				continue
			}
			// set server.key value
			target := msgSplit[1]
			targetSplit := strings.Split(target, ".")
			dest := targetSplit[0]
			para := targetSplit[1]
			val := msgSplit[2]
			//fmt.Println("Server: ",CSConn[ServerName[dest]])
			conn := CSConn[ServerName[dest]]

			sendMsg := wrapMessage(msgSplit[0], para + " " + val )
			//fmt.Println("sendMsg val:", sendMsg)
			b := []byte(sendMsg)
			//fmt.Println("b val: ", b)
			//fmt.Println("conn", conn)
			shouldSetWait = true
			_, _ = conn.Write(b)
			ClientSaveOP[target], _ = strconv.Atoi(val)
			//currentTarget = target
			//fmt.Println("target", target)
			//TargetQ = append(TargetQ, target)
			TargeLog[target] = 1

			//fmt.Println("target", target)
			CommitMap[dest] = 1

			for{
				if shouldSetWait == false{
					break
				}
				if forceAbort == true{
					forceAbort = false
					delete(ClientSaveOP, target)
					for k := range TargeLog{
						currentTarget := k
						delete(TargeLog, k)
						targetSplit := strings.Split(currentTarget, ".")
						dest := targetSplit[0]
						conn := CSConn[ServerName[dest]]
						sendMsg := wrapMessage("ABORT", currentTarget)
						//sendMsg := "ABORT"
						b := []byte(sendMsg)
						conn.Write(b)
					}

					for k := range ClientSaveOP {
						delete(ClientSaveOP, k)
					}

					ClientState = 0
					break
				}
			}

		case "GET":
			// get server.key
			target := msgSplit[1]
			if val, ok := ClientSaveOP[target]; ok{
				fmt.Println(target + " = " + strconv.Itoa(val))
			}else {
				targetSplit := strings.Split(target, ".")
				dest := targetSplit[0]
				para := targetSplit[1]
				conn := CSConn[ServerName[dest]]

				sendMsg := wrapMessage(msgSplit[0], para )
				b := []byte(sendMsg)
				shouldGetWait = true
				conn.Write(b)
				//to wait until that get value has been updated
				for{
					if shouldGetWait == false{
						break
					}
				}


			}
		case "COMMIT":
			if ClientState != 1{
				continue
			}
			//fmt.Println("Current Target: ", currentTarget)
			//for len(TargeLog) > 0{
			//	currentTarget := TargetQ[0]
			//	TargetQ = TargetQ[1:]
			//	targetSplit := strings.Split(currentTarget, ".")
			//	dest := targetSplit[0]
			//	conn := CSConn[ServerName[dest]]
			//	sendMsg := wrapMessage(msgSplit[0], currentTarget + ".")
			//	b := []byte(sendMsg)
			//	conn.Write(b)
			//}

			//fmt.Println("TargetLog length:",len(TargeLog))
			if len(TargeLog) == 0{
				fmt.Println("COMMIT OK!")
			}
			//fmt.Println("TargetLog", TargeLog)
			for k := range TargeLog{
				currentTarget := k
				delete(TargeLog, k)
				targetSplit := strings.Split(currentTarget, ".")
				dest := targetSplit[0]
				conn := CSConn[ServerName[dest]]
				sendMsg := wrapMessage(msgSplit[0], currentTarget + ".")
				//fmt.Println("sendMsg:", sendMsg)
				b := []byte(sendMsg)
				conn.Write(b)
			}
			//problems may happen here! To clear the temporary number
			for k := range ClientSaveOP {
				delete(ClientSaveOP, k)
			}
			ClientState = 0

		case "ABORT":
			if ClientState != 1{
				continue
			}
			//for len(TargetQ) > 0{
			//	currentTarget := TargetQ[0]
			//	TargetQ = TargetQ[1:]
			//	targetSplit := strings.Split(currentTarget, ".")
			//	dest := targetSplit[0]
			//	conn := CSConn[ServerName[dest]]
			//	sendMsg := wrapMessage(msgSplit[0], currentTarget)
			//	//sendMsg := "ABORT"
			//	b := []byte(sendMsg)
			//	conn.Write(b)
			//}

			if len(TargeLog) == 0{
				fmt.Println("ABORTED!")
			}

			for k := range TargeLog{
				currentTarget := k
				delete(TargeLog, k)
				targetSplit := strings.Split(currentTarget, ".")
				dest := targetSplit[0]
				conn := CSConn[ServerName[dest]]
				sendMsg := wrapMessage(msgSplit[0], currentTarget)
				//sendMsg := "ABORT"
				b := []byte(sendMsg)
				conn.Write(b)
			}

			for k := range ClientSaveOP {
				delete(ClientSaveOP, k)
			}

			ClientState = 0

		}
		// fmt.Println(msg)

	}
}


// to change the input message to the version that can be processed
func wrapMessage(op string, msg string) string {
	//msg (From) GET/SET/COMMIT/ABORT A.x (val)
	//myAddr := getIPAddr()
	retMsg :=  LOCALNAME + ":" + op + ":" + msg
	return retMsg
}

func dewrapMessage(msg string) []string{
	msgSplit := strings.Split(msg, ":")
	//fmt.Println("Current splited message", msgSplit)
	var result []string
	result = make([]string, 3)
	result[0] = msgSplit[0]
	result[1] = msgSplit[1]
	if len(msgSplit) > 2 {
		result[2] = msgSplit[2]
	}

	return result

}


//send the msg from client to serever
// func sendRequest(tcpConn *net.TCPConn) {

// }

func serverCode(port string, name string) {
	startServer(port, name)
}

func main() {
	if len(os.Args) < 3 {
		fmt.Println("Usage: ./mp3 coordinator/server/client name port")
		return
	} else if len(os.Args) > 4 {
		fmt.Println("Usage: ./mp3 coordinator/server/client name port")
		return
	} else {
		mode := os.Args[1]
		name := os.Args[2]
		port := os.Args[3]

		// hard-coded server address
		AAddr := "10.195.3.50"
		//AAddr := "192.168.1.6"
		APort := "9000"
		BAddr := "10.195.3.50"
		//BAddr := "192.168.1.6"
		BPort := "9090"
		//CAddr := "10.195.3.50"
		//CPort := "9100"
		//DAddr := "10.195.3.50"
		//DPort := "9190"
		//EAddr := "10.195.3.50"
		//EPort := "9200"

		Server[AAddr + ":" + APort] = "NULL"
		Server[BAddr + ":" + BPort] = "NULL"
		//Server[CAddr + ":" + CPort] = "NULL"
		//Server[DAddr + ":" + DPort] = "NULL"
		//Server[EAddr + ":" + EPort] = "NULL"
		//Server[CoordAddr + ":" + CoordPort] = "NULL"

		ServerName["A"] = AAddr + ":" + APort
		ServerName["B"] = BAddr + ":" + BPort
		//ServerName["C"] = CAddr + ":" + CPort
		//ServerName["D"] = DAddr + ":" + DPort
		//ServerName["E"] = EAddr + ":" + EPort
		//ServerName["Coord"] = CoordAddr + ":" + CoordPort
		LOCALNAME = name
		if mode == "server" {
			serverCode(port, name)
		}else if mode == "coordinator"{
			startCoordinator(port, name)
		} else {
			startClient(port, name)
		}
		// n, _ := strconv.Atoi(os.Args[3])

	}
}
