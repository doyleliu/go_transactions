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

var whoHoldsLock = make(map[string]*net.TCPConn)


var shouldGetWait = true//to lock the get operation

var shouldSetWait = true//to lock the set operation

var ClientState = 0 // The client state {0: Not in a transaction, 1: in the uncommited transaction, wait for commit or abort}

var TargetQ []string // The queue that stores the destination of each set operation

var TargeLog = make(map[string]int)


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
	index := setPort(myAddr, port)

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
		var SavedOp = make(map[string]string)
		var checkLockStatus = make(map[string]int) // check wether the lock has been hold now
		go handleRequest(tcpConn, SavedOp, port, name, checkLockStatus)

	}


}

func checkDeadlock(){

}

// start the server
func startServer(port string, name string) {
	myAddr := getIPAddr()
	// myPort := port
	// myName := name

	// currentNodeIp := myAddr[0]
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
		go handleRequest(tcpConn, SavedOp, port, name, checkLockStatus)

	}

}

// handle the request from the client
func handleRequest(tcpConn *net.TCPConn, SavedOp map[string]string, port string, name string , checkLockStatus map[string]int) {
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
			clientName := recvMsg[0]
			clientInstruction := recvMsg[1]

			msgSplit := strings.Split(clientInstruction, " ")
			//fmt.Println(msgSplit)

			switch msgSplit[0]{
			case "GET":
				go handleGet(tcpConn, msgSplit, recvMsg, name)

			case "SET":
				go handleSet(tcpConn, SavedOp, recvMsg, clientName, checkLockStatus)


			case "COMMIT":
				//fmt.Println("recvMsg[2]", recvMsg[2])
				for{
					if len(checkLockStatus) <= 0{
						break
					}
				}
				recvMsgSplit := strings.Split(recvMsg[2],".")
				fmt.Println("savedop:", SavedOp)
				//fmt.Println("SavedOP length", len(SavedOp))
				//fmt.Println("Mutex Length", len(mutexMap))
				//for k, v := range SavedOp{
				//fmt.Println("recvMsgSplit", recvMsgSplit)
				target := recvMsgSplit[1]
				val := SavedOp[target]
				StoredVal[target], _ = strconv.Atoi(val)
				//fmt.Println("Current val:", StoredVal[target])
				targetSplit := strings.Fields(target)
				//fmt.Println("Current target:", target)
				//fmt.Println("Current value:", val)
				//fmt.Println("Current target[0]:", targetSplit[0])
				//fmt.Println("Current target[0] length:", len(targetSplit[0]))
				var tmpMutex  = mutexMap[targetSplit[0]]
				fmt.Println("Mutex", tmpMutex)
				(*tmpMutex).Unlock()
				delete(whoHoldsLock, target)

				//}
				fmt.Println("Commit msg: ","COMMIT OK!" + ":" + name)
				b := []byte("COMMIT OK!" + ":" + name + ":")
				tcpConn.Write(b)

			case "ABORT":
				delete(SavedOp, clientName)
				b := []byte("ABORTED!"+ ":" + name)
				tcpConn.Write(b)

			}
		}
	}
}

//hanlde the operation of GET
func handleGet(tcpConn *net.TCPConn, msgSplit []string, recvMsg []string, name string){
	target := recvMsg[2] //

	if val, ok := StoredVal[target]; ok {
		//return the search results
		retMsg := wrapMessage(msgSplit[0], strconv.Itoa(val) + ":" + name + "." + target)
		b := []byte(retMsg)
		tcpConn.Write(b)

	}else{
		retMsg := wrapMessage(msgSplit[0], "NOT FOUND" + ":" + name + "." + target)
		b := []byte(retMsg)
		tcpConn.Write(b)
	}
}


//handle the operation of SET
func handleSet(tcpConn *net.TCPConn, SavedOp map[string]string, recvMsg []string, clientName string, checkLockStatus map[string]int){
	msgSplit := strings.Fields(recvMsg[2])
	if _, ok := mutexMap[msgSplit[0]]; !ok{
		//var tmpMutex  = sync.RWMutex{}
		checkLockStatus[msgSplit[0]] = 1
		mutexMap[msgSplit[0]] = &sync.RWMutex{}
		var tmpMutex  = mutexMap[msgSplit[0]]
		(*tmpMutex).Lock()
		delete(checkLockStatus,msgSplit[0])
		whoHoldsLock[msgSplit[0]] = tcpConn
		fmt.Println("clientName", clientName)

		fmt.Println("msgSplit[0]: ", msgSplit[0])
		fmt.Println("Lock", mutexMap[msgSplit[0]])
		//test
		//if mutexMap["X"] == mutexMap[msgSplit[0]]{
		//	fmt.Println("totally equal")
		//}
	}else {
		msgSplit := strings.Fields(recvMsg[2])

		if tcpConn == whoHoldsLock[msgSplit[0]]{
			fmt.Println("Same holds the lock")
		}else{
			checkLockStatus[msgSplit[0]] = 1
			var tmpMutex  = mutexMap[msgSplit[0]]
			fmt.Println("Locked")
			(*tmpMutex).Lock()
			delete(checkLockStatus,msgSplit[0])
		}

		//fmt.Println("msgSplit[0]: ", msgSplit[0])
		//fmt.Println("Lock", tmpMutex)
	}
	retMsg := wrapMessage("SET", "SUCCESSFUL")
	b := []byte(retMsg)
	tcpConn.Write(b)

	fmt.Println("recvMsg[2]: ", recvMsg[2])
	fmt.Println("I am here, the SaveOp length", len(SavedOp))

	//SavedOp[clientName] += recvMsg[2] + "+"
	SavedOp[msgSplit[0]] = msgSplit[1]
}

func startClient(port string, name string) {
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
			}else if len(msgSplit) > 1 && msgSplit[1] == "SET"{
				//fmt.Println("Successful Set", msgSplit[2])
				shouldSetWait = false
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

			fmt.Println("TargetLog length:",len(TargeLog))
			for k := range TargeLog{
				currentTarget := k
				delete(TargeLog, k)
				targetSplit := strings.Split(currentTarget, ".")
				dest := targetSplit[0]
				conn := CSConn[ServerName[dest]]
				sendMsg := wrapMessage(msgSplit[0], currentTarget + ".")
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
	myAddr := getIPAddr()
	retMsg :=  myAddr[0] + ":" + op + ":" + msg
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
		//AAddr := "10.195.3.50"
		AAddr := "192.168.1.6"
		APort := "9000"
		//BAddr := "10.195.3.50"
		BAddr := "192.168.1.6"
		BPort := "9090"
		//CAddr := "10.195.3.50"
		//CPort := "9100"
		//DAddr := "10.195.3.50"
		//DPort := "9190"
		//EAddr := "10.195.3.50"
		//EPort := "9200"
		//CoordAddr := "10.195.3.50"
		//CoordPort := "9290"


		Server[AAddr + ":" + APort] = "NULL"
		Server[BAddr + ":" + BPort] = "NULL"
		ServerName["A"] = AAddr + ":" + APort
		ServerName["B"] = BAddr + ":" + BPort
		if mode == "server" {
			serverCode(port, name)
		} else {
			startClient(port, name)
		}
		// n, _ := strconv.Atoi(os.Args[3])

	}
}
