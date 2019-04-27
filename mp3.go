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

var valMutex = sync.RWMutex{} //to lock the value when check and write

var ClientState = 0 // The client state {0: Not in a transaction, 1: in the uncommited transaction, wait for commit or abort}


func setPort(addr []string, port string) string {
	for a := range addr {
		for s := range Server {
			if addr[a] == s {
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
		go handleRequest(tcpConn, SavedOp)

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

		// start to handle request together with uncommited operation map
		var SavedOp = make(map[string]string)
		go handleRequest(tcpConn, SavedOp)

	}

}

// handle the request from the client
func handleRequest(tcpConn *net.TCPConn, SavedOp map[string]string) {
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

				target := recvMsg[2] //
				if val, ok := StoredVal[target]; ok {
					//return the search results
					retMsg := wrapMessage(msgSplit[0], strconv.Itoa(val))
					b := []byte(retMsg)
					tcpConn.Write(b)

				}else{
					retMsg := wrapMessage(msgSplit[0], "NOT FOUND")
					b := []byte(retMsg)
					tcpConn.Write(b)
				}
			case "SET":
				valMutex.Lock()
				fmt.Println("I am here")
				//target := msgSplit[1]
				//val := msgSplit[2]
				//if _, ok := StoredVal[target]; ok{
				//	StoredVal[target] = strconv.Atoi(val)
				//}
				//fmt.Println("recvMsg[2] :", recvMsg[2])
				//fmt.Println("recvMsg[2] Length :", len(recvMsg[2]))
				SavedOp[clientName] += recvMsg[2] + "+"
				valMutex.Unlock()


			case "COMMIT":
				valMutex.Lock()
				fmt.Println("savedop length", len(SavedOp[clientName]))
				if _, ok := SavedOp[clientName]; ok{

					instructionSplit := strings.Split(SavedOp[clientName], "+")
					for index := range instructionSplit{
						msgsplitC := strings.Fields(instructionSplit[index])
						if len(msgsplitC) > 1{
							target := msgsplitC[0]
							val := msgsplitC[1]
							StoredVal[target], _ = strconv.Atoi(val)
							print("Current val:", StoredVal[target])

						}
					}
					b := []byte("COMMIT OK!")
					tcpConn.Write(b)

					//msgsplitC := strings.Fields(instruction)
					////fmt.Println("msg split[0]: ", msgsplitC[0])
					//target := msgsplitC[0]
					//val := msgsplitC[1]
					//StoredVal[target], _ = strconv.Atoi(val)
					//print("Current val:", StoredVal[target])
					//b := []byte("COMMIT OK!")
					//tcpConn.Write(b)
				}
				valMutex.Unlock()

			case "ABORT":
				delete(SavedOp, clientName)
				b := []byte("ABORTED!")
				tcpConn.Write(b)

			}
		}
	}
}

func startClient(port string, name string) {
	fmt.Println("Server: ", len(Server))
	for ADDR := range Server {
		fmt.Println("Current Addr: " + ADDR)
		tcpAddr, _ := net.ResolveTCPAddr("tcp", ADDR+":"+port)
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
			fmt.Println(recvMsg)
		}
	}
}

//to deal with the user's input and instructions
func doTask() {
	var currentTarget = ""// The target of last set
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
			_, _ = conn.Write(b)
			ClientSaveOP[target], _ = strconv.Atoi(val)
			currentTarget = target

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
				conn.Write(b)

			}
		case "COMMIT":
			if ClientState != 1{
				continue
			}
			//fmt.Println("Current Target: ", currentTarget)
			targetSplit := strings.Split(currentTarget, ".")
			dest := targetSplit[0]
			conn := CSConn[ServerName[dest]]
			sendMsg := wrapMessage(msgSplit[0], currentTarget)
			b := []byte(sendMsg)
			conn.Write(b)

			//problems may happen here! To clear the temporary number
			for k := range ClientSaveOP {
				delete(ClientSaveOP, k)
			}
			ClientState = 0

		case "ABORT":
			if ClientState != 1{
				continue
			}
			targetSplit := strings.Split(currentTarget, ".")
			dest := targetSplit[0]
			conn := CSConn[ServerName[dest]]
			sendMsg := wrapMessage(msgSplit[0], currentTarget)
			//sendMsg := "ABORT"
			b := []byte(sendMsg)
			conn.Write(b)
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
	// remote server
	// Server["172.22.158.185"] = "NULL"

	// local server
	// Server["10.195.3.50"] = "NULL"
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
		Server["192.168.1.6"] = "NULL"
		ServerName["A"] = "192.168.1.6"
		if mode == "server" {
			serverCode(port, name)
		} else {
			startClient(port, name)
		}
		// n, _ := strconv.Atoi(os.Args[3])

	}
}
