package main

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"strings"
)

var Server = make(map[string]string) // {key:"IP", value: "PORT"} IP is hard coded

var Client = make(map[string]string) // key:"IP", value: "PORT"} to store the current client's address

var StoredVal = make(map[string]int) // {key:"obeject", value: "value"} value stored in current server

var CSConn = make(map[string]*net.TCPConn) // current client and server connection

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
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		os.Stderr.WriteString("Oops:" + err.Error())
		os.Exit(1)
	}
	for _, a := range addrs {
		if ipnet, ok := a.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
			if ipnet.IP.To4() != nil {
				res = append(res, ipnet.IP.String())
			}
		}
	}
	return res
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
		// go handleRequest

	}

}

// handle the request from the client
func handleRequest(tcpConn *net.TCPConn) {

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
		// go sendRequest(conn)

	}
	// go doTask()
	doTask()
}

//to deal with the user's input and instructions
func doTask() {
	for {
		var msg string

		in := bufio.NewReader(os.Stdin)
		msg, err := in.ReadString('\n')

		if err != nil {
			fmt.Println("Input reading error!")
			os.Exit(0)
		}

		msgSplit := strings.Split(msg, " ")
		switch msgSplit[0] {
		case "SET":
			// set server.key value
		case "GET":
			// get server.key
		}
		// fmt.Println(msg)

	}
}

func wrapMessge(msg string) string {

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
		fmt.Println("Usage: ./mp1 server/client name port")
		return
	} else if len(os.Args) > 4 {
		fmt.Println("Usage: ./mp1 server/client name port")
		return
	} else {
		mode := os.Args[1]
		name := os.Args[2]
		port := os.Args[3]
		Server["10.195.3.50"] = "NULL"
		if mode == "server" {
			serverCode(port, name)
		} else {
			startClient(port, name)
		}
		// n, _ := strconv.Atoi(os.Args[3])

	}
}
