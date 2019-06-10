# go_transactions
## Compile and Run Instruction:
1. Run `cd go_transactions`.
2. Firstly, start the coordinators. Run ` go run transaction.go coordinator Coord 6060`.
3. Secondly, start the five servers on different VMs. Run `go run transaction.go server A 9000`.The last number is the port which should is defined in the code.
4. Then, start the clients. Run `go run transaction.go client clientnumber 9000`.
5. Type the instructions in all the clients 
