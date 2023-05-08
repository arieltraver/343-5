package main

import (
	"bufio"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"sync"
	"time"
)

// RaftNode represents a single Raft node in the cluster, providing the
// necessary methods to handle Raft RPC calls.
type RaftNode int

// VoteArguments contains the candidate information needed to request a vote
// from a Raft node during an election and for the node to decide whether to
// vote for the candidate in question.
type VoteArguments struct {
	Term        int
	CandidateID int
	Index       int
	Address     string
}

// VoteReply holds the response from a Raft node after processing a
// RequestVote RPC call.
type VoteReply struct {
	Term       int
	ResultVote bool
}

// AppendEntryArgument holds the information used by a leader to send an
// AppendEntry RPC call and for followers to determine whether the AppendEntry
// RPC call is valid.
type AppendEntryArgument struct {
	Term     int
	LeaderID int
	Address  string
	PrevLogIndex int
	PrevLogTerm int
	Entries []LogEntry
}

// AppendEntryReply represents the response from a Raft node after processing a
// AppendEntry RPC call, including the follower's term and the success status
// of the call.
type AppendEntryReply struct {
	Term    int
	Success bool
}

// ServerConnection represents a connection to another node in the Raft cluster.
type ServerConnection struct {
	serverID      int
	Address       string
	rpcConnection *rpc.Client
	m sync.Mutex
}

type LogEntry struct {
	Index int
	Term  int
}

type Logs struct {
	logs []LogEntry
	m sync.Mutex
}

type indexMap struct {
	nextIndex map[int]int
	m sync.Mutex
}
var selfID int
var serverNodes map[string]*ServerConnection
var currentTerm *safeInt
var votedFor *safeInt
var isLeader *safeBool
var myPort string
var electionTimeout *time.Timer
var logs Logs
var lastAppliedIndex *safeInt
var nextIndex indexMap


type safeInt struct {
	i int
	m sync.Mutex
}

func (i *safeInt) add(val int) {
	i.m.Lock()
	defer i.m.Unlock()
	i.i +=1
}

func(i *safeInt) changeTo(val int) {
	i.m.Lock()
	defer i.m.Unlock()
	i.i = val
}

func(i *safeInt) get() int{
	i.m.Lock()
	defer i.m.Unlock()
	return i.i
}

type safeBool struct {
	b bool
	m sync.Mutex
}

func (b *safeBool) check() bool {
	b.m.Lock()
	defer b.m.Unlock()
	return b.b
}

func (b *safeBool) setTrue() {
	b.m.Lock()
	defer b.m.Unlock()
	b.b = true
}

func (b *safeBool) setFalse() {
	b.m.Lock()
	defer b.m.Unlock()
	b.b = false
}

// resetElectionTimeout resets the election timeout to a new random duration.
// This function should be called whenever an event occurs that prevents the need for a new election,
// such as receiving a heartbeat from the leader or granting a vote to a candidate.
func resetElectionTimeout() {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	duration := time.Duration(r.Intn(300)+301) * time.Millisecond
	electionTimeout.Stop()          // Use Reset method only on stopped or expired timers
	electionTimeout.Reset(duration) // Resets the timer to new random value
}

// RequestVote processes a RequestVote RPC call from a candidate and decides
// whether to vote for the candidate based on whether it has not voted in this
// term and the value of the candidate's term.
func (*RaftNode) RequestVote(arguments VoteArguments, reply *VoteReply) error {

	// reject vote request if candidate's term is lower than current term
	if arguments.Term < currentTerm.get() {
		fmt.Println(arguments.CandidateID, "has term:", arguments.Term, "but current term is", currentTerm.get())

		// if candidate has lower term, it may have failed and come back. call
		// Reconnect() to try to update its rpc.Connection value in serverNodes
		reply.Term = currentTerm.get()
		reply.ResultVote = false

		go Reconnect(arguments.CandidateID, arguments.Address)
		return nil
	}

	if arguments.Term > currentTerm.get() {
	//	fmt.Println("Current term updated line 122")
		//currentTerm.changeTo(arguments.Term) // update current term
		votedFor.changeTo(-1)
	}

	reply.Term = currentTerm.get()

	// grant vote if node has not voted
	if votedFor.get() == -1 {
		logs.m.Lock()
		if len(logs.logs) == 0 || (arguments.Term > logs.logs[len(logs.logs)-1].Term || (arguments.Term == logs.logs[len(logs.logs)-1].Term && arguments.Index >= logs.logs[len(logs.logs)-1].Index)) {
			logs.m.Unlock()
			fmt.Println("Voting for candidate", arguments.CandidateID)
			reply.ResultVote = true
			votedFor.changeTo(arguments.CandidateID)
			go resetElectionTimeout()
		} else {
			reply.ResultVote = false
		}
		
	} else {
		reply.ResultVote = false
	}
	return nil
}

// Reconnect attempts to reconnect to the specified server node and updates the
// rpcConnection in serverNodes.
func Reconnect(newId int, address string) error {
	connectTimer := time.NewTimer(200 * time.Millisecond)
	fmt.Println("Attempting to reconnect with", address)
	for {
		select {
		case <-connectTimer.C:
			return errors.New("connection timed out")
		default:
			client, err := rpc.DialHTTP("tcp", address)
			if err != nil {
				fmt.Println("trying again", err)
			} else {
				fmt.Println("Reconnected with", address)
				// close old connection and replace with new connection
				serverNodes[address].m.Lock()
				serverNodes[address].rpcConnection.Close()
				serverNodes[address].serverID = newId
				serverNodes[address].rpcConnection = client
				serverNodes[address].m.Unlock()
				return nil
			}
		}
	}
}

// AppendEntry processes an AppendEntry RPC call from the leader node. It checks the
// term of the leader and, if valid, places the receiving node into the follower state
// and resets the election timeout, ensuring the follower does not start an election.
// It also updates the follower's term and state, as well as the AppendEntryReply.
func (*RaftNode) AppendEntry(arguments AppendEntryArgument, reply *AppendEntryReply) error {

	// if leader's term is less than current term, reject append entry request
	if arguments.Term < currentTerm.get() {
		reply.Term = currentTerm.get()
		reply.Success = false // not a valid heartbeat
		go Reconnect(arguments.LeaderID, arguments.Address)
		return nil
	}
	if arguments.PrevLogIndex > 0 && len(logs.logs) > arguments.PrevLogIndex && logs.logs[arguments.PrevLogIndex].Term != arguments.PrevLogTerm {
		reply.Success = false
		log.Println("received conflicting log from leader")
		//go Reconnect(arguments.LeaderID, arguments.Address)
		return nil
	}
	if len(logs.logs) == 0 && arguments.PrevLogIndex > 0 {
		reply.Success = false
		log.Println("our log is empty, received non-empty log")
		return nil
	}

	for i, entry := range(arguments.Entries) {
		if entry.Index >= len(logs.logs) {
			logs.m.Lock()
			logs.logs = append(logs.logs, arguments.Entries[i:]...) //append all new.
			logs.m.Unlock()
			break; //done here
		}
		if logs.logs[entry.Index].Term != entry.Term {
			logs.logs[entry.Index] = entry //correct the inorrect entry
		}
	}

	for _, entry := range(logs.logs) {
		fmt.Println("entry:", entry.Index, "term:", entry.Term)
	}
	// if leader's term is greater or equal, its leadership is valid
	currentTerm.changeTo(arguments.Term)
	isLeader.setFalse() // current node is follower
	reply.Term = currentTerm.get()
	reply.Success = true
	go resetElectionTimeout() // heartbeat indicates a leader, so no new election
	fmt.Println("Received heartbeat")

	return nil
}

// LeaderElection initiates and manages the election process for the RaftNode. It
// continuously waits for the election timeout, at which point, it initializes a
// new election, sending vote requests to the other nodes in the cluster. If the
// node becomes a leader, it stops the election process and starts sending heartbeats
// to other nodes.
func LeaderElection() {
	for {
		<-electionTimeout.C // wait for election timeout

		// check if node is already leader so loop does not continue
		if isLeader.check() {
			fmt.Println("ending leaderelection because I am now leader")
			return
		}
		// initialize election
		fmt.Println("current term is" + strconv.Itoa(currentTerm.get()))
		currentTerm.add(1)  // new term
		fmt.Println("current term is" + strconv.Itoa(currentTerm.get()))
		votedFor.changeTo(selfID)

		voteCount := &safeInt{i:0}

		// request votes from other nodes
		fmt.Println("Requesting votes")

		for _, server := range(serverNodes) {
			arguments := VoteArguments{
				Term:        currentTerm.get(),
				CandidateID: votedFor.get(),
				Address:     myPort,
			}
			go func(server *ServerConnection) {
				reply := VoteReply{}
				server.m.Lock()
				err := server.rpcConnection.Call("RaftNode.RequestVote", arguments, &reply)
				server.m.Unlock()
				if err != nil {
					return
				}
				if reply.Term > currentTerm.get() {
					currentTerm.changeTo(reply.Term) // update current term
					fmt.Println("Current term updated line 272")
					votedFor.changeTo(-1)          // reset votedFor
				} else if reply.ResultVote {
					voteCount.add(1)
					// receives votes from a majority of the servers
					if !isLeader.check() && voteCount.get() > len(serverNodes)/2 {
						fmt.Println("Won election! ->", voteCount, "votes for", selfID)
						isLeader.setTrue() // enters leader state
						lastAppliedIndex.changeTo(0)
						//initialize next_index for all nodes.
						for _, server := range(serverNodes) {
							nextIndex.m.Lock()
							server.m.Lock()
							nextIndex.nextIndex[server.serverID] = len(logs.logs)
							server.m.Unlock()
							nextIndex.m.Unlock()
						}
						go Heartbeat()  // begins sending heartbeats
						return
					}
				}

			}(server)
		}
		go resetElectionTimeout()
	}
}

// Heartbeat is used when the current node is a leader; it handles the periodic
// sending of heartbeat messages to other nodes in the cluster to establish its
// role as leader.
func Heartbeat() {
	heartbeatTimer := time.NewTimer(100 * time.Millisecond)
	for {
		<-heartbeatTimer.C
		if !isLeader.check() {
			return
		}

		arguments := AppendEntryArgument{
			Term:     currentTerm.get(),
			LeaderID: selfID,
			Address:  myPort,
		}
		fmt.Println("Sending heartbeats")
		for _, server := range serverNodes {
			go func(server *ServerConnection) {
				reply := AppendEntryReply{}
				server.m.Lock()
				server.rpcConnection.Call("RaftNode.AppendEntry", arguments, &reply)
				server.m.Unlock()
			}(server)
		}
		heartbeatTimer.Reset(100 * time.Millisecond)
	}
}


func client() {
	for {
		time.Sleep(5 * time.Second)
		clientAddToLog()
	}
}
/*
This function is designed to emulate a client reaching out to the
server. Note that many of the realistic details are removed, for
simplicity
*/
func clientAddToLog() {
	// In a realistic scenario, the client will find the leader node and communicate with it
	// In this implementation, we are pretending that the client reached out to the server somehow
	// But any new log entries will not be created unless the server/node is a leader
	// isLeader here is a boolean to indicate whether the node is a leader or not
	if isLeader.check() {
		// lastAppliedIndex here is an int variable that is needed by a node to store the value of the last index it used in the log
		entry := LogEntry{lastAppliedIndex.get(), currentTerm.get()}
		logs.m.Lock()
		logs.logs = append(logs.logs, entry) //append new entry to our log.
		l := len(logs.logs)
		prevTerm := logs.logs[l-1].Term
		logs.m.Unlock()
		log.Println("Client communication created the new log entry at index " + strconv.Itoa(entry.Index))
		lastAppliedIndex.add(1)

		for _, server := range serverNodes {
			go func(server *ServerConnection) {
				arguments := AppendEntryArgument{
					Term:     currentTerm.get(),
					LeaderID: selfID,
					Address:  myPort,
					PrevLogIndex: l - 1,
					PrevLogTerm: prevTerm,
				}
				nextIndex.m.Lock()
				server.m.Lock()
				ni := nextIndex.nextIndex[server.serverID]
				server.m.Unlock()
				nextIndex.m.Unlock()
				reply := AppendEntryReply{}
				for { //keep trying if there are errors
					logs.m.Lock()
					arguments.Entries = logs.logs[ni:] //from the next index onward.
					arguments.PrevLogIndex -= 1
					logs.m.Unlock()
					server.m.Lock()
					server.rpcConnection.Call("RaftNode.AppendEntry", arguments, &reply)
					server.m.Unlock()
					if reply.Success || ni <= 0 {
						break; //good job no errors
					}
					ni -= 1
				}
				nextIndex.m.Lock()
				nextIndex.nextIndex[server.serverID] = l //onto the next thing.
				nextIndex.m.Unlock()	
			}(server)
		}
	}
	/* HINT 2: force the thread to sleep for a good amount of time (less
	   than that of the leader election timer) and then repeat the actions above.
	   You may use an endless loop here or recursively call the function
	*/
	// HINT 3: you don’t need to add to the logic of creating new log entries, just handle the replication
}

func main() {
	// The assumption here is that the command line arguments will contain:
	// This server's ID (zero-based), location and name of the cluster configuration file
	arguments := os.Args
	if len(arguments) == 1 {
		fmt.Println("Please provide cluster information.")
		return
	}

	// --- Read the values sent in the command line

	// Get this server's ID (same as its index for simplicity)
	myID, _ := strconv.Atoi(arguments[1])

	// Get the information of the cluster configuration file containing information on other servers
	file, err := os.Open(arguments[2])
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	myPort = "localhost"

	// --- Read the IP:port info from the cluster configuration file
	scanner := bufio.NewScanner(file)
	lines := make([]string, 0)
	index := 0
	for scanner.Scan() {
		// Get server IP:port
		text := scanner.Text()
		log.Printf(text, index)
		if index == myID {
			myPort = text
			index++
			continue
		}
		// Save that information as a string for now
		lines = append(lines, text)
		index++
	}
	// If anything wrong happens with reading the file, simply exit
	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}

	// -- Initialize global variables
	selfID = myID
	currentTerm = &safeInt{i:-1}
	votedFor = &safeInt{i:-1}
	isLeader = &safeBool{b:false}// starts in the follower state
	// mutex = sync.Mutex{}
	logs = Logs{logs:make([]LogEntry, 0)}
	lastAppliedIndex = &safeInt{i:0}

	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	tRandom := time.Duration(r.Intn(300)+301) * time.Millisecond
	electionTimeout = time.NewTimer(tRandom)

	// --- Register the RPCs of this object of type RaftNode
	api := new(RaftNode)
	err = rpc.Register(api)
	if err != nil {
		log.Fatal("error registering the RPCs", err)
	}
	rpc.HandleHTTP()
	go http.ListenAndServe(myPort, nil)
	log.Printf("serving rpc on port" + myPort)

	// This is a workaround to slow things down until all servers are up and running
	// Idea: wait for user input to indicate that all servers are ready for connections
	// Pros: Guaranteed that all other servers are already alive
	// Cons: Non-realistic work around

	// reader := bufio.NewReader(os.Stdin)
	// fmt.Print("Type anything when ready to connect >> ")
	// text, _ := reader.ReadString('\n')
	// fmt.Println(text)

	// Idea 2: keep trying to connect to other servers even if failure is encountered
	// For fault tolerance, each node will continuously try to connect to other nodes
	// This loop will stop when all servers are connected
	// Pro: Realistic setup
	// Con: If one server is not set up correctly, the rest of the system will halt

	serverNodes = make(map[string]*ServerConnection)
	for index, element := range lines {
		// Attempt to connect to the other server node
		client, err := rpc.DialHTTP("tcp", element)
		// If connection is not established
		for err != nil {
			// Record it in log
			log.Println("Trying again. Connection error: ", err)
			// Try again!
			client, err = rpc.DialHTTP("tcp", element)
		}
		// Once connection is finally established
		// Save that connection information in the servers map
		serverNodes[element] = &ServerConnection{index, element, client, sync.Mutex{}}
		// Record that in log
		fmt.Println("Connected to " + element)
	}

	nextIndex = indexMap{nextIndex:make(map[int]int)}
	var wg sync.WaitGroup
	wg.Add(1)
	go LeaderElection() // concurrent and non-stop leader election
	go client()
	wg.Wait()           // waits forever, so main process does not stop
}
