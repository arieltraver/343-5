package main

import (
	"bufio"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"sync"
	"time"
	"errors"
)

// RaftNode represents a single Raft node in the cluster, providing the
// necessary methods to handle Raft RPC calls.
type RaftNode int

type logEntry struct {
	index int
	term  int
}

// VoteArguments contains the candidate information needed to request a vote
// from a Raft node during an election and for the node to decide whether to
// vote for the candidate in question.
type VoteArguments struct {
	Term        int
	CandidateID int
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
	LastLogIndex	int		// check log length
	LastLogTerm		int		// check last log term
	CommitLeader	int 	// what index has been received by the majority
	LogEntry logEntry
}

// AppendEntryReply represents the response from a Raft node after processing a
// AppendEntry RPC call, including the follower's term and the success status
// of the call.
type AppendEntryReply struct {
	Term    int
	Success bool
	MismatchTerm int //last term where a conflict occurs
	MismatchIndex int //the index of the log where there's a mismatch
}

// ServerConnection represents a connection to another node in the Raft cluster.
type ServerConnection struct {
	serverID      int
	Address       string
	rpcConnection *rpc.Client
}


var selfID int
var serverNodes map[string]ServerConnection
var currentTerm int
var currLogTerm int
var votedFor int
var isLeader bool
var myPort string
var mutex sync.Mutex // to lock global variables
var electionTimeout *time.Timer
var selfLog []logEntry


// resetElectionTimeout resets the election timeout to a new random duration.
// This function should be called whenever an event occurs that prevents the need for a new election,
// such as receiving a heartbeat from the leader or granting a vote to a candidate.
func resetElectionTimeout() {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	duration := time.Duration(r.Intn(150)+151) * time.Millisecond
	electionTimeout.Stop()          // Use Reset method only on stopped or expired timers
	electionTimeout.Reset(duration) // Resets the timer to new random value
}

// RequestVote processes a RequestVote RPC call from a candidate and decides
// whether to vote for the candidate based on whether it has not voted in this
// term and the value of the candidate's term.
func (*RaftNode) RequestVote(arguments VoteArguments, reply *VoteReply) error {
	mutex.Lock()
	defer mutex.Unlock()

	// reject vote request if candidate's term is lower than current term
	if arguments.Term < currentTerm {
		fmt.Println(arguments.CandidateID, "has term:", arguments.Term, "but current term is", currentTerm)

		// if candidate has lower term, it may have failed and come back. call
		// Reconnect() to try to update its rpc.Connection value in serverNodes
		reply.Term = currentTerm
		reply.ResultVote = false
		go Reconnect(arguments.CandidateID, arguments.Address)
		return nil
	}

	if arguments.Term > currentTerm {
		currentTerm = arguments.Term // update current term
		votedFor = -1                // has not voted in this new term
		isLeader = false
	}

	reply.Term = currentTerm

	// grant vote if node has not voted
	if votedFor == -1 {
		fmt.Println("Voting for candidate", arguments.CandidateID)
		reply.ResultVote = true
		votedFor = arguments.CandidateID
		resetElectionTimeout()
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
		case <- connectTimer.C:
			return errors.New("connection timed out") 
		default:
			client, err := rpc.DialHTTP("tcp", address)
			if err != nil {
				fmt.Println("trying again", err)
			} else {
				fmt.Println("Reconnected with", address)
				// close old connection and replace with new connection
				mutex.Lock()
				serverNodes[address].rpcConnection.Close()
				serverNodes[address] = ServerConnection{serverID: newId, Address: address, rpcConnection: client}
				mutex.Unlock()
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
	mutex.Lock()
	defer mutex.Unlock()

	/*
	if we are ahead of the leader:
	*/
	if arguments.Term < currentTerm || arguments.LastLogIndex < len(selfLog) - 2 {
		fmt.Println("this node is ahead of the leader")
		reply.Term = currentTerm
		reply.Success = false // not a valid heartbeat
		go Reconnect(arguments.LeaderID, arguments.Address)
		return nil
	}
	/*
	if we look back one node, and see a problem:
	*/
	if selfLog[arguments.LastLogIndex].term != arguments.LastLogTerm {
		fmt.Println("there is an error in our log") //todo: do something
		//todo: ask leader to replicate its log.
		reply.Success = false
		reply.MismatchIndex = len(selfLog) - 1
		reply.MismatchTerm = currentTerm
		isLeader = false
		go resetElectionTimeout()
		return nil

	/*
	if everything is fine:
	*/
	} else {
		selfLog = append(selfLog, arguments.LogEntry)
		currentTerm = arguments.Term
		isLeader = false
		reply.Success = true
		go resetElectionTimeout()
		fmt.Println("received heartbeat:", arguments.LogEntry)
		return nil
	}

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
		if isLeader {
			fmt.Println("ending leaderelection because I am now leader")
			return
		}

		mutex.Lock()
		// initialize election
		currentTerm++     // new term
		votedFor = selfID // votes for itself

		mutex.Unlock()

		arguments := VoteArguments{
			Term:        currentTerm,
			CandidateID: selfID,
			Address:     myPort,
		}

		voteCount := 1

		// request votes from other nodes
		fmt.Println("Requesting votes")
		for _, server := range serverNodes {
			go func(server ServerConnection) {
				reply := VoteReply{}
				err := server.rpcConnection.Call("RaftNode.RequestVote", arguments, &reply)
				if err != nil {
					return
				}

				mutex.Lock()
				defer mutex.Unlock()

				if reply.Term > currentTerm {
					currentTerm = reply.Term // update current term
					votedFor = -1            // reset votedFor
				} else if reply.ResultVote {
					voteCount++
					// receives votes from a majority of the servers
					if !isLeader && voteCount > len(serverNodes)/2 {
						fmt.Println("Won election! ->", voteCount, "votes for", selfID)
						isLeader = true // enters leader state
						go Heartbeat()  // begins sending heartbeats
						return
					}
				}

			}(server)
		}
		resetElectionTimeout()
	}
}
// Heartbeat is used when the current node is a leader; it handles the periodic
// sending of heartbeat messages to other nodes in the cluster to establish its
// role as leader.
func Heartbeat() {
	heartbeatTimer := time.NewTimer(100 * time.Millisecond)
	for {
		<-heartbeatTimer.C
		mutex.Lock()
		if !isLeader {
			mutex.Unlock()
			return
		}
		if len(selfLog) == 0 {
			selfLog = append(selfLog, logEntry{index:0,term:0})
		}
		lastIndex := selfLog[len(selfLog)-1].index
		lastTerm := selfLog[len(selfLog)-1].term
		entryIndex := lastIndex + 1
		newEntry := logEntry{index:entryIndex,term:currentTerm} //TODO: get the correct entry.
		arguments := AppendEntryArgument{
			Term:     currentTerm,
			LeaderID: selfID,
			Address:  myPort,
			LogEntry: newEntry,
			LastLogTerm: lastTerm,
			LastLogIndex: lastIndex,
		}
		ct := currentTerm
		mutex.Unlock()

		fmt.Println("Sending heartbeats")
		for _, server := range serverNodes {
			reply := AppendEntryReply{}
			server.rpcConnection.Call("RaftNode.AppendEntry", arguments, &reply)
			if reply.Term > ct {
				fmt.Println("error, this is term", reply.Term, "I am in", currentTerm)
				mutex.Lock()
				isLeader = false
				mutex.Unlock()
				return
			} else if reply.MismatchIndex != -1 {
				log.Println("worker has log error at", reply.MismatchIndex)
			}
		}
		heartbeatTimer.Reset(100 * time.Millisecond)
	}
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
	currentTerm = 0
	votedFor = -1
	isLeader = false // starts in the follower state
	selfLog = make([]logEntry, 0, 10)
	currLogTerm = 0
	mutex = sync.Mutex{}

	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	tRandom := time.Duration(r.Intn(150)+151) * time.Millisecond
	electionTimeout = time.NewTimer(tRandom)

	// --- Register the RPCs of this object of type RaftNode
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

	serverNodes = make(map[string]ServerConnection)
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
		serverNodes[element] = ServerConnection{index, element, client}
		// Record that in log
		fmt.Println("Connected to " + element)
	}

	api := new(RaftNode)
	err = rpc.Register(api)
	if err != nil {
		log.Fatal("error registering the RPCs", err)
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go LeaderElection() // concurrent and non-stop leader election
	wg.Wait()           // waits forever, so main process does not stop
}
