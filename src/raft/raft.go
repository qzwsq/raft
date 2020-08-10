package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import "sync"
import "sync/atomic"
import "../labrpc"
import "time"
import "math/rand"
import "fmt"
import "bytes"
import "../labgob"



//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

type Entry struct {
	Term         int32
	Command      interface{}
}

var previous int64 = int64(rand.Intn(1200) + 300)
//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm     int32
	voteFor         int
	logs            []Entry

	// volatile state in all servers
	commitIndex   int
	lastApplied   int

	// volatile state in leader
	nextIndex     []int
	matchIndex    []int

	// extra statement
	timer         int64
	applyCh       chan ApplyMsg
	state         int32
	heartbeatTimer int64
	stop           bool
	cnt            int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	term = int(rf.currentTerm)
	isleader = (!rf.killed() && (!rf.stop) && rf.state == 2)
	rf.mu.Unlock()
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.voteFor)
	e.Encode(rf.logs[0:rf.commitIndex+1])
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}


//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int32
	var voteFor     int
	var logs        []Entry
	rf.mu.Lock()
	if d.Decode(&currentTerm) != nil || d.Decode(&voteFor) != nil || d.Decode(&logs) != nil{
		DPrintf(fmt.Sprintf("[ERROR]me: %d, recover error \n", rf.me))
	} else {
		rf.currentTerm = currentTerm
		rf.voteFor = voteFor
		rf.logs = logs
		rf.persist()
		rf.commitIndex = len(rf.logs)-1
		DPrintf(fmt.Sprintf("[Persist Load]me: %d, load state with term:%d, votefor:%d, latest commit %d\n", rf.me, rf.currentTerm, rf.voteFor, rf.commitIndex))
	}
	rf.mu.Unlock()
}




//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term           int32
	CandidateId    int
	LastLogIndex   int
	LastLogTerm    int32
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term           int32
	VoteGranted    bool
}

type AppendEntriesArgs struct {
	// Your data here (2A, 2B).
	Term           int32
	LeaderId       int
	PrevLogIndex   int
	PrevLogTerm    int32
	Entries        []Entry
	LeaderCommit   int
}

type AppendEntriesReply struct {
	// Your data here (2A, 2B).
	Term           int32
	Success        bool
	ConflictTerm   int32
	ConflictFirstIndex int
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	DPrintf(fmt.Sprintf("[RequestVote]me: %d, recv request from %d\n", rf.me, args.CandidateId))
	if !rf.killed() {
		rf.mu.Lock()
		if args.Term < rf.currentTerm {
			reply.VoteGranted = false
			reply.Term = rf.currentTerm
		} else {
			if args.Term > rf.currentTerm {
				rf.currentTerm = args.Term
				rf.voteFor = -1
				rf.persist()
			}
			reply.Term = rf.currentTerm
			// anyway, it should convert to follower
			rf.state = 0
			// up-to-date check
			if (rf.voteFor == -1 || rf.voteFor == args.CandidateId) &&
				(args.LastLogTerm > rf.logs[rf.commitIndex].Term || (args.LastLogTerm == rf.logs[rf.commitIndex].Term && args.LastLogIndex >= rf.commitIndex)) {
				reply.VoteGranted = true
				rf.voteFor = args.CandidateId
				rf.persist()
				rf.timer = time.Now().UnixNano()/1e6
				DPrintf(fmt.Sprintf("[RequestVote]me: %d, vote for %d\n", rf.me, args.CandidateId))
			} else {
				reply.VoteGranted = false
				DPrintf(fmt.Sprintf("[RequestVote]me: %d, refuse vote to %d\n", rf.me, args.CandidateId))
			}
		}
		rf.mu.Unlock()
	}
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}



func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// Your code here (2A, 2B).
	begin := int64(time.Now().UnixNano()/1e6)
	if !rf.killed() {
		rf.mu.Lock()
		DPrintf(fmt.Sprintf("[AppendEntries]me: %d, recv call from %d time wait for lock %d ms", rf.me, args.LeaderId, int64(time.Now().UnixNano()/1e6) - begin))
		if args.Term < rf.currentTerm {
			reply.Success = false
			reply.Term = rf.currentTerm
		} else {
			rf.state = 0
			rf.voteFor = args.LeaderId
			if rf.currentTerm < args.Term {
				rf.currentTerm = args.Term
				rf.persist()
			}
			reply.Term = rf.currentTerm
			rf.timer = time.Now().UnixNano()/1e6
			// check if match
			if args.PrevLogIndex + 1 <= len(rf.logs) && rf.logs[args.PrevLogIndex].Term == args.PrevLogTerm {
				reply.Success = true
				rf.logs = rf.logs[0: args.PrevLogIndex + 1]
				rf.logs = append(rf.logs, args.Entries...)
				if args.LeaderCommit > rf.commitIndex {
					if len(rf.logs)-1 < args.LeaderCommit {
						rf.commitIndex = len(rf.logs)-1
					} else {
						rf.commitIndex = args.LeaderCommit
					}
				}
				rf.persist()
				DPrintf(fmt.Sprintf("[AppendEntries]me: %d, match with leader %d, matchIndex %d, myTerm %d, leader's Term %d, new commitIndex = %d", rf.me, args.LeaderId, args.PrevLogIndex, rf.logs[args.PrevLogIndex].Term, args.PrevLogTerm, rf.commitIndex))
			} else {
				reply.Success = false
				if args.PrevLogIndex + 1 > len(rf.logs)  {
					reply.ConflictTerm = -1
					reply.ConflictFirstIndex = len(rf.logs)
				} else {
					reply.ConflictTerm = rf.logs[args.PrevLogIndex].Term
					index := args.PrevLogIndex
					for ; index>=0 && rf.logs[index].Term == reply.ConflictTerm ; {
						index -= 1
					}
					reply.ConflictFirstIndex = index + 1
				}
				DPrintf(fmt.Sprintf("[AppendEntries]me: %d, not ma_tch with leader %d, confilctIndex = %d, confilctTerm = %d ", rf.me, args.LeaderId, reply.ConflictFirstIndex, reply.ConflictTerm ))
			}
		}
		rf.mu.Unlock()
	}
	DPrintf(fmt.Sprintf("[AppendEntries]me: %d, recv call from %d time release lock %d ms", rf.me, args.LeaderId, int64(time.Now().UnixNano()/1e6) - begin))
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	begin := int64(time.Now().UnixNano()/1e6)
	DPrintf(fmt.Sprintf("[Monitor]me: %d, call leader %d time with begin time %d", rf.me, server, begin))
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	DPrintf(fmt.Sprintf("[Monitor]me: %d, call leader %d time cost %d", rf.me, server, int64(time.Now().UnixNano()/1e6) - begin))
	return ok
}
//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
	term, isLeader = rf.GetState()
	if isLeader {
		rf.mu.Lock()
		index = len(rf.logs)
		rf.logs = append(rf.logs, Entry{
			Term: rf.currentTerm,
			Command: command,
		})
		rf.persist()
		DPrintf(fmt.Sprintf("[start] me: %d, term: %d, command %s\n", rf.me, rf.currentTerm, command))
		rf.mu.Unlock()
	}

	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func monitor(rf *Raft) {
	DPrintf(fmt.Sprintf("[monitor]node %d become leader with term %d\n", rf.me, rf.currentTerm))
	rf.mu.Lock()
	me := rf.me
	l := len(rf.peers)
	rf.mu.Unlock()
	DPrintf(fmt.Sprintf("[monitor]Leader %d get necessary data with Term %d\n", rf.me, rf.currentTerm))
	first := true
	for ;!rf.killed() ; {
		_, isleader := rf.GetState()
		active := 1
		if isleader {
			DPrintf("[monitor] consensus check: leader %d\n", me)
			rf.mu.Lock()
			newCommitIndex := len(rf.logs) - 1
			for ; rf.state == 2 && newCommitIndex > rf.commitIndex && rf.logs[newCommitIndex].Term == rf.currentTerm ; newCommitIndex -= 1{
				cnt := 1
				for i:=0;i< l;i++ {
					if i != me && rf.matchIndex[i] >= newCommitIndex {
						cnt = cnt + 1
					}
				}
				if 2*cnt > l  && rf.logs[newCommitIndex].Term == rf.currentTerm {
					DPrintf("[monitor] consensus! leader %d, new commitIndex: %d\n", me, newCommitIndex)
					rf.commitIndex = newCommitIndex
					first = true
					rf.persist()
					break
				}
			}
			timer := rf.heartbeatTimer
			logLen := len(rf.logs)
			commitIndex := rf.commitIndex
			rf.mu.Unlock()
			if first || time.Now().UnixNano()/1e6 - timer >= int64(100) || (commitIndex < logLen - 1 && time.Now().UnixNano()/1e6 - timer >= int64(50)) {
				first = false
				for i:=0; i<l; i=i + 1 {
					_, isleader = rf.GetState()
					if !isleader{
						break
					}
					var args *AppendEntriesArgs
					var reply *AppendEntriesReply
					if i != me {
						rf.mu.Lock()
						DPrintf(fmt.Sprintf("[Monitor] begin[logLen = %d nextIndex[%d] = %d] \n", logLen, i, rf.nextIndex[i]))
						if rf.nextIndex[i] < logLen {
							args = &AppendEntriesArgs{
								Term: rf.currentTerm,
								LeaderId: rf.me,
								PrevLogIndex: rf.nextIndex[i] - 1,
								PrevLogTerm: rf.logs[rf.nextIndex[i]-1].Term,
								Entries: rf.logs[rf.nextIndex[i]: logLen],
								LeaderCommit: rf.commitIndex,
							}
						} else if rf.nextIndex[i] == logLen {
							args = &AppendEntriesArgs{
								Term: rf.currentTerm,
								LeaderId: rf.me,
								PrevLogIndex: rf.nextIndex[i] - 1,
								PrevLogTerm: rf.logs[rf.nextIndex[i]-1].Term,
								Entries: []Entry{},
								LeaderCommit: rf.commitIndex,
							}
						} else {
							rf.mu.Unlock()
							break
						}
						DPrintf(fmt.Sprintf("[Monitor]me: %d send to server: %d, leader Term: %d PrevlogIndex: %d PrevLogTerm %d\n", rf.me, i, rf.currentTerm, args.PrevLogIndex, args.PrevLogTerm))
						reply = &AppendEntriesReply{
							Term: 0,
							Success: false,
							ConflictFirstIndex: 0,
							ConflictTerm: 0,
						}
						rf.mu.Unlock()
						c := make(chan bool, 1)
						server := i
						rf.mu.Lock()
						go func() { c<-rf.sendAppendEntries(server, args, reply)}()
						rf.mu.Unlock()
						ok := false
						select {
							case ok = <-c:
								DPrintf(fmt.Sprintf("[Monitor] me: %d send to server: %d, success\n", rf.me, i))
								break
							case <-time.After(25*time.Millisecond):
								DPrintf(fmt.Sprintf("[Monitor] me: %d, RPC to %d timeout\n", rf.me, i))
						}
						if ok {
							active += 1
							rf.mu.Lock()
							if rf.currentTerm != args.Term {
								rf.mu.Unlock()
								break
							}else if reply.Term > rf.currentTerm {
								rf.state = 0
								rf.currentTerm = reply.Term
								rf.persist()
								rf.mu.Unlock()
								break
							} else {
								if reply.Success {
									rf.nextIndex[i] = logLen
									rf.matchIndex[i] = rf.nextIndex[i]-1
								} else {
									if reply.ConflictTerm  >= 0 {
										index := rf.nextIndex[i]-1
										for ; rf.logs[index].Term > reply.ConflictTerm ; {
											index -= 1
										}
										if rf.logs[index].Term == reply.ConflictTerm{
											rf.nextIndex[i] = index + 1
										} else {
											rf.nextIndex[i] = reply.ConflictFirstIndex
										}
									} else {
										rf.nextIndex[i] = reply.ConflictFirstIndex
									}
								}
								DPrintf(fmt.Sprintf("[Monitor] logLen = %d update nextIndex[%d] = %d matchIndex[%d] = %d\n", logLen, i, rf.nextIndex[i], i, rf.matchIndex[i]))
							}
							rf.mu.Unlock()
						}
					}
				}
				rf.mu.Lock()
				if active == 1 && rf.cnt <= 3 {
					rf.cnt += 1
					if rf.cnt > 3 {
						rf.stop = true
					}
					DPrintf(fmt.Sprintf("[monitor] me: %d active=1, stop", rf.me))
				}
				rf.heartbeatTimer = time.Now().UnixNano()/1e6
				rf.mu.Unlock()
			} else {
				time.Sleep(20*time.Millisecond)
			}
		} else {
			break
		}
	}
}

func nextRandom() (int64){
	return int64(rand.Intn(600) + 500)
}

func election(rf *Raft) {
	// init timeout value
	rf.mu.Lock()
	term := rf.currentTerm
	me := rf.me
	l := len(rf.peers)
	rf.mu.Unlock()
	var timeout int64 = 0
	restart := false
	if !(term == 0 && me == 0) {
		timeout = nextRandom()
	}
	for ; !rf.killed(); {
		// load state
		rf.mu.Lock()
		state := rf.state
		timer := rf.timer
		rf.mu.Unlock()
		switch state {
			// follower: check whether timeout
			case 0:
				if time.Now().UnixNano()/1e6 - timer > timeout {
					rf.mu.Lock()
					// become candidate and regenerate timeout
					rf.state = 1
					timeout = nextRandom()
					rf.mu.Unlock()
				}
			// candidate: send RequestVote Rpc and handle vote result
			case 1:
				rf.mu.Lock()
				timeout = int64(1000)
				rf.timer = time.Now().UnixNano()/1e6
				if !restart {
					rf.currentTerm = rf.currentTerm + 1
				} else {
					restart = false
				}
				DPrintf(fmt.Sprintf("[election]me: %d, term increase to %d \n", me, rf.currentTerm))
				rf.voteFor = me
				rf.persist()
				var args *RequestVoteArgs
				var reply *RequestVoteReply
				args = &RequestVoteArgs{
					Term: rf.currentTerm,
					CandidateId: me,
					LastLogIndex: rf.commitIndex,
					LastLogTerm: rf.logs[rf.commitIndex].Term,
				}
				reply = &RequestVoteReply{
					Term: rf.currentTerm,
					VoteGranted: false,
				}
				rf.persist()
				rf.mu.Unlock()

				// check active nodes number
				active := 1
				voteCnt := 1
				for i:=0; i<l && state == 1; i++ {
					DPrintf(fmt.Sprintf("[election] me: %d, send vote RPC to %d with LastLogIndex = %d, LastLogTerm = %d\n ", me, i,args.LastLogIndex, args.LastLogTerm))
					rf.mu.Lock()
					state = rf.state
					rf.mu.Unlock()
					if i!=me {
						c := make(chan bool, 1)
						server := i
						go func() { c<-rf.sendRequestVote(server, args, reply)}()
						ok := false
						select {
							case ok= <-c:
								break
							case <-time.After(25*time.Millisecond):
								DPrintf(fmt.Sprintf("[election]me: %d send to server: %d, timeout\n", rf.me, i))
						}
						if ok {
							active = active + 1
							if reply.VoteGranted {
								voteCnt += 1
								reply.VoteGranted = false
							}
							rf.mu.Lock()
							if rf.currentTerm != args.Term {
								rf.mu.Unlock()
								break
							}
							if reply.Term > rf.currentTerm{
								rf.state = 0
								rf.voteFor = -1
								rf.persist()
								rf.mu.Unlock()
								break
							}
							rf.mu.Unlock()
						}
					}
				}
				rf.mu.Lock()
				state = rf.state
				rf.mu.Unlock()
				if !rf.killed() {
					switch state {
						case 0:
							timeout = nextRandom()
						case 1:
							if time.Now().UnixNano()/1e6 - rf.timer < timeout && float64(voteCnt)/float64(l) > 0.5 {
								// votes received from majority of servers, become leader
								rf.mu.Lock()
								rf.stop = false
								rf.cnt = 0
								rf.state = 2
								rf.heartbeatTimer = time.Now().UnixNano()/1e6
								rf.nextIndex = []int{}
								logLen := len(rf.logs)
								for i:=0; i<l;i++ {
									rf.nextIndex = append(rf.nextIndex, logLen)
								}
								rf.matchIndex = []int{}
								for i:=0; i<l;i++ {
									rf.matchIndex = append(rf.matchIndex, 0)
								}
								rf.mu.Unlock()
								go monitor(rf)
							} else {
								restart = true
							}
					}
				}
			case 2:
				time.Sleep(10*time.Millisecond)
		}
		time.Sleep(50*time.Millisecond)
	}
}


func apply(rf *Raft) {
	// peroidly check whether instance should apply one log
	for ;!rf.killed(); {
		time.Sleep(10*time.Millisecond)
		rf.mu.Lock()
		for ; rf.commitIndex > rf.lastApplied;  {
			rf.lastApplied = rf.lastApplied + 1
			DPrintf(fmt.Sprintf("[state machine]me: %d, apply index: %d, Command: %s\n", rf.me, rf.lastApplied, rf.logs[rf.lastApplied].Command))
			rf.applyCh <- ApplyMsg{
				CommandValid: true,
				Command: rf.logs[rf.lastApplied].Command,
				CommandIndex: rf.lastApplied,
			}
		}
		rf.mu.Unlock()
	}
}
//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.currentTerm = 0
	rf.voteFor = -1
	rf.logs = []Entry{
		Entry{
			Command: "init",
			Term: 0,
		},
	}
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.timer = time.Now().UnixNano()/1e6
	rf.applyCh = applyCh
	rf.state = 0
	rf.stop = false
	rf.cnt = 0
	rf.heartbeatTimer = time.Now().UnixNano()/1e6
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go apply(rf)
	go election(rf)

	return rf
}
