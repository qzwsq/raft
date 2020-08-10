# Raft
MIT 6.824 lab(2020 Spring) implementation

## Tutorial
### Lab 2A
1. Initialize states of Raft state machine.  
 > Persist states in all server: currentTerm/voteFor/logs[]  
 > Violate states in follower: commitIndex/lastApplied  
 > Violate states in leader: nextIndex[]/matchIndex[]  
 > Violate states might be useful in all server: timer/state  
 > Channel to apply msg: applyCh
2. Fill RequestVote RPC and AppendEntries RPC(AppendEntries RPC, you could implement it simply, such as return success directly)
 > hint: don't forget to implement AppendEntriesArgs and AppendEntriesReply
3. Add two go coroutines election() and monitor() which action as daemon.(election() use to control elect process of state machine, montior() use to send AppendEntries RPC)

After complete things below, your code should pass test 2A success!


### Lab 2B  
1. Fill start function in raft.go(check whether it is a leader, if answer is yes, then append log to rf.logs)
2. Complete monitor()(how to fill args, how to handle reply)
3. Complete AppendEntries RPC
4. Add a go coroutine apply() to compare size of commitIndex and lastApplied

After complete things below, your code should pass test 2B success!


### Lab 2C
1. Fill persist() and readPersist()
2. Execute persist() immediately after the change of currentTerm/voteFor/commitIndex

After complete things below, your code should pass test 2C success!

### Some Details
However, these tasks are not as simple as they describe. During the period of completing them, you may need lots of details. You can find most of details in [Guide to Raft](https://thesquareplanet.com/blog/students-guide-to-raft/) write by TA of the course.
