{
  server: {
    port: 4949,
    address: "127.0.0.1",
  },
  manager: {
    id: 3,
    leader: false,
    leaderAddress: "http://127.0.0.1:4747",
    peers: ["127.0.0.1:4747", "127.0.0.1:4848"],
    electionTick: 30,
    heartbeatTick: 3,
    applied: 0,
    maxSizePerMsg: 0,
    maxInflightMsgs: 256,
    checkQuorum: false,
    preVote: false,
    minElectionTick: 0,
    maxElectionTick: 0,
    skipBcastCommit: false,
    batchAppend: false,
    priority: 0,
  },
}
