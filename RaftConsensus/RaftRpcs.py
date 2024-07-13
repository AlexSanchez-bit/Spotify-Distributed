from enum import Enum


class RaftRpc(Enum):
    RequestVote = "RequestVote"
    AppendEntries = "AppendEntries"
    LeaderHeartBeat = "LeaderHeartBeat"
