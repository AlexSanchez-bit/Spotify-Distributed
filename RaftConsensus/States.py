from enum import Enum


class RaftState(Enum):
    Leader = "Leader"
    Candidate = "Candidate"
    Follower = "Follower"
