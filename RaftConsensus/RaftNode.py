from enum import Enum
from threading import Thread, Lock
import time
from Kademlia.KBucket import Node
from Kademlia.KademliaNetwork import KademliaNetwork
from Kademlia.RoutingTable import RoutingTable
from Kademlia.utils.MessageType import MessageType
from Kademlia.utils.Rpc import Rpc
from Kademlia.utils.RpcType import RpcType
from RaftConsensus.utils.Server import Server


class NodeState(Enum):
    FOLLOWER = "Follower"
    LEADER = "Leader"
    ELECTION = "Election"


class BullyRpcType(Enum):
    ELECTION = "Election"
    COORDINATOR = "Coordinator"


class BullyConsensus(Server):
    def __init__(
        self, node: Node, network: KademliaNetwork, routing_table: RoutingTable
    ):
        super().__init__(node, network, routing_table)
        self.state = NodeState.FOLLOWER
        self.leader_id = None
        self.lock = Lock()
        self.election_timeout = 4
