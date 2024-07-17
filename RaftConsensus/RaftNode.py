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

        # Thread(target=self.monitor_leader, daemon=True).start()

        # Thread(target=self.monitor_leader, daemon=True).start()

    def monitor_leader(self):
        while True:
            time.sleep(self.election_timeout)
            if not self.ping_leader():
                self.start_election()

    def ping_leader(self):
        if self.leader_id is None:
            return False
        leader_node = self.routing_table.get_node_by_id(self.leader_id)
        if leader_node:
            return self.ping(leader_node)
        return False

    def ping(self, node: Node):
        rpc = Rpc(RpcType.Ping, MessageType.Request, "Ping")
        try:
            self.network.send_rpc(node, rpc)
            return True
        except Exception as e:
            print(f"bully: an exception occurred {e}")
            return False

    def start_election(self):
        print("requesting start elections")
        # with self.lock:
        #     self.state = NodeState.ELECTION
        #     print(f"Node {self.node.id} starting election")
        #     higher_nodes = [
        #         node
        #         for node in self.routing_table.get_all_nodes()
        #         if node.id > self.node.id
        #     ]
        #
        #     if not higher_nodes:
        #         self.become_leader()
        #         return
        #
        #     responses = []
        #     threads = []
        #     for node in higher_nodes:
        #         thread = Thread(
        #             target=self.send_election_message, args=(node, responses)
        #         )
        #         thread.start()
        #         threads.append(thread)
        #
        #     for thread in threads:
        #         thread.join()
        #
        #     if not responses:
        #         self.become_leader()
