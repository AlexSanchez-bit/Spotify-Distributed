from os import lockf
from KBucket import K, Node
from typing import Tuple, Any, List
from RoutingTable import RoutingTable
from utils.Rpc import Rpc
from utils.RpcNode import RpcNode
from utils.RpcType import RpcType
from utils.MessageType import MessageType
from KademliaNetwork import KademliaNetwork

import threading

lock = threading.Lock()

alfa = 3  # the number of paralel calls on node search rpcs


class KademliaRpcNode(RpcNode):
    def __init__(self, ip: str, port: int):
        super().__init__(ip, port, None)
        self.routing_table = RoutingTable(self.id)
        self.network = KademliaNetwork(self)
        self.routing_table.add_node(Node(ip, port))
        self.requested_nodes = {}

    def ping(self, node: Node, type: MessageType = MessageType.Request):
        try:
            print("making ping to", node)
            self.network.send_rpc(node, Rpc(RpcType.Ping, type, "Ping"))
            return True
        except Exception:
            return False

    def store(self, key: str, value: str):
        pass

    def find_node(
        self,
        target_id: int,
        node: Node,
    ):
        self.network.send_rpc(
            node,
            Rpc(
                RpcType.FindNode,
                MessageType.Request,
                (Node(self.ip, self.port), target_id),
            ),
        )

    def find_node_response(
        self,
        target_id: int,
        result: List[Node],
        node: Node,
    ):
        self.network.send_rpc(
            node,
            Rpc(
                RpcType.FindNode,
                MessageType.Response,
                (Node(self.ip, self.port), target_id, result),
            ),
        )

    def find_value(self, key: str) -> str:
        return ""

    def handle_rpc(self, address, rpc: Rpc):
        rpc_type, message_type, payload = rpc
        if rpc_type == RpcType.Ping:
            self.handle_ping(address, message_type)
        if rpc_type == RpcType.FindNode:
            self.handle_find_node(message_type, address, payload)

    def handle_ping(self, node, message_type):
        if message_type == MessageType.Request:
            print("requested ping from", node)
            self.ping(node, type=MessageType.Response)
        if message_type == MessageType.Response:
            print("received ping from", node)
            if node in self.network.sended_pings:
                self.network.sended_pings.remove(node)

    def handle_find_node(self, message_type, node, payload):
        if message_type == MessageType.Request:
            node, target_id = payload
            result = self.routing_table.find_closest_nodes(target_id, K)
            result.sort(key=lambda node: node.id ^ target_id)
            if result[0] ^ target_id >= self.id ^ target_id:
                print("im the closest i know")
                result = [Node(self.ip, self.port, self.id)]
            self.find_node_response(target_id, result, node)
            print("responding to a find node", node, "with", result)
        if message_type == MessageType.Response:
            node, target_id, result = payload
            print(result, "for", target_id)
            for res_node in result:
                with lock:
                    self.requested_nodes[target_id].append(res_node)
