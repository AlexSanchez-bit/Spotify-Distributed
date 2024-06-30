from os import lockf
from KBucket import K, Node
from typing import Tuple, Any, List
from RoutingTable import RoutingTable
from utils.DataTransfer.FileTransfer import FileTransfer
from utils.DataType import DataType
from utils.Rpc import Rpc
from utils.RpcNode import RpcNode
from utils.RpcType import RpcType
from utils.MessageType import MessageType
from KademliaNetwork import KademliaNetwork
import threading

from utils.StoreAction import StoreAction
import time

lock = threading.Lock()

alfa = 3  # the number of paralel calls on node search rpcs


class KademliaRpcNode(RpcNode):
    def __init__(self, ip: str, port: int):
        super().__init__(ip, port, None)
        self.routing_table = RoutingTable(self.id)
        self.network = KademliaNetwork(self)
        self.routing_table.add_node(Node(ip, port))
        self.requested_nodes = {}
        self.file_transfers = {}

    def ping(self, node: Node, type: MessageType = MessageType.Request):
        try:
            print("making ping to", node)
            self.network.send_rpc(node, Rpc(RpcType.Ping, type, "Ping"))
            return True
        except Exception:
            return False

    def store(
        self,
        key,
        node,
        value: Tuple[StoreAction, DataType, Any],
    ):
        print(value)
        action, type, data = value
        if type is DataType.Data:
            self.network.send_rpc(
                node, Rpc(RpcType.Store, MessageType.Request, (key, value))
            )
        else:
            transfer = FileTransfer(self.ip, file_direction=data)
            my_direction = transfer.direction()
            self.file_transfers[f"{key}{my_direction[1]}"] = transfer
            self.network.send_rpc(
                node,
                Rpc(
                    RpcType.Store,
                    MessageType.Request,
                    (key, (action, type, my_direction)),
                ),
            )

            while f"{key}{my_direction[1]}" in self.file_transfers:
                if self.file_transfers[f"{key}{my_direction[1]}"] == "Error":
                    del self.file_transfers[f"{key}{my_direction[1]}"]
                    return "Error"
                time.sleep(0.5)
            return "OK"

    def store_response(self, key, node, value):
        self.network.send_rpc(
            node,
            Rpc(
                RpcType.Store,
                MessageType.Response,
                (key, "Ok"),
            ),
        )

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
        if rpc_type == RpcType.Store:
            key, value = payload
            self.handle_store(key, address, value, message_type)

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
            print("results: ", result)
            result.sort(key=lambda node: node.id ^ target_id)
            self.find_node_response(target_id, result, node)
            print("responding to a find node", node, "with", result)
        if message_type == MessageType.Response:
            node, target_id, result = payload
            for res_node in result:
                with lock:
                    print(self.requested_nodes)
                    if target_id in self.requested_nodes:
                        self.requested_nodes[target_id].append(res_node)
                    else:
                        self.requested_nodes[target_id] = [res_node]

    def handle_store(
        self,
        key,
        node,
        value: Tuple[StoreAction, DataType, Any],
        type: MessageType = MessageType.Request,
    ):
        action, data_type, data = value
        if type is MessageType.Request:
            if data_type is DataType.File:
                print("------------------", data)
                ip, port = data
                file_transfers = FileTransfer(ip)
                self.network.send_rpc(
                    node,
                    Rpc(
                        RpcType.Store,
                        MessageType.Response,
                        (key, (action, type, (port, file_transfers.port))),
                    ),
                )
                file_transfers.receive_file(f"{key}.mp3")
                file_transfers.close_transmission()
        if type is MessageType.Response:
            print(node, " respondio con ", data, " al store ", key)
            request_port, peer_port = data
            identifier = f"{key}{request_port}"
            try:
                self.file_transfers[identifier].start_trasmission((node.ip, peer_port))

                self.file_transfers[identifier].close_transmission()

                del self.file_transfers[identifier]
            except Exception:
                del self.file_transfers[identifier]
                self.file_transfers[identifier] = "Error"
