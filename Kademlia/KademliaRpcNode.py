from Database.database_connectiom import PlaylistManager
from Kademlia.KBucket import K, Node
from typing import Tuple, Any, List
from Kademlia.RoutingTable import RoutingTable
from Kademlia.utils.DataTransfer.FileTransfer import FileTransfer
from Kademlia.utils.DataType import DataType
from Kademlia.utils.Rpc import Rpc
from Kademlia.utils.RpcNode import RpcNode
from Kademlia.utils.RpcType import RpcType
from Kademlia.utils.MessageType import MessageType
from Kademlia.KademliaNetwork import KademliaNetwork
import threading

from Kademlia.utils.StoreAction import StoreAction
import time

from RaftConsensus.utils.Server import Server

lock = threading.Lock()

timeout = 4


class KademliaRpcNode(RpcNode):
    def __init__(self, ip: str, port: int):
        super().__init__(ip, port, None)
        self.routing_table = RoutingTable(self.id)
        self.network = KademliaNetwork(self)
        self.routing_table.add_node(Node(ip, port))
        self.database = PlaylistManager()
        self.requested_nodes = {}
        self.file_transfers = {}
        self.values_requests = {}
        self.pings = {}

        self.consensus = Server(
            Node(self.ip, self.port, self.id), self.network, self.routing_table
        )

    def ping(self, node: Node, type: MessageType = MessageType.Request):
        print("making ping to", node)
        try:
            self.network.send_rpc(node, Rpc(RpcType.Ping, type, "Ping"))
            with lock:
                self.pings[node.id] = 1
            waiting = True
            start_time = time.time()
            while waiting:
                waiting = node.id in self.pings
                if time.time() - start_time > timeout:
                    return False
                time.sleep(0.5)
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
            self.file_transfers[f"{key}{node.id}"] = True
            start_time = time.time()
            while time.time() - start_time < timeout:
                time.sleep(0.5)
                if not self.file_transfers[f"{key}{node.id}"]:
                    return True
            return False

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

            start_time = time.time()
            while f"{key}{my_direction[1]}" in self.file_transfers:
                if time.time() - start_time > timeout:
                    transfer.close_transmission()
                    del self.file_transfers[f"{key}{my_direction[1]}"]
                    return "TIMEOUT"
                if self.file_transfers[f"{key}{my_direction[1]}"] == "Error":
                    transfer.close_transmission()
                    del self.file_transfers[f"{key}{my_direction[1]}"]
                    return "Error"
                time.sleep(0.5)
            return "OK"

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

    def find_value(self, key: int, node: Node, data: Tuple[DataType, str]):
        data_type, _ = data
        if data_type is DataType.Data:
            return self.find_value_data(key, node, data)
        else:
            return self.find_value_file(key, node)

    def find_value_file(self, key: int, node: Node):
        try:
            filetransfer = FileTransfer(self.ip)
            direction = filetransfer.direction()
            identifier = f"{key}{direction}"
            self.file_transfers[identifier] = (True, 0)
            start_time = time.time()
            self.network.send_rpc(
                node,
                Rpc(
                    RpcType.FindValue,
                    MessageType.Request,
                    (key, DataType.File, direction),
                ),
            )
            filetransfer.receive_file(f"./songs/{node.id}{key}.mp4")
            while time.time() - start_time < timeout:
                time.sleep(0.5)
                with lock:
                    if not self.file_transfers[identifier][0]:
                        _, clock_ticks = self.file_transfers[identifier]
                        del self.file_transfers[identifier]
                        filetransfer.close_transmission()
                        return (f"./songs/{key}.mp3", clock_ticks)
            filetransfer.close_transmission()
            return "Conection TimeOut"
        except Exception as e:
            print("error findin a file {key}: ", e)
            return None

    def find_value_data(self, key: int, node: Node, data: Tuple[DataType, str]):
        try:
            data_type, elid = data
            self.network.send_rpc(
                node,
                Rpc(RpcType.FindValue, MessageType.Request, (key, data_type, elid)),
            )
            identifier = f"{key}{node.id}"
            self.values_requests[f"{key}{node.id}"] = None
            start_time = time.time()
            while time.time() - start_time < timeout:
                time.sleep(0.5)
                if self.values_requests[f"{key}{node.id}"] is not None:
                    ret_val = self.values_requests[identifier]
                    del self.values_requests[f"{key}{node.id}"]
                    return ret_val
            print(f"Timeout Exceeded: on key {key} for {node}")
            return None
        except Exception as e:
            print(f"error buscando la llave {key} - > {e}")
            return None

    def handle_rpc(self, address, rpc, clock_ticks):
        rpc_type, message_type, payload = rpc
        if rpc_type == RpcType.Ping:
            self.handle_ping(address, message_type)
        if rpc_type == RpcType.FindNode:
            self.handle_find_node(message_type, address, payload)
        if rpc_type == RpcType.Store:
            key, value = payload
            self.handle_store(key, address, value, message_type, clock_ticks)
        if rpc_type == RpcType.FindValue:
            key, data_type, data = payload
            self.handle_find_value(
                key, address, data_type, message_type, data, clock_ticks
            )
        else:
            print(address, rpc)

    def handle_ping(self, node, message_type):
        if message_type == MessageType.Request:
            print("requested ping from", node)
            thread = threading.Thread(
                target=self.ping, args=[node, MessageType.Response]
            )
            thread.start()
        if message_type == MessageType.Response:
            print("received ping from", node)
            with lock:
                del self.pings[node.id]
            with lock:
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
        clock_tick=1,
    ):
        action, data_type, data = value
        if type is MessageType.Request:
            if data_type is DataType.File:
                print("------------------", data)
                ip, port = data
                file_transfers = FileTransfer(self.ip)
                self.network.send_rpc(
                    node,
                    Rpc(
                        RpcType.Store,
                        MessageType.Response,
                        (key, (action, data_type, (port, file_transfers.port))),
                    ),
                )
                file_transfers.receive_file(f"./songs/{key}.mp3")
                file_transfers.close_transmission()
            else:
                print("recibido : ", data, " para: ", action)
                try:
                    with lock:
                        self.database.make_action(action, data, clock_tick)
                        self.network.send_rpc(
                            node,
                            Rpc(
                                RpcType.Store,
                                MessageType.Response,
                                (key, (action, data_type, "OK")),
                            ),
                        )
                except Exception as e:
                    print(f"ocurrio un error al guardar la playlist {data.id}")
                    self.network.send_rpc(
                        node,
                        Rpc(
                            RpcType.Store,
                            MessageType.Response,
                            (key, (action, data_type, f"{e} on {data.name}")),
                        ),
                    )
        if type is MessageType.Response:
            print(node, " respondio con ", data, " al store ", key)
            print("el datatype: ", data_type)
            if data_type is DataType.File:
                print("es un file")
                request_port, peer_port = data
                identifier = f"{key}{request_port}"
                try:
                    self.file_transfers[identifier].start_trasmission(
                        (node.ip, peer_port)
                    )
                    self.file_transfers[identifier].close_transmission()
                    del self.file_transfers[identifier]
                except Exception:
                    del self.file_transfers[identifier]
                    self.file_transfers[identifier] = "Error"
            else:
                if data == "OK":
                    self.file_transfers[f"{key}{node.id}"] = False
                else:
                    print(f"Error on action over playlist {key}{node.id}")

    def handle_find_value(
        self, key, address, data_type, message_type, data, clock_ticks
    ):
        if message_type is MessageType.Request:
            if data_type is DataType.Data:
                self.network.send_rpc(
                    address,
                    Rpc(
                        RpcType.FindValue,
                        MessageType.Response,
                        (key, DataType.Data, self.database.get_by_id(data)),
                    ),
                )
            else:
                self.network.send_rpc(
                    address,
                    Rpc(
                        RpcType.FindValue,
                        MessageType.Response,
                        (key, DataType.File, data),
                    ),
                )
                filetransfer = FileTransfer(self.ip, f"./songs/{key}.mp3")
                print("enviando a: ", data, address)
                filetransfer.start_trasmission(data)
                filetransfer.close_transmission()
        if message_type is MessageType.Response:
            if data_type is DataType.Data:
                self.values_requests[f"{key}{address.id}"] = (data, clock_ticks)
            else:
                print(
                    "*********llego como respuesta del find_value: ",
                    data,
                    " para ",
                    address,
                )
                self.file_transfers[f"{key}{data}"] = (False, clock_ticks)
