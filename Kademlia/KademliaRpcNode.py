from Database.database_connectiom import Playlist, PlaylistManager
import os
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

from RaftConsensus.RaftNode import BullyConsensus

lock = threading.Lock()

timeout = 1


class KademliaRpcNode(RpcNode):
    def __init__(self, ip: str, port: int):
        super().__init__(ip, port, None)
        self.routing_table = RoutingTable(self.id)
        self.network = KademliaNetwork(self)
        self.database = PlaylistManager()
        self.requested_nodes = {}
        self.file_transfers = {}
        self.values_requests = {}
        self.pings = {}

        self.consensus = BullyConsensus(
            Node(self.ip, self.port, self.id), self.network, self.routing_table
        )

    def ping(self, node: Node, type: MessageType = MessageType.Request):
        print("kademlia:rpc making ping to", node)
        try:
            self.network.send_rpc(node, Rpc(RpcType.Ping, type, "Ping"))
            with lock:
                if node.id in self.pings:
                    self.pings[node.id] = 1
            waiting = True
            start_time = time.time()
            while waiting:
                waiting = node.id in self.pings
                if time.time() - start_time > timeout:
                    print("kademlia: ping timeout passed")
                    return False
                time.sleep(0.002)
            return True
        except Exception as e:
            print("kademlia: error  ", e)
            return False

    def store(
        self,
        key,
        node,
        value: Tuple[StoreAction, DataType, Any],
    ):
        action, type, data = value
        if type is DataType.Data:
            self.network.send_rpc(
                node, Rpc(RpcType.Store, MessageType.Request, (key, value))
            )
            self.file_transfers[f"{key}{node.id}"] = True
            start_time = time.time()
            while time.time() - start_time < timeout:
                time.sleep(0.005)
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
                    with lock:
                        del self.file_transfers[f"{key}{my_direction[1]}"]
                    return "TIMEOUT"
                if self.file_transfers[f"{key}{my_direction[1]}"] == "Error":
                    transfer.close_transmission()
                    del self.file_transfers[f"{key}{my_direction[1]}"]
                    return "Error"
                time.sleep(0.0005)
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
                (Node(self.ip, self.port, self.id), target_id, result),
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
            identifier = f"{key}{node.id}"
            self.file_transfers[identifier] = None
            start_time = time.time()
            self.network.send_rpc(
                node,
                Rpc(
                    RpcType.FindValue,
                    MessageType.Request,
                    (key, DataType.File, None),
                ),
            )
            while time.time() - start_time < timeout:
                time.sleep(0.000005)
                if self.file_transfers[identifier] is not None:
                    print(
                        f"se obtuvo respuesta para el find file:"
                        + f" {self.file_transfers[identifier]}"
                    )
                    has_file, clock_ticks = self.file_transfers[identifier]
                    with lock:
                        del self.file_transfers[identifier]
                    print("find file returning: ", (node, has_file, clock_ticks))
                    return (node, has_file, clock_ticks)
            print("timeout exceeded on file search: ", key)
            with lock:
                del self.file_transfers[identifier]
            return (node, None, 0)
        except Exception as e:
            print(f"kademlia:rpc error finding a file {key}: ", e)
            return (node, None, 0)

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
                time.sleep(0.005)
                if self.values_requests[f"{key}{node.id}"] is not None:
                    ret_val = self.values_requests[identifier]
                    with lock:
                        del self.values_requests[f"{key}{node.id}"]
                    return ret_val
            print(f"kademlia:rpc Timeout Exceeded: on key {key} for {node}")
            print(self.values_requests)
            print(f"elapsed: {time.time() - start_time} of {timeout}")
            return None
        except Exception as e:
            print(f"kademlia:rpc error buscando la llave {key} - > {e}")
            return None

    def handle_rpc(self, address, rpc, clock_ticks):
        rpc_type, message_type, payload = rpc
        if rpc_type == RpcType.Ping:
            self.handle_ping(address, message_type)
            return
        if rpc_type == RpcType.FindNode:
            self.handle_find_node(message_type, address, payload)
            return
        if rpc_type == RpcType.Store:
            key, value = payload
            self.handle_store(key, address, value, message_type, clock_ticks)
            return
        if rpc_type == RpcType.FindValue:
            key, data_type, data = payload
            self.handle_find_value(
                key, address, data_type, message_type, data, clock_ticks
            )
            return
        # else:
        #     self.consensus.handle_rpc(address, rpc, clock_ticks)
        #     return

    def handle_ping(self, node, message_type):
        if message_type == MessageType.Request:
            print("kademlia:rpc requested ping from", node)
            thread = threading.Thread(
                target=self.ping, args=[node, MessageType.Response]
            )
            thread.start()
        if message_type == MessageType.Response:
            print("kademlia:rpc received ping from", node)
            with lock:
                if node.id in self.pings:
                    del self.pings[node.id]

    def handle_find_node(self, message_type, node, payload):
        if message_type == MessageType.Request:
            node, target_id = payload
            if target_id < 0:
                self.find_node_response(
                    target_id, self.routing_table.get_all_nodes(), node
                )
                return

            result = self.routing_table.find_closest_nodes(target_id, K)
            print("kademlia:rpc results: ", result)
            result.sort(key=lambda node: node.id ^ target_id)
            self.find_node_response(target_id, result, node)
            print("kademlia:rpc responding to a find node", node, "with", result)
        if message_type == MessageType.Response:
            node, target_id, result = payload
            for res_node in result:
                with lock:
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
                print("kademlia:rpc ------------------", data)
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
                file_transfers.receive_file(f"/tmp/songs/{hex(key).lstrip('0x')}.mp3")
                file_transfers.close_transmission()
            else:
                print("kademlia:rpc recibido : ", data, " para: ", action)
                try:
                    with lock:
                        self.database.make_action(action, data, clock_tick)
                    # self.consensus.log.append((action, (data.id, data.name)))
                    self.network.send_rpc(
                        node,
                        Rpc(
                            RpcType.Store,
                            MessageType.Response,
                            (key, (action, data_type, "OK")),
                        ),
                    )
                except Exception as e:
                    print(
                        f"kademlia:rpc ocurrio un error al guardar la playlist", data.id
                    )
                    self.network.send_rpc(
                        node,
                        Rpc(
                            RpcType.Store,
                            MessageType.Response,
                            (key, (action, data_type, f"{e} on {data.name}")),
                        ),
                    )
        if type is MessageType.Response:
            print(node, "kademlia:rpc  respondio con ", data, " al store ", key)
            print("kademlia:rpc el datatype: ", data_type)
            if data_type is DataType.File:
                request_port, peer_port = data
                identifier = f"{key}{request_port}"
                try:
                    self.file_transfers[identifier].start_trasmission(
                        (node.ip, peer_port)
                    )
                    self.file_transfers[identifier].close_transmission()
                    del self.file_transfers[identifier]
                except Exception:
                    print(f"kademlia:rpc:file transfer {identifier}")
                    del self.file_transfers[identifier]
                    self.file_transfers[identifier] = "Error"
            else:
                if data == "OK":
                    self.file_transfers[f"{key}{node.id}"] = False
                else:
                    print(f"kademlia:rpc Error on action over playlist", key, node.id)

    def handle_find_value(
        self, key, address, data_type, message_type, data, clock_ticks
    ):
        if message_type is MessageType.Request:
            if data_type is DataType.Data:
                print("find value data: ", key)
                if key < 0:
                    print("find value responding: ", self.database.get_all(data))
                    print("find value filter", data)
                    self.network.send_rpc(
                        address,
                        Rpc(
                            RpcType.FindValue,
                            MessageType.Response,
                            (key, DataType.Data, self.database.get_all(data)),
                        ),
                    )
                    return
                print(f"kademlia:rpc:find-value searching {key}")
                self.network.send_rpc(
                    address,
                    Rpc(
                        RpcType.FindValue,
                        MessageType.Response,
                        (key, DataType.Data, self.database.get_by_id(data)),
                    ),
                )
                print(
                    f"kademlia:rpc:find-value responding",
                    f" {self.database.get_by_id(data)}",
                )
            else:
                self.network.send_rpc(
                    address,
                    Rpc(
                        RpcType.FindValue,
                        MessageType.Response,
                        (
                            key,
                            DataType.File,
                            os.path.exists(f"/tmp/songs/{hex(key).lstrip('0x')}.mp3"),
                        ),
                    ),
                )
                print(f"find value para {key}--", hex(key).lstrip("0x"))
                # filetransfer = FileTransfer(
                #     self.ip, f"/tmp/songs/{hex(key).lstrip('0x')}.mp3"
                # )
                # print("enviando a: ", data, address)
                # filetransfer.start_trasmission(data)
                # filetransfer.close_transmission()
        if message_type is MessageType.Response:
            if data_type is DataType.Data:
                print(f"rpc: find-value: recibida{data} para{key}")
                self.values_requests[f"{key}{address.id}"] = (data, clock_ticks)
                print(self.values_requests)
            else:
                print(
                    "kademlia:rpc *********llego como respuesta del find_value: ",
                    data,
                    " para ",
                    address,
                )
                self.file_transfers[f"{key}{address.id}"] = (
                    data,
                    clock_ticks,
                )
