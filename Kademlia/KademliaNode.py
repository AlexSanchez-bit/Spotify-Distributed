from collections import defaultdict
from typing import Any, List, Tuple
from KBucket import Node, K, sha1_hash
from KademliaRpcNode import KademliaRpcNode
from RoutingTable import RoutingTable
from KademliaNetwork import KademliaNetwork
import threading
import time

import threading


class KademliaNode(KademliaRpcNode):
    def __init__(self, ip: str, port: int):
        super().__init__(ip, port)
        self.routing_table = RoutingTable(self.id)
        self.data_store = defaultdict(str)
        self.network = KademliaNetwork(self)

    def ping(self, node: Node) -> bool:
        try:
            print(f"pinging from : {self} to {node}")
            self.network.send_rpc(node, ("PING", Node(self.ip, self.port)))
            return True
        except Exception as e:
            print(e)
            return False

    def store(self, key: str, value: str):
        self.data_store[key] = value

    def find_node(self, target_id: int) -> List[Node]:
        return self.routing_table.find_closest_nodes(target_id, K)

    def find_value(self, key: str) -> str:
        return self.data_store.get(key, "")

    def handle_rpc(self, rpc: Tuple[str, Any]):
        command, args = rpc
        print(rpc)
        print(command, args)
        if command == "PING":
            node = args
            print(f"recived ping from :  {node}")
            return self.ping(node)
        elif command == "STORE":
            key, value = args
            self.store(key, value)
        elif command == "FIND_NODE":
            target_id = args
            return self.find_node(target_id)
        elif command == "FIND_VALUE":
            key = args
            return self.find_value(key)

    def refresh_buckets(self):
        while True:
            for bucket in self.routing_table.buckets:
                for node in bucket.get_nodes():
                    if not self.ping(node):
                        bucket.remove_node(node)
            time.sleep(6)

    def start(self):
        self.network.start()
        refresh_thread = threading.Thread(target=self.refresh_buckets)
        refresh_thread.start()
        print("starting bucket refresh subroutine")
