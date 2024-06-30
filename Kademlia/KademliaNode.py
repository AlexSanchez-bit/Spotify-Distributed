from collections import defaultdict
from typing import Any, List, Tuple
from KBucket import Node, K, sha1_hash
from KademliaRpcNode import KademliaRpcNode
import threading
import time


import threading

from utils.DataType import DataType
from utils.StoreAction import StoreAction

alpha = 3


class KademliaNode(KademliaRpcNode):
    def __init__(self, ip: str, port: int):
        super().__init__(ip, port)
        self.data_store = defaultdict(str)

    def find_node_on_network(self, search_key, on_founded=None):
        find_node_subroutine = threading.Thread(
            target=self._find_node_on_network, args=[search_key, on_founded]
        )
        find_node_subroutine.start()
        print("starting a find_node subroutine")

    def _find_node_on_network(self, search_key, on_founded=None):
        network_key = sha1_hash(search_key)
        nodes = self.routing_table.find_closest_nodes(network_key, K)
        nodes.sort(key=lambda node: node.id ^ network_key)
        closest_knowed = Node(self.ip, self.port, self.id)
        self.requested_nodes[network_key] = [closest_knowed] + nodes
        for node in nodes:
            if node.id != self.id:
                self.find_node(network_key, node)
        # if passed 3 secs a node doesnt respond its better to leave it
        break_ = True
        visited = []
        while break_:
            print("arrived", self.requested_nodes[network_key])

            self.requested_nodes[network_key].sort(
                key=lambda node: node.id ^ network_key
            )

            next_closest = self.requested_nodes[network_key][0]

            print("actual min", closest_knowed, closest_knowed.id ^ network_key)
            print("next min", next_closest, next_closest.id ^ network_key)

            break_ = (closest_knowed.id ^ network_key) < (next_closest.id ^ network_key)

            if not break_:
                closest_knowed = self.requested_nodes[network_key][:K]
                break
            for i in [x for x in self.requested_nodes[network_key] if x not in visited][
                :alpha
            ]:
                self.find_node(network_key, i)
                visited.append(i)

        self.requested_nodes.pop(network_key)
        print("resultado final: ", closest_knowed)
        if on_founded is not None:
            on_founded(closest_knowed)
        return closest_knowed

    def store_a_file(self, file_direction: str):
        key = sha1_hash(file_direction)
        self.find_node_on_network(
            file_direction, lambda nodes: self.send_store(nodes, key, file_direction)
        )

    def send_store(self, nodes, key, file_direction):
        for node in nodes:
            self.store(key, node, (StoreAction.INSERT, DataType.File, file_direction))

    def refresh_buckets(self):
        while True:
            print("revising buckets")
            for bucket in self.routing_table.buckets:
                for node in bucket.get_nodes():
                    self.ping(node)
            time.sleep(600)

    def start(self):
        self.network.start()
        refresh_thread = threading.Thread(target=self.refresh_buckets)
        refresh_thread.start()
        print("starting bucket refresh subroutine")
