from collections import defaultdict
from typing import Any, List, Tuple
from KBucket import Node, K, sha1_hash
from KademliaRpcNode import KademliaRpcNode
import threading
import time


import threading

alpha = 3


class KademliaNode(KademliaRpcNode):
    def __init__(self, ip: str, port: int):
        super().__init__(ip, port)
        self.data_store = defaultdict(str)

    def find_node_on_network(self, search_key):
        network_key = sha1_hash(search_key)
        nodes = self.routing_table.find_closest_nodes(network_key, K)
        nodes.sort(key=lambda node: node.id ^ network_key)
        closest_knowed = nodes[0]
        self.requested_nodes[network_key] = []
        for node in nodes:
            self.find_node(network_key, node)
        # if passed 10 secs a node doesnt respond its better to leave it
        time.sleep(10)
        print(closest_knowed)
        print("arrived", self.requested_nodes[network_key])

    def refresh_buckets(self):
        while True:
            print("revising buckets")
            for bucket in self.routing_table.buckets:
                for node in bucket.get_nodes():
                    print("buckets", node)
            time.sleep(600)

    def start(self):
        self.network.start()
        refresh_thread = threading.Thread(target=self.refresh_buckets)
        refresh_thread.start()
        print("starting bucket refresh subroutine")
