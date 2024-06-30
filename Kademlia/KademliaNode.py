from collections import defaultdict
from typing import Any, List, Optional, Tuple
from KBucket import Node, K, sha1_hash
from KademliaRpcNode import KademliaRpcNode
import threading
import time
from queue import PriorityQueue

import threading

from utils.DataType import DataType
from utils.StoreAction import StoreAction

lock = threading.Lock()
alpha = 3


class KademliaNode(KademliaRpcNode):
    def __init__(self, ip: str, port: int):
        super().__init__(ip, port)
        self.data_store = defaultdict(str)

    def node_lookup(self, target_id: int) -> List[Node]:
        shortlist = self.routing_table.find_closest_nodes(target_id, K)
        already_queried = set()
        closest_nodes = []

        while shortlist:
            # Sort shortlist by distance to target_id
            shortlist.sort(key=lambda node: node.id ^ target_id)
            closest_nodes = shortlist[:K]

            # Parallel RPCs to alpha nodes
            threads = []
            for node in closest_nodes[:alpha]:
                if node.id not in already_queried:
                    already_queried.add(node.id)
                    thread = threading.Thread(
                        target=self._query_node, args=(node, target_id, shortlist)
                    )
                    threads.append(thread)
                    thread.start()

            # Wait for all threads to complete
            for thread in threads:
                thread.join()

            # Check if the closest nodes list has stabilized
            if all(node.id in already_queried for node in closest_nodes):
                break

        print("resultado del node_lookup: ", closest_nodes)
        return closest_nodes

    def _query_node(self, node: Node, target_id: int, shortlist: List[Node]):
        self.find_node(target_id, node)
        response = self._wait_for_response(target_id)
        if response:
            new_nodes = response
            with lock:
                for new_node in new_nodes:
                    if new_node not in shortlist:
                        shortlist.append(new_node)

    def _wait_for_response(self, target_id: int, timeout: int = 5) -> List[Node]:
        start_time = time.time()
        while time.time() - start_time < timeout:
            with lock:
                if target_id in self.requested_nodes:
                    response = self.requested_nodes[target_id]
                    del self.requested_nodes[target_id]
                    return response
            time.sleep(0.1)
        print("timeout passed")
        return []

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
