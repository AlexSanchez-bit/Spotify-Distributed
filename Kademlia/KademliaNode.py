from collections import defaultdict
from typing import Any, List, Optional, Tuple
from Database.database_connectiom import Playlist
from Kademlia.KBucket import Node, K, sha1_hash
from Kademlia.KademliaRpcNode import KademliaRpcNode
import threading
import time
from queue import PriorityQueue

import threading

from Kademlia.utils.DataType import DataType
from Kademlia.utils.StoreAction import StoreAction

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
            print("short list", shortlist)
            # Sort shortlist by distance to target_id
            shortlist.sort(key=lambda node: node.id ^ target_id)
            closest_nodes = list(set(shortlist[:K]))

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
            print("contenido de la shortlist: ", shortlist)

            # Check if the closest nodes list has stabilized
            if all(node.id in already_queried for node in closest_nodes):
                break

        print("resultado del node_lookup: ", closest_nodes)
        return closest_nodes

    def _query_node(self, node: Node, target_id: int, shortlist: List[Node]):
        self.find_node(target_id, node)
        response = self._wait_for_response(target_id)
        print("wait for response dio: ", response)
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
                print("lo nodos pedios", self.requested_nodes)
                if target_id in self.requested_nodes:
                    response = self.requested_nodes[target_id]
                    del self.requested_nodes[target_id]
                    return response
            time.sleep(0.1)
        print("timeout passed")
        return []

    def store_playlist(self, action: StoreAction, playlistData: Playlist):
        key = sha1_hash(playlistData.id)
        nodes = self.node_lookup(key)
        resp = self.send_store_playlist(nodes, key, action, playlistData)
        print(resp)

    def send_store_playlist(
        self, nodes, key, action: StoreAction, playlistData: Playlist
    ):
        threads = []
        for node in nodes:
            print(f"sending a store to {node} on {key}")
            thread = threading.Thread(
                target=self.store,
                args=[
                    key,
                    node,
                    (action, DataType.Data, playlistData),
                ],
            )
            threads.append(thread)
            thread.start()

        for th in threads:
            th.join()
        print("Archivo enviado")

    def store_a_file(self, file_direction: str):
        key = sha1_hash(file_direction)
        nodes = self.node_lookup(key)
        resp = self.send_store_file(nodes, key, file_direction)
        print(resp)

    def send_store_file(self, nodes, key, file_direction):
        threads = []
        while len(nodes) > 0:
            for node in nodes[alpha:]:
                print(f"sending a store to {node} on {key}")
                thread = threading.Thread(
                    target=self.store,
                    args=[
                        key,
                        node,
                        (StoreAction.INSERT, DataType.File, file_direction),
                    ],
                )
                threads.append(thread)
                thread.start()
            for th in threads:
                th.join()
                nodes.pop()

    def get_data(self, data_id: str):
        print("manana lo termino")

    def refresh_buckets(self):
        while True:
            for bucket in self.routing_table.buckets:
                for node in bucket.get_nodes():
                    self.ping(node)
            time.sleep(700)

    def start(self):
        self.network.start()
        refresh_thread = threading.Thread(target=self.refresh_buckets)
        refresh_thread.start()
        print("starting bucket refresh subroutine")
