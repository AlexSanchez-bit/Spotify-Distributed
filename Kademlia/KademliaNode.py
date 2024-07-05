from collections import defaultdict
import os
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
        self.searched_data = {}

    # Find Nodes on the network

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
                        target=self._query_node, args=(
                            node, target_id, shortlist)
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
                if target_id in self.requested_nodes:
                    response = self.requested_nodes[target_id]
                    del self.requested_nodes[target_id]
                    return response
            time.sleep(0.1)
        print("timeout passed")
        return []

    # manage data storage

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

    def store_a_file(self, file_direction: str):
        key = sha1_hash(file_direction)
        nodes = self.node_lookup(key)
        resp = self.send_store_file(nodes, key, file_direction)
        print(resp)

    def send_store_file(self, nodes, key, file_direction):
        threads = []
        time.sleep(0.5)
        while len(nodes) > 0:
            for node in nodes[:alpha]:
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

    # get values
    def get_playlist(self, playlist_id: str):
        print("buscando la playlist: ", playlist_id)
        key = sha1_hash(playlist_id)
        nodes = self.node_lookup(key)
        threads = []
        self.searched_data[key] = []
        while len(nodes) > 0:
            time.sleep(0.5)
            for node in nodes[:alpha]:
                print(f"sending a find_value to {node} for {key}")
                thread = threading.Thread(
                    target=self.wait_for_playlist,
                    args=[
                        key,
                        node,
                        (DataType.Data, playlist_id),
                    ],
                )
                threads.append(thread)
                thread.start()
            for th in threads:
                th.join()
                nodes.pop()

        if self.searched_data[key] is None:
            del self.searched_data[key]
            return None
        returned_values = self.searched_data[key]
        del self.searched_data[key]
        returned_values.sort(key=lambda x: x[1], reverse=True)
        ret_val, _ = returned_values[0]
        th = threading.Thread(
            target=self.sincronize_peer_data, args=[ret_val, DataType.Data]
        )  # sincronize the highest clock in all k nearest nodes
        th.start()
        return ret_val

    def wait_for_playlist(self, key: int, node: Node, data: Tuple[DataType, str]):
        value = self.find_value(key, node, data)
        if value is not None:
            with lock:
                self.searched_data[key].append(value)
        print("encontrado: ", value)

    def sincronize_peer_data(self, latest_value, data_type):
        if data_type is DataType.Data:
            self.store_playlist(StoreAction.UPDATE, latest_value)
        else:
            self.store_a_file(latest_value)

    def get_a_file(self, key: int):
        print("buscando la cancion: ", key)
        nodes = self.node_lookup(key)
        threads = []
        self.searched_data[key] = []
        while len(nodes) > 0:
            time.sleep(0.5)
            for node in nodes[:alpha]:
                print(f"sending a find_value to {node} for {key}")
                thread = threading.Thread(
                    target=self.wait_for_playlist,
                    args=[
                        key,
                        node,
                        (DataType.File, "any"),
                    ],
                )
                threads.append(thread)
                thread.start()
            for th in threads:
                th.join()
                nodes.pop()
        returned_values = self.searched_data[key]
        del self.searched_data[key]
        returned_values.sort(key=lambda x: x[1], reverse=True)
        ret_val, _ = returned_values[0]
        th = threading.Thread(
            target=self.sincronize_peer_data, args=[ret_val, DataType.File]
        )  # sincronize the highest clock in all k nearest nodes
        th.start()

        for resp_file, clock in returned_values[1:]:
            if resp_file is not None:  # remove excedent files
                os.remove(resp_file)

        return ret_val

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
