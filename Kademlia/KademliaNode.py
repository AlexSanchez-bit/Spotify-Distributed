from collections import defaultdict
from copyreg import pickle
import json
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
        print("en el node lookup")
        shortlist = self.routing_table.find_closest_nodes(target_id, K)
        already_queried = set()
        closest_nodes = []

        while shortlist:
            print("kademlia:lookup: short list", shortlist)
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
            print("kademlia:lookup: contenido de la shortlist: ", shortlist)
            print("kademlia:lookup: already queried: ", list(already_queried))
            print("kademlia:lookup closest nodes: ", closest_nodes)

            # Check if the closest nodes list has stabilized
            if all(node.id in already_queried for node in closest_nodes):
                break
        print("kademlia:lookup: resultado del node_lookup: ", closest_nodes)
        return closest_nodes

    def _query_node(self, node: Node, target_id: int, shortlist: List[Node]):
        self.find_node(target_id, node)
        response = self._wait_for_response(target_id)
        print("kademlia:lookup: wait for response dio: ", response)
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
        print("kademlia:lookup: timeout passed")
        return []

    # manage data storage

    def store_playlist(self, action: StoreAction, playlistData: Playlist):
        key = sha1_hash(playlistData.id)
        nodes = self.node_lookup(key)
        resp = self.send_store_playlist(nodes, key, action, playlistData)
        return resp

    def send_store_playlist(
        self, nodes, key, action: StoreAction, playlistData: Playlist
    ):
        threads = []
        responses = []
        while len(nodes) > 0:
            for node in nodes[:alpha]:
                print(f"kademlia:lookup: sending a store to {node} on {key}")
                thread = threading.Thread(
                    target=lambda key, node, rpc: responses.append(
                        self.store(key, node, rpc)
                    ),
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
                    if len(nodes) > 0:
                        nodes.pop()
        return responses

    def store_a_file(self, file_direction: str, key_save: Optional[int] = None):
        key = sha1_hash(file_direction) if key_save is None else key_save
        nodes = self.node_lookup(key)
        resp = self.send_store_file(nodes, key, file_direction)
        print("kademlia:lookup: respuesta del store file: ", resp)
        return key_save

    def send_store_file(self, nodes, key, file_direction):
        threads = []
        time.sleep(0.5)
        responses = []
        while len(nodes) > 0:
            for node in nodes[:alpha]:
                print(f"kademlia:lookup: sending a store to {node} on {key}")
                thread = threading.Thread(
                    target=lambda key, node, rpc: responses.append(
                        self.store(key, node, rpc)
                    ),
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
                if len(nodes) > 0:
                    nodes.pop()
        return responses

    # get values
    def get_playlist(self, playlist_id: str):
        print("kademlia:lookup: buscando la playlist: ", playlist_id)
        key = sha1_hash(playlist_id)
        nodes = self.node_lookup(key)
        if len(nodes) == 0:
            print("kademlia:lookup: El valor buscano no esta en la red")
            return None
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
        returned_values = list(filter(lambda x: x is not None, self.searched_data[key]))
        del self.searched_data[key]
        returned_values.sort(key=lambda x: x[1], reverse=True)
        ret_val, _ = returned_values[0]
        if ret_val is not None:
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
        print("kademlia:lookup: encontrado: ", value)

    def sincronize_peer_data(self, latest_value, data_type, original_key=None):
        if data_type is DataType.Data:
            self.store_playlist(StoreAction.UPDATE, latest_value)
        else:
            print("kademlia:lookup: archivo a actualizar: ", latest_value)
            self.store_a_file(latest_value, original_key)

    def get_a_file(self, key: int):
        print("kademlia:lookup: buscando la cancion: ", key)
        nodes = self.node_lookup(key)
        threads = []
        self.searched_data[key] = []
        while len(nodes) > 0:
            time.sleep(0.5)
            for node in nodes[:alpha]:
                print(f"kademlia:lookup: sending a find_value to", node, " for ", key)
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
        if len(returned_values) == 0:
            print("kademlia:lookup: valor no encontrado", key)
            return None
        ret_val, _ = returned_values[0]
        th = threading.Thread(
            target=self.sincronize_peer_data, args=[ret_val, DataType.File]
        )  # sincronize the highest clock in all k nearest nodes
        th.start()

        for resp_file, clock in returned_values[1:]:
            if resp_file is not None:  # remove excedent files
                os.remove(resp_file)

        return ret_val

    def get_all(self):
        all_list = set(map(lambda x: (x.id, x.title), self.database.playlists))
        short_list = self.routing_table.get_all_nodes()
        already_seen = set([])
        while len(short_list) > 0:
            threeads = []
            for node in short_list[:alpha]:
                already_seen.add(node.id)
                th = threading.Thread(
                    target=self.find_all_values, args=[node, all_list, short_list]
                )
                short_list.remove(node)
                threeads.append(th)
                th.start()
            for th in threeads:
                th.join()
            if all([node.id in already_seen for node in short_list]):
                break
        return list(all_list)

    def find_all_values(self, node: Node, all_list, nodes_list):
        values = self.find_value(-1, node, (DataType.Data, "get all"))
        self.find_node(-1, node)
        if values is not None:
            values = values[0]
            for value in values:
                all_list.add(value)
        nodes = self._wait_for_response(-1)
        if nodes is not None:
            for node in nodes:
                nodes_list.append(node)

    def refresh_buckets(self):
        while True:
            my_knowed_nodes = {}
            for i, bucket in enumerate(self.routing_table.buckets):
                for node in bucket.get_nodes():
                    print("ping: ", node)
                    if self.ping(node):
                        if i not in my_knowed_nodes:
                            my_knowed_nodes[i] = []
                        my_knowed_nodes[i].append(node.__dict__)
            json_str = json.dumps(my_knowed_nodes, indent=4)
            with open("buckets.log", "w") as file:
                file.write(json_str)
            time.sleep(5)

    def start(self):
        self.network.start()
        refresh_thread = threading.Thread(target=self.refresh_buckets)
        refresh_thread.start()
        print("starting bucket refresh subroutine")
