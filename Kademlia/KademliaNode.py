from collections import defaultdict
from copyreg import pickle
import json
import os
from typing import Any, List, Optional, Set, Tuple
from Database.database_connectiom import Playlist, Song
from Kademlia.KBucket import Node, K, sha1_hash
from Kademlia.KademliaRpcNode import KademliaRpcNode
import threading
import time

import threading

from Kademlia.utils.DataTransfer.FileTransfer import FileTransfer
from Kademlia.utils.DataType import DataType
from Kademlia.utils.StoreAction import StoreAction

lock = threading.Lock()
alpha = 10


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

        while len(shortlist) > 0:
            print("kademlia:lookup: short list", shortlist)
            # Sort shortlist by distance to target_id
            shortlist.sort(key=lambda node: node.id ^ target_id)
            tmp_order = shortlist + closest_nodes
            tmp_order.sort(key=lambda node: node.id ^ target_id)
            closest_nodes = list(set(tmp_order))[:K]
            # Parallel RPCs to alpha nodes
            threads = []
            for node in shortlist[:alpha]:
                if node.id not in already_queried:
                    already_queried.add(node.id)
                    thread = threading.Thread(
                        target=self._query_node, args=(node, target_id, shortlist)
                    )
                    threads.append(thread)
                    shortlist.remove(node)
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

    def _wait_for_response(self, target_id: int, timeout: int = 4) -> List[Node]:
        start_time = time.time()
        while time.time() - start_time < timeout:
            with lock:
                if target_id in self.requested_nodes:
                    response = self.requested_nodes[target_id]
                    del self.requested_nodes[target_id]
                    return response
            time.sleep(0.0001)
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
                        nodes.pop(0)
        return responses

    def store_a_file(self, file_direction: str, key_save: Optional[int] = None):
        key = sha1_hash(file_direction) if key_save is None else key_save
        print("kademlia:store-file: buscando nodos para: ", key)
        nodes = self.node_lookup(key)
        print("kademlia:store-file: nodos para: ", key, "->", nodes)
        resp = self.send_store_file(nodes, key, file_direction)
        print("kademlia:store-file: respuesta del store file: ", resp)
        return key_save

    def send_store_file(self, nodes, key, file_direction):
        threads = []
        time.sleep(0.005)
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
                    nodes.pop(0)
        return responses

    # get values
    def get_playlist(self, playlist_id: str):
        key = sha1_hash(playlist_id)
        print("kademlia:lookup: buscando la playlist: ", playlist_id, key)
        nodes = self.node_lookup(key)
        if len(nodes) == 0:
            print("kademlia:lookup: El valor buscano no esta en la red")
            return None
        threads = []
        owned = self.database.get_by_id(playlist_id)
        self.searched_data[key] = []
        if owned is not None:
            self.searched_data[key].append((owned, self.network.clock.ticks))
        while len(nodes) > 0:
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

        print("find value returned: ", self.searched_data[key])
        returned_values = list(
            filter(lambda x: x[0] is not None, self.searched_data[key])
        )
        with lock:
            del self.searched_data[key]
        returned_values.sort(key=lambda x: x[1], reverse=True)
        print("find value ordered returned: ", returned_values)
        ret_val, _ = returned_values[0] if len(returned_values) > 0 else (None, None)
        if ret_val is not None:
            th = threading.Thread(
                target=self.sincronize_peer_data, args=[ret_val, DataType.Data]
            )  # sincronize the highest clock in all k nearest nodes
            th.start()
        print(f"kademlia:find-value: devolviendo{ret_val}")
        return ret_val

    def wait_for_playlist(self, key: int, node: Node, data: Tuple[DataType, str, None]):
        value = self.find_value(key, node, data)
        if value is not None:
            with lock:
                self.searched_data[key].append(value)
        print("kademlia:playlist: encontrado: ", value)

    def sincronize_peer_data(self, latest_value, data_type, original_key=None):
        if data_type is DataType.Data:
            self.store_playlist(
                StoreAction.UPDATE,
                Playlist.from_dict(latest_value),
            )
        else:
            print("kademlia:lookup: archivo a actualizar: ", latest_value)
            filetransfer = FileTransfer(self.ip)
            filetransfer.receive_file(f"/tmp/songs/{latest_value[1]}")
            filetransfer.close_transmission()
            self.store_a_file(latest_value, original_key)

    def get_a_file(self, key: int):
        print("kademlia:get-file: buscando la cancion: ", hex(key))
        nodes = self.node_lookup(key)
        print(type(key))
        print("nodos a buscar: ", nodes)
        threads = []
        self.searched_data[key] = []
        while len(nodes) > 0:
            time.sleep(0.0005)
            for node in nodes[:alpha]:
                print(f"kademlia:get-file: sending a find_value to", node, " for ", key)
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
                nodes.pop(0)
                print("nodos restantes: ", len(nodes))
                th.join()
        returned_values = list(
            filter(lambda x: x[1] is not None and x[1], self.searched_data[key])
        )
        print("wawawa")
        with lock:
            del self.searched_data[key]
        returned_values.sort(key=lambda x: x[1], reverse=True)
        print("get-file:encontrado: ", returned_values)
        if len(returned_values) == 0:
            print("kademlia:get-file: valor no encontrado", key)
            return None
        ret_val, _, _ = returned_values[0]
        # th = threading.Thread(
        #     target=self.sincronize_peer_data, args=[
        #         (ret_val, key), DataType.File]
        # )  # sincronize the highest clock in all k nearest nodes
        # th.start()
        return ret_val

    def get_all(self, filter) -> List[Tuple[str, str, str]]:
        # Obtener todos los datos locales
        all_list = set(self.database.get_all(filter))
        short_list = set(
            self.routing_table.get_all_nodes()
        )  # Obtener todos los nodos conocidos
        already_seen = set()

        start_time = time.time()
        while len(list(short_list)) > 0:
            threads = []
            print(f"get-all: shortlist {short_list}")
            print(f"get-all: seen {already_seen}")
            print(f"get-all: allvalues {all_list}")
            print(f" ---------- get-all: elapsed-time: {time.time()-start_time}")

            # Procesar en paralelo hasta `alpha` nodos
            for node in list(short_list)[:alpha]:
                if node.id in already_seen:
                    continue
                already_seen.add(node.id)
                th = threading.Thread(
                    target=self.find_all_values,
                    args=[node, filter, all_list, short_list],
                )
                threads.append(th)
                th.start()

            # Esperar a que todos los hilos terminen
            for th in threads:
                th.join()

            # Verificar si todos los nodos han sido vistos
            if all(node.id in already_seen for node in short_list):
                break

        print(f" ---------- get-all: elapsed-time: {time.time()-start_time}")
        print("find-all result: ", all_list)
        print("find-all result: ", list(all_list))
        print("find-all nodes seen: ", list(already_seen))
        return list(all_list)

    def find_all_values(
        self,
        node: Node,
        filters,
        all_list: Set[Tuple[str, str, str]],
        nodes_list: Set[Node],
    ):
        values = self.find_value(-node.id, node, (DataType.Data, filters))
        print(f"get-all: {node} responded values: ", values)

        if values is not None:
            for value in values[0]:
                with lock:
                    all_list.add(value)  # Agregar valores a la lista total

        self.find_node(-node.id, node)
        nodes = self._wait_for_response(-node.id, timeout=1)
        if nodes is not None:
            for node in nodes:
                with lock:
                    nodes_list.add(node)
        nodes_list.remove(node)

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
            time.sleep(5000)

    def start(self):
        self.network.start()
        refresh_thread = threading.Thread(target=self.refresh_buckets)
        refresh_thread.start()
        print("starting bucket refresh subroutine")
