import socket
from typing import Tuple


class FileTransfer:
    def __init__(self, ip, file_direction: str, port=None):
        self.ip = ip
        self.file_direction = file_direction
        if port is None:  # if no port was provided then search for one
            port_, socket_ = self.get_free_port()
            self.port = port_
            self.socket = socket_
        else:  # else just make a binding
            self.port = port
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            # s.bind((self.ip, self.port))
            self.socket = s

    def get_free_port(self):  # port sniffer
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.bind((self.ip, 0))
        port = s.getsockname()[1]
        s.close()
        return port, s

    def start_trasmission(self, peer_address: Tuple[str, int]):
        with open(self.file_direction, "rb") as file:
            self.socket.connect(peer_address)
            self.socket.sendfile(file)

    def direction(self):
        return (self.ip, self.port)

    def receive_file(self, save_path: str):
        self.socket.listen(1)
        conn, addr = self.socket.accept()
        with open(save_path, "wb") as file:
            while True:
                data = conn.recv(4096)
                if not data:
                    break
                file.write(data)
        print(f"Archivo recibido y guardado en '{save_path}'")

    def close_transmission(self):
        self.socket.close()
