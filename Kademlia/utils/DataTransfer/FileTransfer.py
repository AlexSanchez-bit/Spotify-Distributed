import socket
from typing import Optional, Tuple


class FileTransfer:
    def __init__(self, ip, file_direction: Optional[str] = None):
        self.ip = ip
        self.file_direction = file_direction
        port_, socket_ = self.get_free_port()
        self.port = port_
        self.socket = socket_

    def get_free_port(self):  # port sniffer
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.bind((self.ip, 0))
        port = s.getsockname()[1]
        return port, s

    def start_trasmission(self, peer_address: Tuple[str, int]):
        try:
            with open(self.file_direction, "rb") as file:
                print(f"starting the transmission of {file}")
                self.socket.connect(peer_address)
                self.socket.sendfile(file)
        except Exception as e:
            print(e)

    def direction(self):
        return (self.ip, self.port)

    def receive_file(self, save_path: str):
        try:
            self.socket.listen(1)
            self.socket.settimeout(10)
            print("waiting for file transimission")
            conn, addr = self.socket.accept()
            with open(save_path, "wb") as file:
                while True:
                    data = conn.recv(4096)
                    if not data:
                        break
                    file.write(data)
            print(f"Archivo recibido y guardado en '{save_path}'")
        except socket.timeout:
            print("timeout exceded for conection stablishing")

    def close_transmission(self):
        print("fianalizando tansmision")
        self.socket.close()

    def __str__(self) -> str:
        return f"current from {self.ip, self.port}"
