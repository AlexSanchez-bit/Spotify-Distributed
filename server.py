from Database.database_connectiom import Playlist, Song
from Kademlia.KBucket import sha1_hash
from Kademlia.utils.StoreAction import StoreAction
from KademliaNodeInit import init_node
from flask import Flask, jsonify, request
from flask_cors import CORS
import os
import io
import time
import threading
from pydub import AudioSegment

# el send es para enviar eventos sin nombreado y el emit tiene nombrado
from flask_socketio import SocketIO, send, emit

kademliaNode = init_node()

UPLOAD_FOLDER = "uploads"


def upload_song(data):
    songs = []
    print("data ", data)
    for song in data["songs"]:
        print(song)
        key = int(song["id"])
        kademliaNode.store_a_file(f"{UPLOAD_FOLDER}/{key}", key)
        new_song = Song(song["name"], song["author"], key)
        songs.append(new_song)
    return songs


def create_app():
    global kademliaNode
    app = Flask(__name__)
    CORS(app, resources={r"/*": {"origins": "*"}})
    if not os.path.exists(UPLOAD_FOLDER):
        os.makedirs(UPLOAD_FOLDER)
    app.config["UPLOAD_FOLDER"] = UPLOAD_FOLDER
    app.config["MAX_CONTENT_LENGTH"] = 16 * 1024 * 1024  # 16 MB max file size
    # Diccionario para almacenar los puertos de los sockets creados
    sockets_ports = {}

    @app.route("/")
    def index():
        my_knowed_nodes = {}
        for i, bucket in enumerate(kademliaNode.routing_table.buckets):
            for node in bucket.get_nodes():
                if i not in my_knowed_nodes:
                    my_knowed_nodes[i] = []
                my_knowed_nodes[i].append(node.__dict__)
        return f"Hello, World! from node {kademliaNode.id} look at my routing table {my_knowed_nodes}"

    def get_free_port():  # port sniffer
        import socket

        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.bind((kademliaNode.ip, 0))
        port = s.getsockname()[1]
        s.close()
        return port

    @app.route("/get-playlist/:id", methods=["GET"])
    def get_playlist(id):
        result = kademliaNode.get_playlist(id)
        if result is None:
            return "Playlist No Encontrada"
        print("encontrada: ", result)
        return result.to_dict()

    @app.route("/add-playlist", methods=["POST"])
    def set_playlist():
        data = request.json
        new_playlist = Playlist(data["title"], data["author"], f"{time.time()}")
        songs = upload_song(data)
        new_playlist.songs = songs
        print("adding playlist", new_playlist)
        kademliaNode.store_playlist(StoreAction.INSERT, new_playlist)
        return new_playlist.to_dict()

    @app.route("/upload-file", methods=["POST"])
    def upload_file():
        if "file" not in request.files:
            return "No file part"
        file = request.files["file"]
        if file.filename == "":
            return "No selected file"
        if file:
            filename = str(sha1_hash(file.filename))
            file.save(os.path.join(app.config["UPLOAD_FOLDER"], f"{filename}"))
            return f"{filename}"

    @app.route("/get-song/<int:id>")
    def get_song(id):
        global socketio
        global sockets_ports
        print("obteniendo socket")
        # Obtener el puerto libre para el socket
        print("obteniendo puerto")
        port = get_free_port()
        print("puerto", port, "libre")
        # Iniciar el socket en el puerto obtenido
        thread = threading.Thread(target=set_socket, args=(id, port))
        thread.start()
        print("devolviendo puerto")
        # Devolver el puerto como respuesta
        return jsonify({"port": port})

    def play_song(socketio, instant, id):
        print("leyendo cancion")
        kademliaNode.get_a_file(id)
        audio_file = AudioSegment.from_file(f"songs/{id}.mp3")
        print("Canción convertida a WAV")
        wav_audio = audio_file.export(format="wav")
        wav_audio_segment = AudioSegment.from_wav(io.BytesIO(wav_audio.read()))

        print("cancion leida")
        chunk_size = 1000  # Tamaño del fragmento en bytes

        socketio.emit("song_info", {"size": len(wav_audio_segment) // 1000})

        @socketio.on("next_package")
        def send_packages(next_instant):
            for i in range(next_instant * 1000, len(wav_audio_segment), chunk_size)[:3]:
                # Obtener el chunk de audio en formato WAV
                chunk_audio = wav_audio_segment[i : i + chunk_size]

                # Convertir el chunk a bytes
                chunk_bytes = io.BytesIO()
                chunk_audio.export(chunk_bytes, format="wav")
                # Enviar el chunk al cliente
                print(i // chunk_size)
                socketio.emit(
                    "audio_chunk",
                    {"index": i // chunk_size, "data": chunk_bytes.getvalue()},
                )

        send_packages(instant)
        # Notificar al cliente cuando se ha enviado todo el archivo

    def set_socket(id, port):
        socketio = SocketIO(app, resources={r"/*": {"origins": "*"}}, port=port)
        # Registrar el puerto asociado al id de la canción
        sockets_ports[id] = port

        @socketio.on("connect")
        def handle_connect():
            print("conexion establecida")

        @socketio.on("init_song")
        def play(instant):
            play_song(socketio, instant, id)

        @socketio.on("message")
        def handle_message(data):
            print("received message: " + data)
            emit("response", "pinga consorteeee")

        @socketio.on("free_port")
        def close_socket(port):
            socketio.stop()
            return "Socket cerrado", 200

        print("instanciando el socket")
        socketio.run(app, port=port, debug=True, use_reloader=False, log_output=False)

    app.run(host=kademliaNode.ip, port=54321)


create_app()
