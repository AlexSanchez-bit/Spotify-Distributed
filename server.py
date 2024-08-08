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
    for song in data["songs"]:
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
        template = "<h4>My buckets</h4>"
        for node in kademliaNode.routing_table.get_all_nodes():
            template += f"<ul> "
            template += f"<li>{node}</li>"
            template += "</ul>"
        template += f"<h2> Current state: {kademliaNode.consensus.state}"
        template += f" {kademliaNode.consensus.leader_id}</h2>"
        return f"<h1>Hello, World! from node {kademliaNode.id}</h1>" + template

    def get_free_port():  # port sniffer
        import socket

        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.bind((kademliaNode.ip, 0))
        port = s.getsockname()[1]
        s.close()
        return port

    @app.route("/get-all-playlists", methods=["POST"])
    def get_all_playlists():
        data = request.json
        result = kademliaNode.get_all()
        to_index = data.get("to", len(result) - 1)
        from_index = data.get("from", 0)
        if result is None:
            return jsonify({"message": "No playlists found"}), 404
        return jsonify(
            {"items": result[from_index:to_index], "count": len(result)}
        ), 200

    @app.route("/get-playlist/<id>", methods=["GET"])
    def get_playlist(id):
        result = kademliaNode.get_playlist(id)
        if result is None:
            return jsonify({"message": "Playlist not found"}), 404
        return jsonify(result.to_dict()), 200

    @app.route("/add-playlist", methods=["POST"])
    def add_playlist():
        data = request.json
        new_playlist = Playlist(data["title"], data["author"], f"{time.time()}")
        songs = upload_song(data)
        new_playlist.songs = songs
        kademliaNode.store_playlist(StoreAction.INSERT, new_playlist)
        return jsonify(new_playlist.to_dict()), 201

    @app.route("/update-playlist", methods=["PUT"])
    def update_playlist():
        data = request.json
        print(data)
        playlist = Playlist(data["title"], data["author"], data["id"])
        songs = upload_song(data)
        playlist.songs = songs
        kademliaNode.store_playlist(StoreAction.UPDATE, playlist)
        return jsonify(playlist.to_dict()), 200

    @app.route("/delete-playlist/<id>", methods=["DELETE"])
    def delete_playlist(id):
        existing_playlist = Playlist("", "", id)
        kademliaNode.store_playlist(StoreAction.DELETE, existing_playlist)
        return jsonify({"message": "Playlist deleted"}), 200

    @app.route("/upload-file", methods=["POST"])
    def upload_file():
        if "file" not in request.files:
            return "No file part", 400
        file = request.files["file"]
        if file.filename == "":
            return "No selected file", 400
        if file:
            filename = str(sha1_hash(file.filename))
            file.save(os.path.join(app.config["UPLOAD_FOLDER"], f"{filename}"))
            return jsonify({"filename": filename}), 201

    @app.route("/get-song/<id>")
    def get_song(id):
        global socketio
        global sockets_ports
        port = get_free_port()
        thread = threading.Thread(target=set_socket, args=(id, port))
        thread.start()
        return jsonify({"port": port})

    def play_song(socketio, instant, id):
        resp = kademliaNode.get_a_file(id)
        if resp is None:
            return "Song Not Found", 400
        audio_file = AudioSegment.from_file(f"songs/{id}.mp3")
        wav_audio = audio_file.export(format="wav")
        wav_audio_segment = AudioSegment.from_wav(io.BytesIO(wav_audio.read()))
        chunk_size = 1000  # Tamaño del fragmento en bytes

        socketio.emit("song_info", {"size": len(wav_audio_segment) // 1000})

        @socketio.on("next_package")
        def send_packages(next_instant):
            for i in range(next_instant * 1000, len(wav_audio_segment), chunk_size)[:3]:
                chunk_audio = wav_audio_segment[i : i + chunk_size]
                chunk_bytes = io.BytesIO()
                chunk_audio.export(chunk_bytes, format="wav")
                socketio.emit(
                    "audio_chunk",
                    {"index": i // chunk_size, "data": chunk_bytes.getvalue()},
                )

        send_packages(instant)

    def set_socket(id, port):
        socketio = SocketIO(app, resources={r"/*": {"origins": "*"}}, port=port)
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

        socketio.run(
            app,
            port=port,
            debug=True,
            use_reloader=False,
            log_output=False,
            allow_unsafe_werkzeug=True,
        )

    app.run(host=kademliaNode.ip, port=54321)


create_app()
