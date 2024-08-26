import json
from Database.database_connectiom import Gender, Playlist, Song
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
        print("server:upload-song: saving: ", song)
        if "id" not in song:
            songs.append(Song.from_dict(song))
            continue
        key = int(song["id"], 16)
        key_str = song["id"]
        try:
            kademliaNode.store_a_file(f"{UPLOAD_FOLDER}/{key_str}", key)
            new_song = Song(song["name"], song["author"], key_str, song["gender"])
            songs.append(new_song)
        except Exception as e:
            print("server error on file upload: ", e)
            pass
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
        # Leer el contenido del archivo /tmp/data/store.json
        store_data = None
        try:
            with open("/tmp/data/store.json", "r") as file:
                store_data = json.load(file)
        except Exception:
            pass

        # Obtener la lista de archivos en la carpeta /tmp/songs
        songs_dir = "/tmp/songs"
        songs_files = os.listdir(songs_dir)

        # Generar la respuesta HTML
        response = (
            "<h1>Hello, World! from node"
            + f" {kademliaNode.ip} : "
            + f" {kademliaNode.id}</h1>"
        )

        response += "<h4>My buckets</h4>"
        for node in kademliaNode.routing_table.get_all_nodes():
            response += "<ul>"
            response += (
                f"<li>{kademliaNode.routing_table.get_bucket_index(node.id)} :"
                + f" {node.ip}</li>"
            )
            response += "</ul>"

        response += (
            f"<h2>Current state: {kademliaNode.consensus.state}"
            + f" {kademliaNode.consensus.leader_id}</h2>"
        )

        response += "<h3>store.json content:</h3>"
        if store_data is not None:
            response += f"<pre>{json.dumps(store_data, indent=2)}</pre>"

        response += "<h3>Songs in /tmp/songs:</h3>"
        response += "<ul>"
        for song_file in songs_files:
            response += f"<li>{song_file}</li>"
        response += "</ul>"

        return response

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
        filter = data.get("filter", None)
        result = kademliaNode.get_all(filter)
        to_index = data.get("to", len(result))
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
        return jsonify(result), 200

    @app.route("/add-playlist", methods=["POST"])
    def add_playlist():
        data = request.json
        gender = data["gender"]
        if data["gender"] is None:
            counts = {}
            for song in data["songs"]:
                if song.key not in counts:
                    counts[song.key] = 0
                else:
                    counts[song.key] += 1
            maximun = list(sorted(counts, key=lambda item: item[1]))[0]
            gender = maximun.gender

        new_playlist = Playlist(
            data["title"],
            data["author"],
            f"{time.time()}",
            gender,
        )
        songs = upload_song(data)
        new_playlist.songs = songs
        kademliaNode.store_playlist(StoreAction.INSERT, new_playlist)
        return jsonify(new_playlist.to_dict()), 201

    @app.route("/update-playlist", methods=["PUT"])
    def update_playlist():
        data = request.json
        print(data)
        playlist = Playlist(data["title"], data["author"], data["id"], data["gender"])
        songs = upload_song(data)
        playlist.songs = songs
        kademliaNode.store_playlist(StoreAction.UPDATE, playlist)
        return jsonify(playlist.to_dict()), 200

    @app.route("/delete-playlist/<id>", methods=["DELETE"])
    def delete_playlist(id):
        existing_playlist = Playlist("", "", id, Gender.Rock)
        kademliaNode.store_playlist(StoreAction.DELETE, existing_playlist)
        return jsonify({"message": "Playlist deleted"}), 200

    @app.route("/alive")
    def is_alive():
        return True, 200

    @app.route("/upload-file", methods=["POST"])
    def upload_file():
        if "file" not in request.files:
            return "No file part", 400
        file = request.files["file"]
        if file.filename == "":
            return "No selected file", 400
        if file:
            filename = hex(sha1_hash(file.filename + f"{time.time()}")).lstrip("0x")
            file.save(os.path.join(app.config["UPLOAD_FOLDER"], f"{filename}"))
            return jsonify({"filename": filename}), 201

    @app.route("/get-song/<id>", methods=["GET"])
    def get_song(id):
        host = kademliaNode.get_a_file(int(id, 16))
        if host is None:
            return "Song Not Found", 404
        return jsonify({"host": host.ip}), 200

    def play_song(socketio, instant, id):
        audio_file = AudioSegment.from_file(f"/tmp/songs/{id}.mp3")
        wav_audio = audio_file.export(format="wav")
        wav_audio_segment = AudioSegment.from_wav(io.BytesIO(wav_audio.read()))
        chunk_size = 1000  # Tamaño del fragmento en bytes

        socketio.emit("song_info", {"size": len(wav_audio_segment) // 1000})

        @socketio.on("next_package")
        def send_packages(next_instant):
            for i in range(next_instant[0] * 1000, len(wav_audio_segment), chunk_size)[
                :10
            ]:
                chunk_audio = wav_audio_segment[i : i + chunk_size]
                chunk_bytes = io.BytesIO()
                chunk_audio.export(chunk_bytes, format="wav")
                socketio.emit(
                    "audio_chunk",
                    {"index": i // chunk_size, "data": chunk_bytes.getvalue()},
                )

        send_packages([instant])

    def set_socket(id, port):
        pass

    port = kademliaNode.port
    socketio = SocketIO(app, resources={r"/*": {"origins": "*"}}, port=port)
    sockets_ports[id] = port

    @socketio.on("connect")
    def handle_connect():
        print("conexion establecida")

    @socketio.on("init_song")
    def play(params):
        [instant, id] = params
        play_song(socketio, instant, id)

    socketio.run(
        app,
        host=kademliaNode.ip,
        port=54321,
        debug=True,
        use_reloader=False,
        log_output=False,
        allow_unsafe_werkzeug=True,
    )


create_app()
