from flask import Flask, jsonify
from flask_cors import CORS
import io
import time
import threading
from pydub import AudioSegment

# el send es para enviar eventos sin nombreado y el emit tiene nombrado
from flask_socketio import SocketIO, send, emit


def create_app():
    CORS(app, origins=["http://localhost:4321"])
    # Diccionario para almacenar los puertos de los sockets creados
    sockets_ports = {}

    @app.route("/")
    def index():
        return "Hello, World!"

    def get_free_port():  # port sniffer
        import socket

        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.bind(("localhost", 0))
        port = s.getsockname()[1]
        s.close()
        return port

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

    def play_song(socketio, instant):
        print("leyendo cancion")
        audio_file = AudioSegment.from_file("audio.mp3")
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
                chunk_audio = wav_audio_segment[i: i + chunk_size]

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
        socketio = SocketIO(
            app, cors_allowed_origins="http://localhost:4321", port=port
        )
        # Registrar el puerto asociado al id de la canción
        sockets_ports[id] = port

        @socketio.on("connect")
        def handle_connect():
            print("conexion establecida")

        @socketio.on("init_song")
        def play(instant):
            play_song(socketio, instant)

        @socketio.on("message")
        def handle_message(data):
            print("received message: " + data)
            emit("response", "pinga consorteeee")

        @socketio.on("free_port")
        def close_socket(port):
            socketio.stop()
            return "Socket cerrado", 200

        print("instanciando el socket")
        socketio.run(app, port=port, debug=True,
                     use_reloader=False, log_output=False)

    if __name__ == "__main__":
        app.run()
