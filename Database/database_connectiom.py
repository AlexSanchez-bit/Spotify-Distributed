import sqlite3


def conect_database():
    # Conectar a la base de datos
    conn = sqlite3.connect("spotify.db")
    cursor = conn.cursor()

    # Crear la tabla playlists
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS playlists (
        id INTEGER PRIMARY KEY,
        nombre TEXT NOT NULL,
        autor TEXT NOT NULL,
        imagen TEXT
    )
    """)

    # Crear la tabla songs con una clave foránea que referencia a playlists
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS songs (
        song_url TEXT NOT NULL,
        playlist INTEGER NOT NULL,
        FOREIGN KEY (playlist) REFERENCES playlists(id)
    )
    """)

    # Guardar (commit) los cambios
    conn.commit()
    return conn


def close_conection_with_database(conn):
    # Cerrar la conexión
    conn.close()
