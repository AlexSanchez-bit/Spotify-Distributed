import json

from Kademlia.utils.StoreAction import StoreAction


class Song:
    def __init__(self, name, author):
        self.name = name
        self.author = author
        self.key = self.generate_key()

    def to_dict(self):
        return {"name": self.name, "author": self.author, "key": self.key}

    @classmethod
    def from_dict(cls, data):
        song = cls(data["name"], data["author"])
        song.key = data["key"]
        return song


class Playlist:
    def __init__(self, title, author, id):
        self.title = title
        self.author = author
        self.songs = []
        self.id = id

    def to_dict(self):
        return {
            "title": self.title,
            "author": self.author,
            "songs": [song.to_dict() for song in self.songs],
            "id": self.id,
        }

    def __eq__(self, o):
        return self.id == o.id

    @classmethod
    def from_dict(cls, data):
        playlist = cls(data["title"], data["author"], data["id"])
        playlist.songs = [Song.from_dict(song_data) for song_data in data["songs"]]
        return playlist

    def __str__(self):
        return f"{'{'}title:{self.title},author:{self.author}{'}'}"


class PlaylistManager:
    def __init__(self):
        saved = self.load_from_json("../data/store.json")
        self.playlists = saved.playlists if saved is not None else []

    def get_all(self):
        return self.playlists

    def get_by_id(self, id):
        return next((item for item in self.playlists if item.id == id), None)

    def make_action(self, action: StoreAction, playlist_data: Playlist):
        if action == StoreAction.INSERT:
            self.add_playlist(playlist_data)
        elif action == StoreAction.UPDATE:
            self.playlists = [
                playlist_data if item.id == playlist_data.id else item
                for item in self.playlists
            ]
        elif action == StoreAction.DELETE:
            self.playlists.remove(playlist_data)
        self.save_snapshop()

    def add_playlist(self, playlist: Playlist):
        self.playlists.append(playlist)

    def save_snapshop(self):
        self.save_to_json("data/store.json")

    def to_dict(self):
        return [playlist.to_dict() for playlist in self.playlists]

    @classmethod
    def from_dict(cls, data):
        manager = cls()
        manager.playlists = [
            Playlist.from_dict(playlist_data) for playlist_data in data
        ]
        return manager

    # Métodos para guardar y cargar en JSON
    def save_to_json(self, filename):
        with open(filename, "w") as f:
            json.dump(self.to_dict(), f, indent=2)

    @classmethod
    def load_from_json(cls, filename):
        try:
            with open(filename, "r") as f:
                data = json.load(f)
            return cls.from_dict(data)
        except Exception:
            return None
