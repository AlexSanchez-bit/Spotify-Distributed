from enum import Enum
import json
import threading
import time

from Kademlia.utils.StoreAction import StoreAction

lock = threading.Lock()


class Gender(Enum):
    Rock = "Rock"
    Jazz = "Jazz"
    Romance = "Romance"
    Balad = "Balad"
    Pop = "Pop"
    Metal = "Metal"


class Song:
    def __init__(self, name, author, key, gender: Gender):
        self.name = name
        self.author = author
        self.key = key
        self.gender = gender

    def to_dict(self):
        return {
            "name": self.name,
            "author": self.author,
            "key": self.key,
            "gender": self.gender,
        }

    @classmethod
    def from_dict(cls, data):
        song = cls(data["name"], data["author"], data["key"], data["gender"])
        song.key = data["key"]
        return song


class Playlist:
    def __init__(self, title, author, id, gender: Gender, songs=[]):
        self.title = title
        self.author = author
        self.songs = songs
        self.gender = gender
        self.id = id

    def to_dict(self):
        return {
            "title": self.title,
            "author": self.author,
            "songs": [song.to_dict() for song in self.songs],
            "gender": self.gender,
            "id": self.id,
        }

    def __eq__(self, o):
        return self.id == o.id

    @classmethod
    def from_dict(cls, data):
        playlist = cls(data["title"], data["author"], data["id"], data["gender"])
        playlist.songs = [Song.from_dict(song_data) for song_data in data["songs"]]
        return playlist

    def __str__(self):
        return f"{'{'}title:{self.title},author:{self.author}{'}'}"


class PlaylistManager:
    def __init__(self):
        saved = self.load_from_json("/tmp/data/store.json")
        self.playlists = saved.playlists if saved is not None else []
        self.actions_registered = []

    def get_all(self, filter):
        print("filtering", filter)
        if filter is not None:
            filtered_playlists = []
            for playlist in self.playlists:
                match = True
                if (
                    filter.get("title") not in [None, ""]
                    and filter["title"].lower() not in playlist.title.lower()
                ):
                    match = False
                if (
                    filter.get("author") not in [None, ""]
                    and filter["author"].lower() not in playlist.author.lower()
                ):
                    match = False
                if (
                    filter.get("gender") not in [None, ""]
                    and filter["gender"] != playlist.gender
                ):
                    match = False

                if match:
                    filtered_playlists.append(
                        (playlist.id, playlist.title, playlist.author)
                    )
            return filtered_playlists
        else:
            with lock:
                return list(map(lambda x: (x.id, x.title, x.author), self.playlists))

    def get_by_id(self, id):
        playlist = None
        with lock:
            for play in self.playlists:
                if play.id == id:
                    playlist = play
                    break
        print(
            f"database:find{id}:result",
            f"{playlist.to_dict() if playlist is not None else None}",
        )
        with lock:
            return playlist.to_dict() if playlist is not None else None

    def make_action(self, action: StoreAction, playlist_data: Playlist, order: int):
        with lock:
            self.actions_registered.append((action, playlist_data, order))
            self.actions_registered.sort(key=lambda x: x[2])

        action, playlist_data, _ = self.actions_registered.pop()
        with lock:
            if action == StoreAction.INSERT:
                self.add_playlist(playlist_data)
            elif action == StoreAction.UPDATE:
                if playlist_data not in self.playlists:
                    self.playlists.append(playlist_data)
                else:
                    self.playlists = [
                        playlist_data if item.id == playlist_data.id else item
                        for item in self.playlists
                    ]
            elif action == StoreAction.DELETE:
                print(f"before remove: {self.playlists}")
                self.playlists.remove(playlist_data)
                print(f"after remove: {self.playlists}")

        with lock:
            self.save_snapshop()
            saved = self.load_from_json("/tmp/data/store.json")
            self.playlists = saved.playlists if saved is not None else []

    def add_playlist(self, playlist: Playlist):
        self.playlists.append(playlist)

    def save_snapshop(self):
        self.save_to_json("/tmp/data/store.json")

    def to_dafter(self):
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
            json.dump(self.to_dafter(), f, indent=2)

    @classmethod
    def load_from_json(cls, filename):
        try:
            with open(filename, "r") as f:
                data = json.load(f)
            return cls.from_dict(data)
        except Exception:
            return None
