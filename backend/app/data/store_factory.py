from __future__ import annotations

from app.config import Settings
from app.data.db_peer_store import MongoPeerStore
from app.data.peer_store import PeerStore


def build_peer_store(settings: Settings):
    csv_store = PeerStore(settings)
    if not settings.mongodb_uri:
        return csv_store

    mongo_store = MongoPeerStore(settings, csv_fallback=csv_store)
    if mongo_store.ready():
        return mongo_store

    return csv_store


def configured_peer_source(settings: Settings) -> str:
    return "mongodb" if settings.mongodb_uri else "csv"
