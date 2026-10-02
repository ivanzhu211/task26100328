"""SQLite 持久化层：对话与轮次。"""

from __future__ import annotations

import sqlite3
import threading
import time
import uuid

from .models import Conversation, Turn

_SCHEMA = """
CREATE TABLE IF NOT EXISTS conversations (
    id TEXT PRIMARY KEY,
    created_at REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS turns (
    conversation_id TEXT NOT NULL,
    seq INTEGER NOT NULL,
    user_message TEXT NOT NULL,
    assistant_reply TEXT NOT NULL,
    created_at REAL NOT NULL,
    PRIMARY KEY (conversation_id, seq)
);
"""


class Store:
    def __init__(self, db_path: str) -> None:
        self._conn = sqlite3.connect(db_path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA synchronous=FULL")
        self._lock = threading.Lock()
        with self._lock:
            self._conn.executescript(_SCHEMA)

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    def create_conversation(self) -> Conversation:
        conv = Conversation(id=uuid.uuid4().hex, created_at=time.time())
        with self._lock:
            self._conn.execute(
                "INSERT INTO conversations (id, created_at) VALUES (?, ?)",
                (conv.id, conv.created_at),
            )
            self._conn.commit()
        return conv

    def get_conversation(self, conversation_id: str) -> Conversation | None:
        with self._lock:
            row = self._conn.execute(
                "SELECT id, created_at FROM conversations WHERE id = ?",
                (conversation_id,),
            ).fetchone()
        if row is None:
            return None
        return Conversation(id=row["id"], created_at=row["created_at"])

    def count_turns(self, conversation_id: str) -> int:
        with self._lock:
            row = self._conn.execute(
                "SELECT COUNT(*) AS n FROM turns WHERE conversation_id = ?",
                (conversation_id,),
            ).fetchone()
        return int(row["n"])

    def list_turns(self, conversation_id: str, limit: int | None = None) -> list[Turn]:
        sql = (
            "SELECT conversation_id, seq, user_message, assistant_reply, created_at"
            " FROM turns WHERE conversation_id = ? ORDER BY seq ASC"
        )
        params: tuple = (conversation_id,)
        if limit is not None:
            sql = (
                "SELECT * FROM ("
                "SELECT conversation_id, seq, user_message, assistant_reply, created_at"
                " FROM turns WHERE conversation_id = ? ORDER BY seq DESC LIMIT ?"
                ") ORDER BY seq ASC"
            )
            params = (conversation_id, limit)
        with self._lock:
            rows = self._conn.execute(sql, params).fetchall()
        return [
            Turn(
                conversation_id=row["conversation_id"],
                seq=row["seq"],
                user_message=row["user_message"],
                assistant_reply=row["assistant_reply"],
                created_at=row["created_at"],
            )
            for row in rows
        ]

    def append_turn(
        self, conversation_id: str, user_message: str, assistant_reply: str
    ) -> Turn:
        with self._lock:
            row = self._conn.execute(
                "SELECT COALESCE(MAX(seq), 0) AS max_seq FROM turns"
                " WHERE conversation_id = ?",
                (conversation_id,),
            ).fetchone()
            turn = Turn(
                conversation_id=conversation_id,
                seq=int(row["max_seq"]) + 1,
                user_message=user_message,
                assistant_reply=assistant_reply,
                created_at=time.time(),
            )
            self._conn.execute(
                "INSERT INTO turns"
                " (conversation_id, seq, user_message, assistant_reply, created_at)"
                " VALUES (?, ?, ?, ?, ?)",
                (
                    turn.conversation_id,
                    turn.seq,
                    turn.user_message,
                    turn.assistant_reply,
                    turn.created_at,
                ),
            )
            self._conn.commit()
        return turn
