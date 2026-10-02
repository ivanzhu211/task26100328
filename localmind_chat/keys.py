"""幂等键记录的持久化：状态机为 generating -> completed / failed。"""

from __future__ import annotations

import sqlite3
import threading
import enum
import time
from dataclasses import dataclass

_SCHEMA = """
CREATE TABLE IF NOT EXISTS idempotency_keys (
    key TEXT PRIMARY KEY,
    conversation_id TEXT NOT NULL,
    content TEXT NOT NULL,
    status TEXT NOT NULL,
    response TEXT,
    created_at REAL NOT NULL
);
"""


class KeyStatus(enum.Enum):
    GENERATING = "generating"
    COMPLETED = "completed"
    FAILED = "failed"


@dataclass(frozen=True)
class KeyRecord:
    key: str
    conversation_id: str
    content: str
    status: KeyStatus
    response: str | None
    created_at: float


class KeyStore:
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

    def reset_stale_generating(self) -> None:
        """进程重启后，把遗留的 generating 记录置为 failed（可重试）。"""
        with self._lock:
            self._conn.execute(
                "UPDATE idempotency_keys SET status = ? WHERE status = ?",
                (KeyStatus.FAILED.value, KeyStatus.GENERATING.value),
            )
            self._conn.commit()

    def get(self, key: str) -> KeyRecord | None:
        with self._lock:
            row = self._conn.execute(
                "SELECT key, conversation_id, content, status, response, created_at"
                " FROM idempotency_keys WHERE key = ?",
                (key,),
            ).fetchone()
        if row is None:
            return None
        return KeyRecord(
            key=row["key"],
            conversation_id=row["conversation_id"],
            content=row["content"],
            status=KeyStatus(row["status"]),
            response=row["response"],
            created_at=row["created_at"],
        )

    def begin_generating(self, key: str, conversation_id: str, content: str) -> None:
        """把键置为 generating；不存在则插入，失败/过期的记录则覆盖重新开始。"""
        with self._lock:
            self._conn.execute(
                "INSERT INTO idempotency_keys"
                " (key, conversation_id, content, status, response, created_at)"
                " VALUES (?, ?, ?, ?, NULL, ?)"
                " ON CONFLICT(key) DO UPDATE SET"
                "   conversation_id = excluded.conversation_id,"
                "   content = excluded.content,"
                "   status = excluded.status,"
                "   response = NULL,"
                "   created_at = excluded.created_at",
                (
                    key,
                    conversation_id,
                    content,
                    KeyStatus.GENERATING.value,
                    time.time(),
                ),
            )
            self._conn.commit()

    def mark_completed(self, key: str, response: str) -> None:
        with self._lock:
            self._conn.execute(
                "UPDATE idempotency_keys SET status = ?, response = ? WHERE key = ?",
                (KeyStatus.COMPLETED.value, response, key),
            )
            self._conn.commit()

    def mark_failed(self, key: str) -> None:
        with self._lock:
            self._conn.execute(
                "UPDATE idempotency_keys SET status = ? WHERE key = ?",
                (KeyStatus.FAILED.value, key),
            )
            self._conn.commit()
