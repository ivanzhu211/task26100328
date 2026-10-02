"""SQLite 持久化层。"""

from __future__ import annotations

import sqlite3
import threading
from typing import List, Optional

from .events import Turn

SCHEMA = """
CREATE TABLE IF NOT EXISTS conversations (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    created_at REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS turns (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    conversation_id INTEGER NOT NULL REFERENCES conversations(id),
    seq INTEGER NOT NULL,
    user_message TEXT NOT NULL,
    assistant_reply TEXT NOT NULL,
    idempotency_key TEXT NOT NULL,
    created_at REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS submissions (
    idempotency_key TEXT PRIMARY KEY,
    conversation_id INTEGER NOT NULL REFERENCES conversations(id),
    message TEXT NOT NULL,
    status TEXT NOT NULL,
    reply TEXT,
    created_at REAL NOT NULL
);
"""


class Store:
    """对 SQLite 的薄封装，线程安全。"""

    def __init__(self, db_path: str) -> None:
        self._conn = sqlite3.connect(db_path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA foreign_keys=ON")
        self._lock = threading.Lock()
        with self._lock:
            self._conn.executescript(SCHEMA)
            self._conn.commit()

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    def recover_interrupted(self) -> None:
        """启动时调用：把所有 generating 状态的提交标记为 failed。"""
        with self._lock:
            self._conn.execute(
                "UPDATE submissions SET status='failed' WHERE status='generating'"
            )
            self._conn.commit()

    def create_conversation(self, now: float) -> int:
        with self._lock:
            cur = self._conn.execute(
                "INSERT INTO conversations (created_at) VALUES (?)", (now,)
            )
            self._conn.commit()
            return int(cur.lastrowid)

    def conversation_exists(self, conversation_id: int) -> bool:
        with self._lock:
            row = self._conn.execute(
                "SELECT 1 FROM conversations WHERE id=?", (conversation_id,)
            ).fetchone()
            return row is not None

    def count_turns(self, conversation_id: int) -> int:
        with self._lock:
            row = self._conn.execute(
                "SELECT COUNT(*) AS n FROM turns WHERE conversation_id=?",
                (conversation_id,),
            ).fetchone()
            return int(row["n"])

    def recent_turns(self, conversation_id: int, limit: int) -> List[Turn]:
        """按时间正序返回最近 limit 轮。"""
        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM (SELECT * FROM turns WHERE conversation_id=?"
                " ORDER BY seq DESC LIMIT ?) ORDER BY seq ASC",
                (conversation_id, limit),
            ).fetchall()
            return [self._row_to_turn(r) for r in rows]

    def list_turns(self, conversation_id: int) -> List[Turn]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM turns WHERE conversation_id=? ORDER BY seq ASC",
                (conversation_id,),
            ).fetchall()
            return [self._row_to_turn(r) for r in rows]

    def get_submission(self, key: str) -> Optional[dict]:
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM submissions WHERE idempotency_key=?", (key,)
            ).fetchone()
            return dict(row) if row is not None else None

    def delete_submission(self, key: str) -> None:
        with self._lock:
            self._conn.execute(
                "DELETE FROM submissions WHERE idempotency_key=?", (key,)
            )
            self._conn.commit()

    def insert_submission(
        self, key: str, conversation_id: int, message: str, now: float
    ) -> None:
        with self._lock:
            self._conn.execute(
                "INSERT INTO submissions "
                "(idempotency_key, conversation_id, message, status, created_at) "
                "VALUES (?, ?, ?, 'generating', ?)",
                (key, conversation_id, message, now),
            )
            self._conn.commit()

    def mark_generating(self, key: str) -> None:
        """把 failed 的提交重新置为 generating（重试）。"""
        with self._lock:
            self._conn.execute(
                "UPDATE submissions SET status='generating', reply=NULL"
                " WHERE idempotency_key=? AND status='failed'",
                (key,),
            )
            self._conn.commit()

    def has_generating(self, conversation_id: int) -> bool:
        with self._lock:
            row = self._conn.execute(
                "SELECT 1 FROM submissions "
                "WHERE conversation_id=? AND status='generating'",
                (conversation_id,),
            ).fetchone()
            return row is not None

    def complete_submission(
        self,
        key: str,
        conversation_id: int,
        message: str,
        reply: str,
        now: float,
    ) -> None:
        """事务性地写入一轮完整问答并把提交标记为 completed。"""
        with self._lock:
            row = self._conn.execute(
                "SELECT COALESCE(MAX(seq), 0) AS m FROM turns WHERE conversation_id=?",
                (conversation_id,),
            ).fetchone()
            seq = int(row["m"]) + 1
            try:
                self._conn.execute("BEGIN")
                self._conn.execute(
                    "INSERT INTO turns "
                    "(conversation_id, seq, user_message, assistant_reply, "
                    " idempotency_key, created_at) "
                    "VALUES (?, ?, ?, ?, ?, ?)",
                    (conversation_id, seq, message, reply, key, now),
                )
                self._conn.execute(
                    "UPDATE submissions SET status='completed', reply=? "
                    "WHERE idempotency_key=?",
                    (reply, key),
                )
                self._conn.commit()
            except Exception:
                self._conn.rollback()
                raise

    def fail_submission(self, key: str) -> None:
        with self._lock:
            self._conn.execute(
                "UPDATE submissions SET status='failed' WHERE idempotency_key=?",
                (key,),
            )
            self._conn.commit()

    @staticmethod
    def _row_to_turn(row: sqlite3.Row) -> Turn:
        return Turn(
            seq=int(row["seq"]),
            user_message=row["user_message"],
            assistant_reply=row["assistant_reply"],
            idempotency_key=row["idempotency_key"],
            created_at=float(row["created_at"]),
        )
