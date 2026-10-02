"""内存中的生成任务与扇出：一个生成对应任意多个读者。"""

from __future__ import annotations

import enum
import threading
from collections.abc import Iterator

from .models import ReplyEvent, ReplyEventKind


class GenerationStatus(enum.Enum):
    RUNNING = "running"
    DONE = "done"
    FAILED = "failed"


class Generation:
    """一次回复生成。后台线程写入片段，读者各自按偏移量读取。

    片段全部缓存在内存列表中：写得快、读得慢互不影响；
    读者中途离开也不需要任何清理，生成照常进行。
    """

    def __init__(self, idempotency_key: str, conversation_id: str, user_message: str) -> None:
        self.idempotency_key = idempotency_key
        self.conversation_id = conversation_id
        self.user_message = user_message
        self._chunks: list[str] = []
        self._status = GenerationStatus.RUNNING
        self._error = ""
        self._cond = threading.Condition()

    @property
    def status(self) -> GenerationStatus:
        with self._cond:
            return self._status

    @property
    def full_text(self) -> str:
        with self._cond:
            return "".join(self._chunks)

    def append_chunk(self, text: str) -> None:
        with self._cond:
            if self._status is not GenerationStatus.RUNNING:
                return
            self._chunks.append(text)
            self._cond.notify_all()

    def finish(self) -> None:
        with self._cond:
            if self._status is GenerationStatus.RUNNING:
                self._status = GenerationStatus.DONE
                self._cond.notify_all()

    def fail(self, error: str) -> None:
        with self._cond:
            if self._status is GenerationStatus.RUNNING:
                self._status = GenerationStatus.FAILED
                self._error = error
                self._cond.notify_all()

    def events_from(self, offset: int = 0) -> Iterator[ReplyEvent]:
        """从指定片段偏移量开始产出事件，直到 DONE 或 FAILED。

        任何时刻接入的读者都先拿到已生成的全部片段，再实时接后续片段，
        因此每个读到结束的读者拼出的文本都与保存的回复逐字一致。
        """
        index = offset
        while True:
            with self._cond:
                while index >= len(self._chunks) and self._status is GenerationStatus.RUNNING:
                    self._cond.wait()
                pending = self._chunks[index:]
                status = self._status
                error = self._error
                full = "".join(self._chunks) if status is not GenerationStatus.RUNNING else ""
            for chunk in pending:
                index += 1
                yield ReplyEvent(kind=ReplyEventKind.CHUNK, text=chunk)
            if status is GenerationStatus.DONE:
                yield ReplyEvent(kind=ReplyEventKind.DONE, text=full)
                return
            if status is GenerationStatus.FAILED:
                yield ReplyEvent(kind=ReplyEventKind.FAILED, error=error)
                return


class GenerationRegistry:
    """按幂等键和对话跟踪进行中的生成（仅 RUNNING 的在册）。"""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._by_key: dict[str, Generation] = {}
        self._by_conv: dict[str, Generation] = {}

    def get_by_key(self, idempotency_key: str) -> Generation | None:
        with self._lock:
            return self._by_key.get(idempotency_key)

    def get_by_conversation(self, conversation_id: str) -> Generation | None:
        with self._lock:
            return self._by_conv.get(conversation_id)

    def register(self, generation: Generation) -> None:
        with self._lock:
            self._by_key[generation.idempotency_key] = generation
            self._by_conv[generation.conversation_id] = generation

    def unregister(self, generation: Generation) -> None:
        with self._lock:
            if self._by_key.get(generation.idempotency_key) is generation:
                del self._by_key[generation.idempotency_key]
            if self._by_conv.get(generation.conversation_id) is generation:
                del self._by_conv[generation.conversation_id]
