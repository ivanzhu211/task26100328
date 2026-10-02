from __future__ import annotations

import threading
import time

import pytest

from localmind_chat import ChatService


class ScriptedModel:
    """可控假模型：按脚本逐段产出，可延时、可在指定位置出错。

    calls 记录被调用次数，用于验证“模型只被调用一次”。
    """

    def __init__(
        self,
        chunks: list[str],
        delay: float = 0.0,
        error_at: int | None = None,
        fail_calls: int = 0,
    ) -> None:
        self.chunks = chunks
        self.delay = delay
        self.error_at = error_at
        self.fail_calls = fail_calls
        self.calls = 0
        self.contexts: list = []
        self.started = threading.Event()
        self._lock = threading.Lock()

    def __call__(self, context):
        with self._lock:
            self.calls += 1
            call_no = self.calls
        self.contexts.append(list(context))
        self.started.set()
        should_fail = self.error_at is not None and call_no <= self.fail_calls
        for index, chunk in enumerate(self.chunks):
            if should_fail and index == self.error_at:
                raise RuntimeError("model exploded")
            if self.delay:
                time.sleep(self.delay)
            yield chunk


@pytest.fixture()
def db_path(tmp_path):
    return str(tmp_path / "chat.db")


@pytest.fixture()
def make_service(db_path):
    services = []

    def factory(model, path=None):
        service = ChatService(path or db_path, model)
        services.append(service)
        return service

    yield factory
    for service in services:
        service.close()


def read_stream(stream, stop_after_chunks: int | None = None) -> dict:
    """读取一个 ReplyStream，返回观察到的事件序列与拼接文本。

    stop_after_chunks 模拟调用方中途断开：读够指定片段数后停止迭代。
    """
    chunks: list[str] = []
    terminal = None
    for event in stream:
        if event.kind.value == "chunk":
            chunks.append(event.text)
            if stop_after_chunks is not None and len(chunks) >= stop_after_chunks:
                break
        else:
            terminal = event
    return {
        "chunks": chunks,
        "text": "".join(chunks),
        "terminal": terminal,
        "done": terminal is not None and terminal.kind.value == "done",
        "failed": terminal is not None and terminal.kind.value == "failed",
    }
