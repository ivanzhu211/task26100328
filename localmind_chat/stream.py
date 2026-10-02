"""ReplyStream：调用方读取流式回复的句柄。"""

from __future__ import annotations

from collections.abc import Iterator

from .generation import Generation
from .models import ReplyEvent
from .models import ReplyEventKind


class ReplyStream:
    """迭代产生 ReplyEvent；调用方中途停止迭代不影响生成本身。

    事件序列保证以恰好一个 DONE 或 FAILED 事件结束。
    """

    def __init__(self, events: Iterator[ReplyEvent]) -> None:
        self._events = events

    def __iter__(self) -> Iterator[ReplyEvent]:
        return self._events

    @classmethod
    def from_generation(cls, generation: Generation) -> "ReplyStream":
        return cls(generation.events_from(0))

    @classmethod
    def from_completed(cls, text: str) -> "ReplyStream":
        def events() -> Iterator[ReplyEvent]:
            if text:
                yield ReplyEvent(kind=ReplyEventKind.CHUNK, text=text)
            yield ReplyEvent(kind=ReplyEventKind.DONE, text=text)

        return cls(events())

    def collect(self) -> ReplyEvent:
        """消费到结束，返回终止事件（DONE 含完整文本，FAILED 含错误）。"""
        terminal: ReplyEvent | None = None
        for event in self._events:
            terminal = event
        if terminal is None or terminal.kind is ReplyEventKind.CHUNK:
            raise RuntimeError("stream ended without a terminal event")
        return terminal
