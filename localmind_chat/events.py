"""流式事件与数据结构定义。"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Union


@dataclass(frozen=True)
class Message:
    """上下文中的一条消息，role 为 user 或 assistant。"""

    role: str
    content: str


@dataclass(frozen=True)
class Turn:
    """对话历史中的一轮完整问答。"""

    seq: int
    user_message: str
    assistant_reply: str
    idempotency_key: str
    created_at: float


@dataclass(frozen=True)
class Chunk:
    """回复的一个增量片段。"""

    text: str


@dataclass(frozen=True)
class Completed:
    """生成成功结束。reply 为完整回复文本。"""

    reply: str


@dataclass(frozen=True)
class Failed:
    """生成失败。此前已收到的 Chunk 为已生成的部分内容。"""

    error: str


StreamEvent = Union[Chunk, Completed, Failed]
