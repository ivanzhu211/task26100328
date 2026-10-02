"""数据类型定义。"""

from __future__ import annotations

import enum
from dataclasses import dataclass


@dataclass(frozen=True)
class Conversation:
    id: str
    created_at: float


@dataclass(frozen=True)
class Turn:
    """一轮已完成的问答。只有完整轮次才会出现在历史中。"""

    conversation_id: str
    seq: int
    user_message: str
    assistant_reply: str
    created_at: float


class ReplyEventKind(enum.Enum):
    CHUNK = "chunk"
    DONE = "done"
    FAILED = "failed"


@dataclass(frozen=True)
class ReplyEvent:
    """流式回复事件：文本片段、完成（含完整文本）或失败（含错误信息）。"""

    kind: ReplyEventKind
    text: str = ""
    error: str = ""


@dataclass(frozen=True)
class ModelMessage:
    """传给模型的上下文消息。"""

    role: str  # "user" | "assistant"
    content: str
