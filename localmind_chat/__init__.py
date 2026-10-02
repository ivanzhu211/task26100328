"""LocalMind 对话消息提交与流式回复库。

上层对话服务通过 ChatService 调用：
    service = ChatService(db_path, model=my_model)
    conv = service.create_conversation()
    stream = service.submit_message(conv.id, "你好", idempotency_key="k1")
    for event in stream:
        ...
"""

from .errors import (
    ChatError,
    ConversationBusyError,
    ConversationFullError,
    ConversationNotFoundError,
    IdempotencyConflictError,
)
from .models import (
    Conversation,
    ReplyEvent,
    ReplyEventKind,
    Turn,
)
from .service import ChatService
from .stream import ReplyStream

__all__ = [
    "ChatError",
    "ChatService",
    "Conversation",
    "ConversationBusyError",
    "ConversationFullError",
    "ConversationNotFoundError",
    "IdempotencyConflictError",
    "ReplyEvent",
    "ReplyEventKind",
    "ReplyStream",
    "Turn",
]
