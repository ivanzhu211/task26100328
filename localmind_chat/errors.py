"""库对外暴露的异常类型。"""


class ChatError(Exception):
    """本库所有业务异常的基类。"""


class ConversationNotFoundError(ChatError):
    """对话不存在。"""


class ConversationFullError(ChatError):
    """对话轮次已达上限，拒绝新提交。"""


class ConversationBusyError(ChatError):
    """对话中已有回复正在生成，拒绝新提交。"""


class IdempotencyConflictError(ChatError):
    """同一幂等键被用于不同的对话或不同的消息内容。"""
