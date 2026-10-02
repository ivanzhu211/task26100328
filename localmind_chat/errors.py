"""异常类型定义。"""


class ChatError(Exception):
    """本模块所有业务异常的基类。"""


class ConversationNotFoundError(ChatError):
    """对话不存在。"""


class IdempotencyConflictError(ChatError):
    """同一幂等键绑定了不同的对话或消息内容。"""


class ConversationBusyError(ChatError):
    """对话中已有一条回复正在生成。"""


class TurnLimitExceededError(ChatError):
    """对话轮次已达上限。"""
