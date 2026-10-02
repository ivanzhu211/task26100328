"""ChatService：上层对话服务的入口。"""

from __future__ import annotations

import threading
import time
from collections.abc import Callable, Iterator

from .errors import (
    ConversationBusyError,
    ConversationFullError,
    ConversationNotFoundError,
    IdempotencyConflictError,
)
from .generation import Generation, GenerationRegistry
from .keys import KeyStatus, KeyStore
from .models import Conversation, ModelMessage, Turn
from .store import Store
from .stream import ReplyStream

ModelFn = Callable[[list[ModelMessage]], Iterator[str]]


class ChatService:
    MAX_TURNS = 200
    CONTEXT_TURNS = 20
    KEY_TTL_SECONDS = 24 * 3600

    def __init__(self, db_path: str, model: ModelFn) -> None:
        self._store = Store(db_path)
        self._keys = KeyStore(db_path)
        self._keys.reset_stale_generating()
        self._model = model
        self._registry = GenerationRegistry()
        # 串行化“查键/查忙 -> 登记生成”的检查-登记序列，
        # 保证同一键、同一对话的并发提交只产生一次生成。
        self._submit_lock = threading.Lock()

    def close(self) -> None:
        self._store.close()
        self._keys.close()

    def create_conversation(self) -> Conversation:
        return self._store.create_conversation()

    def get_history(self, conversation_id: str) -> list[Turn]:
        if self._store.get_conversation(conversation_id) is None:
            raise ConversationNotFoundError(conversation_id)
        return self._store.list_turns(conversation_id)

    def submit_message(
        self, conversation_id: str, content: str, idempotency_key: str
    ) -> ReplyStream:
        if self._store.get_conversation(conversation_id) is None:
            raise ConversationNotFoundError(conversation_id)
        with self._submit_lock:
            record = self._keys.get(idempotency_key)
            fresh = (
                record is not None
                and time.time() - record.created_at < self.KEY_TTL_SECONDS
            )
            if fresh and record.status is KeyStatus.COMPLETED:
                self._check_conflict(record, conversation_id, content)
                return ReplyStream.from_completed(record.response or "")
            if fresh and record.status is KeyStatus.GENERATING:
                self._check_conflict(record, conversation_id, content)
                generation = self._registry.get_by_key(idempotency_key)
                if generation is not None:
                    return ReplyStream.from_generation(generation)
                # 记录与注册表不一致（如生成刚结束）：按新提交处理。
            return self._start_generation(conversation_id, content, idempotency_key)

    @staticmethod
    def _check_conflict(record, conversation_id: str, content: str) -> None:
        if record.conversation_id != conversation_id or record.content != content:
            raise IdempotencyConflictError(record.key)

    def _start_generation(
        self, conversation_id: str, content: str, idempotency_key: str
    ) -> ReplyStream:
        if self._store.count_turns(conversation_id) >= self.MAX_TURNS:
            raise ConversationFullError(conversation_id)
        if self._registry.get_by_conversation(conversation_id) is not None:
            raise ConversationBusyError(conversation_id)
        self._keys.begin_generating(idempotency_key, conversation_id, content)
        generation = Generation(idempotency_key, conversation_id, content)
        self._registry.register(generation)
        thread = threading.Thread(
            target=self._run_generation, args=(generation,), daemon=True
        )
        thread.start()
        return ReplyStream.from_generation(generation)

    def _run_generation(self, generation: Generation) -> None:
        conversation_id = generation.conversation_id
        try:
            context = self._build_context(conversation_id, generation.user_message)
            for chunk in self._model(context):
                generation.append_chunk(chunk)
        except Exception as exc:  # 模型中途出错：本轮不进历史，键可重试
            self._keys.mark_failed(generation.idempotency_key)
            generation.fail(str(exc) or exc.__class__.__name__)
            self._registry.unregister(generation)
            return
        full_text = generation.full_text
        try:
            # 先落库再向读者宣告完成：已宣告完成的回复一条不丢。
            self._store.append_turn(
                conversation_id, generation.user_message, full_text
            )
            self._keys.mark_completed(generation.idempotency_key, full_text)
        except Exception as exc:
            self._keys.mark_failed(generation.idempotency_key)
            generation.fail(f"persist failed: {exc}")
            self._registry.unregister(generation)
            return
        generation.finish()
        self._registry.unregister(generation)

    def _build_context(
        self, conversation_id: str, user_message: str
    ) -> list[ModelMessage]:
        turns = self._store.list_turns(conversation_id, limit=self.CONTEXT_TURNS)
        messages: list[ModelMessage] = []
        for turn in turns:
            messages.append(ModelMessage(role="user", content=turn.user_message))
            messages.append(ModelMessage(role="assistant", content=turn.assistant_reply))
        messages.append(ModelMessage(role="user", content=user_message))
        return messages
