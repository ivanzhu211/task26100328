"""对话服务：提交消息、流式回复、幂等与并发控制。"""

from __future__ import annotations

import threading
import time
from typing import Callable, Dict, Iterable, Iterator, List, Optional

from .errors import (
    ConversationBusyError,
    ConversationNotFoundError,
    IdempotencyConflictError,
    TurnLimitExceededError,
)
from .events import Chunk, Completed, Failed, Message, StreamEvent, Turn
from .store import Store

# 模型类型：接收上下文消息列表，逐步产出回复文本片段；可能抛异常。
ModelFn = Callable[[List[Message]], Iterable[str]]

MAX_TURNS = 200
CONTEXT_TURNS = 20
KEY_TTL_SECONDS = 24 * 3600


class _Generation:
    """一条正在进行的生成的内存态。读者通过条件变量订阅增量。"""

    def __init__(self, key: str, conversation_id: int, message: str) -> None:
        self.key = key
        self.conversation_id = conversation_id
        self.message = message
        self.chunks: List[str] = []
        self.finished = False
        self.reply: Optional[str] = None
        self.error: Optional[str] = None
        self.cond = threading.Condition()


class ChatService:
    """上层对话服务调用的入口。"""

    def __init__(self, db_path: str, model: ModelFn) -> None:
        self._store = Store(db_path)
        self._store.recover_interrupted()
        self._model = model
        self._lock = threading.Lock()
        self._generations: Dict[str, _Generation] = {}

    def close(self) -> None:
        self._store.close()

    def create_conversation(self) -> int:
        """新建对话，返回对话 id。"""
        return self._store.create_conversation(time.time())

    def history(self, conversation_id: int) -> List[Turn]:
        """查看对话历史，只包含完整问答轮次。"""
        if not self._store.conversation_exists(conversation_id):
            raise ConversationNotFoundError(f"conversation {conversation_id} not found")
        return self._store.list_turns(conversation_id)

    def submit(
        self, conversation_id: int, message: str, idempotency_key: str
    ) -> Iterator[StreamEvent]:
        """提交一条消息，返回流式事件迭代器（Chunk* 后跟 Completed 或 Failed）。

        业务校验（对话不存在、键冲突、对话忙、轮次满）在此同步抛出；
        返回的迭代器被消费时增量产出事件，中途放弃不影响生成。
        """
        now = time.time()
        with self._lock:
            sub = self._store.get_submission(idempotency_key)
            if sub is not None and now - sub["created_at"] >= KEY_TTL_SECONDS:
                self._store.delete_submission(idempotency_key)
                self._discard_generation(idempotency_key)
                sub = None

            if sub is not None:
                if sub["conversation_id"] != conversation_id or sub["message"] != message:
                    raise IdempotencyConflictError(
                        f"idempotency key {idempotency_key!r} already bound to "
                        f"another conversation or message"
                    )
                gen = self._generations.get(idempotency_key)
                if gen is not None:
                    with gen.cond:
                        live = not gen.finished
                    if live:
                        return self._attach(gen)
                    self._discard_generation(idempotency_key)
                if sub["status"] == "completed":
                    return self._replay(sub["reply"] or "")
                # failed（或异常遗留的 generating）：按重试处理，重新生成。
                self._check_new_turn(conversation_id)
                self._store.mark_generating(idempotency_key)
                return self._start_generation(idempotency_key, conversation_id, message)

            self._check_new_turn(conversation_id)
            self._store.insert_submission(idempotency_key, conversation_id, message, now)
            return self._start_generation(idempotency_key, conversation_id, message)

    # ---- 内部 ----

    def _check_new_turn(self, conversation_id: int) -> None:
        if not self._store.conversation_exists(conversation_id):
            raise ConversationNotFoundError(f"conversation {conversation_id} not found")
        if self._store.count_turns(conversation_id) >= MAX_TURNS:
            raise TurnLimitExceededError(
                f"conversation {conversation_id} reached {MAX_TURNS} turns"
            )
        if self._conversation_busy(conversation_id):
            raise ConversationBusyError(
                f"conversation {conversation_id} has a reply being generated"
            )

    def _conversation_busy(self, conversation_id: int) -> bool:
        if self._store.has_generating(conversation_id):
            return True
        for gen in self._generations.values():
            if gen.conversation_id == conversation_id:
                with gen.cond:
                    if not gen.finished:
                        return True
        return False

    def _discard_generation(self, key: str) -> None:
        gen = self._generations.get(key)
        if gen is not None:
            with gen.cond:
                done = gen.finished
            if done:
                self._generations.pop(key, None)

    def _start_generation(
        self, key: str, conversation_id: int, message: str
    ) -> Iterator[StreamEvent]:
        gen = _Generation(key, conversation_id, message)
        self._generations[key] = gen
        thread = threading.Thread(
            target=self._run_generation, args=(gen,), daemon=True,
            name=f"localmind-gen-{key}",
        )
        thread.start()
        return self._attach(gen)

    def _run_generation(self, gen: _Generation) -> None:
        parts: List[str] = []
        try:
            context = self._build_context(gen.conversation_id, gen.message)
            for piece in self._model(context):
                text = piece if isinstance(piece, str) else str(piece)
                parts.append(text)
                with gen.cond:
                    gen.chunks.append(text)
                    gen.cond.notify_all()
            reply = "".join(parts)
            if not self._is_current(gen):
                # 键已过期被重新提交：结果不落库，但要放走本次的读者
                self._finish_readers(gen, error="superseded by a newer submission")
                return
            # 先落库再通知：已承诺完成的回复一条不丢。
            self._store.complete_submission(
                gen.key, gen.conversation_id, gen.message, reply, time.time()
            )
            self._finish_readers(gen, reply=reply)
        except Exception as exc:  # 模型或落库失败：本轮作废，可同键重试
            if not self._is_current(gen):
                self._finish_readers(gen, error="superseded by a newer submission")
                return
            try:
                self._store.fail_submission(gen.key)
            except Exception:
                pass
            self._finish_readers(gen, error=f"{type(exc).__name__}: {exc}")

    def _finish_readers(
        self, gen: _Generation, reply: Optional[str] = None, error: Optional[str] = None
    ) -> None:
        with gen.cond:
            gen.reply = reply
            gen.error = error
            gen.finished = True
            gen.cond.notify_all()

    def _is_current(self, gen: _Generation) -> bool:
        with self._lock:
            return self._generations.get(gen.key) is gen

    def _build_context(self, conversation_id: int, message: str) -> List[Message]:
        context: List[Message] = []
        for turn in self._store.recent_turns(conversation_id, CONTEXT_TURNS):
            context.append(Message(role="user", content=turn.user_message))
            context.append(Message(role="assistant", content=turn.assistant_reply))
        context.append(Message(role="user", content=message))
        return context

    def _attach(self, gen: _Generation) -> Iterator[StreamEvent]:
        def stream() -> Iterator[StreamEvent]:
            index = 0
            while True:
                with gen.cond:
                    while index >= len(gen.chunks) and not gen.finished:
                        gen.cond.wait()
                    new = gen.chunks[index:]
                    index = len(gen.chunks)
                    finished = gen.finished
                    reply = gen.reply
                    error = gen.error
                for text in new:
                    yield Chunk(text)
                if finished:
                    if error is None:
                        yield Completed(reply if reply is not None else "")
                    else:
                        yield Failed(error)
                    return

        return stream()

    @staticmethod
    def _replay(reply: str) -> Iterator[StreamEvent]:
        def stream() -> Iterator[StreamEvent]:
            if reply:
                yield Chunk(reply)
            yield Completed(reply)

        return stream()
