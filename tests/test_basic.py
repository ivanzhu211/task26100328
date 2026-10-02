from __future__ import annotations

import sqlite3
import time

import pytest

from localmind_chat import (
    ChatService,
    ConversationBusyError,
    ConversationFullError,
    ConversationNotFoundError,
    IdempotencyConflictError,
)

from .conftest import ScriptedModel, read_stream


def test_create_conversation_and_history_empty(make_service):
    service = make_service(ScriptedModel(["ok"]))
    conv = service.create_conversation()
    assert service.get_history(conv.id) == []


def test_submit_message_returns_full_reply_and_records_turn(make_service):
    model = ScriptedModel(["你好", "，", "世界"])
    service = make_service(model)
    conv = service.create_conversation()

    result = read_stream(service.submit_message(conv.id, "打招呼", "k1"))

    assert result["done"]
    assert result["text"] == "你好，世界"
    assert result["terminal"].text == "你好，世界"
    history = service.get_history(conv.id)
    assert len(history) == 1
    assert history[0].user_message == "打招呼"
    assert history[0].assistant_reply == "你好，世界"


def test_context_contains_recent_turns_and_current_message(make_service):
    model = ScriptedModel(["r"])
    service = make_service(model)
    conv = service.create_conversation()
    read_stream(service.submit_message(conv.id, "q1", "k1"))
    read_stream(service.submit_message(conv.id, "q2", "k2"))

    context = model.contexts[-1]
    assert [(m.role, m.content) for m in context] == [
        ("user", "q1"),
        ("assistant", "r"),
        ("user", "q2"),
    ]


def test_context_limited_to_last_20_turns(make_service):
    model = ScriptedModel(["r"])
    service = make_service(model)
    conv = service.create_conversation()
    for i in range(25):
        read_stream(service.submit_message(conv.id, f"q{i}", f"k{i}"))

    context = model.contexts[-1]
    # 最近 20 轮（40 条）+ 本次消息
    assert len(context) == 41
    assert context[0].content == "q4"
    assert context[-1].content == "q24"


def test_unknown_conversation_rejected(make_service):
    service = make_service(ScriptedModel(["r"]))
    with pytest.raises(ConversationNotFoundError):
        service.submit_message("no-such-conv", "hi", "k1")
    with pytest.raises(ConversationNotFoundError):
        service.get_history("no-such-conv")


def test_conversation_busy_rejects_other_key(make_service):
    model = ScriptedModel(["a", "b", "c"], delay=0.1)
    service = make_service(model)
    conv = service.create_conversation()
    stream = service.submit_message(conv.id, "q", "k1")
    assert model.started.wait(timeout=5)

    with pytest.raises(ConversationBusyError):
        service.submit_message(conv.id, "q2", "k2")

    read_stream(stream)


def test_same_key_different_content_rejected(make_service):
    model = ScriptedModel(["a", "b"], delay=0.05)
    service = make_service(model)
    conv = service.create_conversation()
    stream = service.submit_message(conv.id, "q", "k1")
    assert model.started.wait(timeout=5)

    with pytest.raises(IdempotencyConflictError):
        service.submit_message(conv.id, "different", "k1")

    read_stream(stream)
    # 已完成之后同样拒绝
    with pytest.raises(IdempotencyConflictError):
        service.submit_message(conv.id, "different", "k1")


def test_same_key_different_conversation_rejected(make_service):
    model = ScriptedModel(["a"])
    service = make_service(model)
    conv1 = service.create_conversation()
    conv2 = service.create_conversation()
    read_stream(service.submit_message(conv1.id, "q", "k1"))

    with pytest.raises(IdempotencyConflictError):
        service.submit_message(conv2.id, "q", "k1")


def test_completed_key_replays_without_model_call(make_service):
    model = ScriptedModel(["answer"])
    service = make_service(model)
    conv = service.create_conversation()
    read_stream(service.submit_message(conv.id, "q", "k1"))
    assert model.calls == 1

    replay = read_stream(service.submit_message(conv.id, "q", "k1"))
    assert replay["done"]
    assert replay["text"] == "answer"
    assert model.calls == 1


def test_turn_limit_200(make_service):
    model = ScriptedModel(["r"])
    service = make_service(model)
    conv = service.create_conversation()
    for i in range(ChatService.MAX_TURNS):
        read_stream(service.submit_message(conv.id, f"q{i}", f"k{i}"))

    with pytest.raises(ConversationFullError):
        service.submit_message(conv.id, "one more", "k-overflow")
    assert len(service.get_history(conv.id)) == ChatService.MAX_TURNS


def test_expired_key_treated_as_new_submission(make_service, db_path):
    model = ScriptedModel(["first"])
    service = make_service(model)
    conv = service.create_conversation()
    read_stream(service.submit_message(conv.id, "q", "k1"))
    assert model.calls == 1

    # 把键的创建时间改成 25 小时前，使其过期
    conn = sqlite3.connect(db_path)
    conn.execute(
        "UPDATE idempotency_keys SET created_at = ? WHERE key = ?",
        (time.time() - 25 * 3600, "k1"),
    )
    conn.commit()
    conn.close()

    model.chunks = ["second"]
    result = read_stream(service.submit_message(conv.id, "q", "k1"))
    assert result["done"]
    assert result["text"] == "second"
    assert model.calls == 2
