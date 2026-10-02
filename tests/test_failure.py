from __future__ import annotations

import threading
import time

from .conftest import ScriptedModel, read_stream

CHUNKS = ["aa", "bb", "cc", "dd"]


def test_concurrent_same_key_submissions_hit_mid_generation_error(make_service):
    """验收场景二：同键并发提交，正好撞上生成中途出错。

    模型只调用一次；所有读者拿到已生成部分 + 失败结果；
    没有人一直等不到结束；失败轮次不进历史；同键可重试。
    """
    model = ScriptedModel(list(CHUNKS), delay=0.05, error_at=2, fail_calls=1)
    service = make_service(model)
    conv = service.create_conversation()

    first = service.submit_message(conv.id, "q", "k")
    assert model.started.wait(timeout=5)

    # 在生成进行到一半时，多个调用方带着同一个键同时进来
    barrier = threading.Barrier(8)
    results: dict[int, dict] = {}

    def late_submitter(index: int) -> None:
        barrier.wait(timeout=10)
        results[index] = read_stream(service.submit_message(conv.id, "q", "k"))

    threads = [
        threading.Thread(target=late_submitter, args=(i,)) for i in range(8)
    ]
    for thread in threads:
        thread.start()

    first_result = read_stream(first)
    for thread in threads:
        thread.join(timeout=30)

    assert model.calls == 1

    # 所有读者（包括发起方）都收到已生成的前缀和失败结果
    assert first_result["failed"]
    assert first_result["text"] == "aabb"
    assert "model exploded" in first_result["terminal"].error
    for index in range(8):
        assert results[index]["failed"]
        assert results[index]["text"] in ("", "aa", "aabb")
        assert results[index]["terminal"].error

    # 失败轮次不进历史，也不占轮次额度
    assert service.get_history(conv.id) == []

    # 同一个键重试：重新生成并成功
    retry = read_stream(service.submit_message(conv.id, "q", "k"))
    assert retry["done"]
    assert retry["text"] == "aabbccdd"
    assert model.calls == 2
    history = service.get_history(conv.id)
    assert len(history) == 1
    assert history[0].assistant_reply == "aabbccdd"


def test_error_does_not_leave_conversation_busy(make_service):
    model = ScriptedModel(["x", "y"], delay=0.02, error_at=1, fail_calls=1)
    service = make_service(model)
    conv = service.create_conversation()

    failed = read_stream(service.submit_message(conv.id, "q", "k1"))
    assert failed["failed"]

    # 失败后对话立即可用，其他键可以提交
    ok = read_stream(service.submit_message(conv.id, "q2", "k2"))
    assert ok["done"]
    assert model.calls == 2


def test_model_raises_immediately(make_service):
    model = ScriptedModel(["x"], error_at=0, fail_calls=1)
    service = make_service(model)
    conv = service.create_conversation()

    result = read_stream(service.submit_message(conv.id, "q", "k1"))
    assert result["failed"]
    assert result["text"] == ""
    assert service.get_history(conv.id) == []
