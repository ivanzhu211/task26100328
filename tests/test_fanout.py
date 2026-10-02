from __future__ import annotations

import threading
import time

from .conftest import ScriptedModel, read_stream

CHUNKS = [f"chunk-{i}-" for i in range(20)]
FULL_TEXT = "".join(CHUNKS)


def _reader(service, conv_id, content, key, stop_after, results, index, delay=0.0):
    if delay:
        time.sleep(delay)
    stream = service.submit_message(conv_id, content, key)
    results[index] = read_stream(stream, stop_after_chunks=stop_after)


def test_multiple_callers_join_at_different_times_some_disconnect(make_service):
    """验收场景一：生成期间多个同键调用方在不同时刻接入，部分中途断开。

    模型只调用一次；每个读到结束的调用方拼出的文本与保存的回复逐字一致。
    """
    model = ScriptedModel(list(CHUNKS), delay=0.02)
    service = make_service(model)
    conv = service.create_conversation()

    results: dict[int, dict] = {}
    threads = [
        threading.Thread(  # 0: 从头读到尾
            target=_reader, args=(service, conv.id, "q", "k", None, results, 0)
        ),
        threading.Thread(  # 1: 读到 5 段后断开
            target=_reader, args=(service, conv.id, "q", "k", 5, results, 1)
        ),
        threading.Thread(  # 2: 中途接入，读到尾
            target=_reader, args=(service, conv.id, "q", "k", None, results, 2, 0.15)
        ),
        threading.Thread(  # 3: 中途接入，读 3 段后断开
            target=_reader, args=(service, conv.id, "q", "k", 3, results, 3, 0.2)
        ),
        threading.Thread(  # 4: 快结束时接入，读到尾
            target=_reader, args=(service, conv.id, "q", "k", None, results, 4, 0.3)
        ),
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=30)

    assert model.calls == 1

    # 断开的读者：拿到的是已生成部分的前缀
    assert results[1]["text"] == "".join(CHUNKS[:5])
    assert results[1]["terminal"] is None
    assert results[3]["chunks"] == CHUNKS[:3]

    # 读到结束的读者：逐字一致，且与保存的回复一致
    history = service.get_history(conv.id)
    assert len(history) == 1
    saved = history[0].assistant_reply
    assert saved == FULL_TEXT
    for index in (0, 2, 4):
        assert results[index]["done"]
        assert results[index]["text"] == saved
        assert results[index]["terminal"].text == saved

    # 断开的调用方不影响生成落库；同键再提交直接拿到完整回复
    replay = read_stream(service.submit_message(conv.id, "q", "k"))
    assert replay["done"]
    assert replay["text"] == saved
    assert model.calls == 1


def test_slow_reader_does_not_block_generation_or_others(make_service):
    model = ScriptedModel(list(CHUNKS), delay=0.01)
    service = make_service(model)
    conv = service.create_conversation()

    slow_result: dict = {}

    def slow_reader():
        chunks = []
        terminal = None
        for event in service.submit_message(conv.id, "q", "k"):
            if event.kind.value == "chunk":
                chunks.append(event.text)
                time.sleep(0.05)  # 读得很慢
            else:
                terminal = event
        slow_result["text"] = "".join(chunks)
        slow_result["terminal"] = terminal

    thread = threading.Thread(target=slow_reader)
    start = time.monotonic()
    thread.start()
    assert model.started.wait(timeout=5)
    time.sleep(0.05)

    # 快读者在慢读者还没读完时就已读到结束
    fast = read_stream(service.submit_message(conv.id, "q", "k"))
    fast_elapsed = time.monotonic() - start
    assert fast["done"]
    assert fast["text"] == FULL_TEXT
    assert fast_elapsed < 0.8  # 生成本身约 0.2s，不被慢读者拖累

    thread.join(timeout=30)
    # 慢读者最终也拿到逐字一致的完整文本
    assert slow_result["text"] == FULL_TEXT
    assert slow_result["terminal"].text == FULL_TEXT


def test_conversations_generate_in_parallel(make_service):
    """不同对话互不等待：10 个对话各需 0.3s，总耗时应远小于串行。"""
    model = ScriptedModel(["x"] * 6, delay=0.05)
    service = make_service(model)
    convs = [service.create_conversation() for _ in range(10)]

    start = time.monotonic()
    streams = [
        service.submit_message(conv.id, "q", f"k{i}")
        for i, conv in enumerate(convs)
    ]
    results = [read_stream(stream) for stream in streams]
    elapsed = time.monotonic() - start

    assert all(result["done"] for result in results)
    assert model.calls == 10
    assert elapsed < 3.0
    for conv in convs:
        assert len(service.get_history(conv.id)) == 1


def test_dozens_of_simultaneous_same_key_submissions_single_model_call(make_service):
    """同一个键被几十个调用方同时首次提交，模型只被调用一次。"""
    model = ScriptedModel(list(CHUNKS), delay=0.01)
    service = make_service(model)
    conv = service.create_conversation()

    barrier = threading.Barrier(30)
    results: dict[int, dict] = {}

    def submitter(index: int) -> None:
        barrier.wait(timeout=10)
        results[index] = read_stream(service.submit_message(conv.id, "q", "k"))

    threads = [threading.Thread(target=submitter, args=(i,)) for i in range(30)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=30)

    assert model.calls == 1
    saved = service.get_history(conv.id)[0].assistant_reply
    assert saved == FULL_TEXT
    for index in range(30):
        assert results[index]["done"]
        assert results[index]["text"] == saved
