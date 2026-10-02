import threading
import time

import pytest

from localmind_chat import (
    ChatService,
    Chunk,
    Completed,
    ConversationBusyError,
    ConversationNotFoundError,
    Failed,
    IdempotencyConflictError,
    TurnLimitExceededError,
)


def make_service(tmp_path, model):
    return ChatService(str(tmp_path / "chat.db"), model)


def collect(iterator):
    """消费一条事件流，返回 (拼接文本, 终止事件)。"""
    parts = []
    terminal = None
    for event in iterator:
        if isinstance(event, Chunk):
            parts.append(event.text)
        else:
            terminal = event
    return "".join(parts), terminal


def instant_model(ctx):
    yield "回复"


def test_basic_submit_and_history(tmp_path):
    svc = make_service(tmp_path, instant_model)
    cid = svc.create_conversation()
    text, terminal = collect(svc.submit(cid, "你好", "k1"))
    assert text == "回复"
    assert isinstance(terminal, Completed) and terminal.reply == "回复"
    history = svc.history(cid)
    assert len(history) == 1
    assert history[0].user_message == "你好"
    assert history[0].assistant_reply == "回复"
    svc.close()


def test_history_of_missing_conversation(tmp_path):
    svc = make_service(tmp_path, instant_model)
    with pytest.raises(ConversationNotFoundError):
        svc.history(999)
    with pytest.raises(ConversationNotFoundError):
        svc.submit(999, "x", "k")
    svc.close()


def test_same_key_replay_without_model_call(tmp_path):
    calls = []

    def model(ctx):
        calls.append(1)
        yield "一次"

    svc = make_service(tmp_path, model)
    cid = svc.create_conversation()
    collect(svc.submit(cid, "q", "k"))
    text, terminal = collect(svc.submit(cid, "q", "k"))
    assert text == "一次" and isinstance(terminal, Completed)
    assert len(calls) == 1
    svc.close()


def test_same_key_different_message_or_conversation_rejected(tmp_path):
    svc = make_service(tmp_path, instant_model)
    cid1 = svc.create_conversation()
    cid2 = svc.create_conversation()
    collect(svc.submit(cid1, "q", "k"))
    with pytest.raises(IdempotencyConflictError):
        svc.submit(cid1, "别的消息", "k")
    with pytest.raises(IdempotencyConflictError):
        svc.submit(cid2, "q", "k")
    svc.close()


def test_conversation_busy_rejects_other_keys(tmp_path):
    entered = threading.Event()
    release = threading.Event()

    def model(ctx):
        entered.set()
        release.wait(5)
        yield "慢回复"

    svc = make_service(tmp_path, model)
    cid = svc.create_conversation()
    other = svc.create_conversation()
    stream = svc.submit(cid, "q", "k1")
    assert entered.wait(5)
    with pytest.raises(ConversationBusyError):
        svc.submit(cid, "q2", "k2")
    # 别的对话不受影响
    text, terminal = collect(svc.submit(other, "q", "k3"))
    assert isinstance(terminal, Completed)
    release.set()
    text, terminal = collect(stream)
    assert text == "慢回复" and isinstance(terminal, Completed)
    svc.close()


def test_multiple_readers_join_midway_some_disconnect(tmp_path):
    chunks = [f"seg{i}" for i in range(10)]
    calls = []
    tick = threading.Event()

    def model(ctx):
        calls.append(1)
        for piece in chunks:
            tick.wait(0.02)
            yield piece

    svc = make_service(tmp_path, model)
    cid = svc.create_conversation()
    full_text = "".join(chunks)
    completed_texts = []
    errors = []

    def reader(stop_after=None, delay=0.0):
        time.sleep(delay)
        try:
            parts = []
            for event in svc.submit(cid, "q", "k"):
                if isinstance(event, Chunk):
                    parts.append(event.text)
                    if stop_after is not None and len(parts) >= stop_after:
                        return  # 读到一半断开
                elif isinstance(event, Completed):
                    completed_texts.append(("".join(parts), event.reply))
                    return
                else:
                    errors.append(event)
                    return
        except Exception as exc:  # pragma: no cover
            errors.append(exc)

    threads = [
        threading.Thread(target=reader),                       # 从头读到尾
        threading.Thread(target=reader, kwargs={"stop_after": 3}),  # 读 3 段断开
        threading.Thread(target=reader, kwargs={"delay": 0.05}),    # 中途接入
        threading.Thread(target=reader, kwargs={"stop_after": 1, "delay": 0.03}),
        threading.Thread(target=reader, kwargs={"delay": 0.12}),    # 更晚接入
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join(10)
    # 生成结束后再来一个迟到的调用方
    text, terminal = collect(svc.submit(cid, "q", "k"))
    assert text == full_text and isinstance(terminal, Completed)

    assert not errors
    assert len(calls) == 1, "模型只能被调用一次"
    assert len(completed_texts) == 3
    for joined, reply in completed_texts:
        assert joined == full_text, "每个读到结束的调用方拼出的文本必须逐字一致"
        assert reply == full_text
    assert svc.history(cid)[0].assistant_reply == full_text
    svc.close()


def test_concurrent_submit_racing_mid_failure(tmp_path):
    first_started = threading.Event()
    fail_now = threading.Event()
    lock = threading.Lock()
    stats = {"active": 0, "max_active": 0, "calls": 0}

    def model(ctx):
        with lock:
            stats["active"] += 1
            stats["max_active"] = max(stats["max_active"], stats["active"])
            stats["calls"] += 1
            call_no = stats["calls"]
        try:
            if call_no == 1:
                first_started.set()
                yield "半截"
                fail_now.wait(5)
                raise RuntimeError("模型炸了")
            yield "重来之后的完整回复"
        finally:
            with lock:
                stats["active"] -= 1

    svc = make_service(tmp_path, model)
    cid = svc.create_conversation()

    outcomes = []

    def submit_and_read():
        text, terminal = collect(svc.submit(cid, "q", "k"))
        outcomes.append((text, terminal))

    first = threading.Thread(target=submit_and_read)
    first.start()
    assert first_started.wait(5)

    racers = [threading.Thread(target=submit_and_read) for _ in range(20)]
    for t in racers:
        t.start()
    fail_now.set()  # 在 20 个同键提交涌入的同时让生成出错
    for t in [first, *racers]:
        t.join(10)
        assert not t.is_alive(), "不能有调用方一直等不到结束"

    assert stats["max_active"] == 1, "同一键不能出现两路同时生成"
    for text, terminal in outcomes:
        assert isinstance(terminal, (Failed, Completed))
        if isinstance(terminal, Failed):
            assert text == "半截"  # 读到已生成部分后收到失败
        else:
            assert text == "重来之后的完整回复" == terminal.reply

    # 历史里只可能出现完整轮次：要么还没有，要么已是重试成功的完整回复
    history = svc.history(cid)
    assert len(history) <= 1
    if history:
        assert history[0].assistant_reply == "重来之后的完整回复"
    # 同键可重试到成功
    deadline = time.time() + 5
    while True:
        text, terminal = collect(svc.submit(cid, "q", "k"))
        if isinstance(terminal, Completed):
            break
        assert time.time() < deadline
    assert text == "重来之后的完整回复"
    assert len(svc.history(cid)) == 1
    svc.close()


def test_failure_does_not_consume_turn_or_history(tmp_path):
    attempts = []

    def model(ctx):
        attempts.append(1)
        if len(attempts) == 1:
            yield "废"
            raise ValueError("bad")
        yield "好"

    svc = make_service(tmp_path, model)
    cid = svc.create_conversation()
    text, terminal = collect(svc.submit(cid, "q", "k"))
    assert text == "废" and isinstance(terminal, Failed)
    assert svc.history(cid) == []
    text, terminal = collect(svc.submit(cid, "q", "k"))
    assert text == "好" and isinstance(terminal, Completed)
    history = svc.history(cid)
    assert len(history) == 1 and history[0].assistant_reply == "好"
    svc.close()


def test_turn_limit(tmp_path):
    svc = make_service(tmp_path, instant_model)
    cid = svc.create_conversation()
    for i in range(200):
        collect(svc.submit(cid, f"q{i}", f"k{i}"))
    assert len(svc.history(cid)) == 200
    with pytest.raises(TurnLimitExceededError):
        svc.submit(cid, "超了", "k200")
    svc.close()


def test_context_is_recent_20_turns_plus_current(tmp_path):
    contexts = []

    def model(ctx):
        contexts.append(list(ctx))
        yield "r"

    svc = make_service(tmp_path, model)
    cid = svc.create_conversation()
    for i in range(25):
        collect(svc.submit(cid, f"q{i}", f"k{i}"))
    ctx = contexts[-1]
    assert len(ctx) == 41  # 20 轮 * 2 + 本次消息
    assert ctx[-1].role == "user" and ctx[-1].content == "q24"
    assert ctx[0].content == "q4"  # 只保留最近 20 轮（q4..q23）
    assert ctx[1].content == "r"
    svc.close()


def test_expired_key_treated_as_new(tmp_path):
    calls = []

    def model(ctx):
        calls.append(1)
        yield f"r{len(calls)}"

    svc = make_service(tmp_path, model)
    cid = svc.create_conversation()
    collect(svc.submit(cid, "q", "k"))
    # 把键的创建时间改到 25 小时前，模拟过期
    svc._store._conn.execute(
        "UPDATE submissions SET created_at=? WHERE idempotency_key='k'",
        (time.time() - 25 * 3600,),
    )
    svc._store._conn.commit()
    text, terminal = collect(svc.submit(cid, "q", "k"))
    assert isinstance(terminal, Completed)
    assert len(calls) == 2, "过期后同一个键按新提交处理"
    svc.close()


def test_slow_reader_does_not_block_generation_or_others(tmp_path):
    def model(ctx):
        for i in range(30):
            yield f"c{i}"

    svc = make_service(tmp_path, model)
    cid = svc.create_conversation()
    slow_stream = svc.submit(cid, "q", "k")
    slow_iter = iter(slow_stream)
    next(slow_iter)  # 慢读者只读一段就卡住

    start = time.monotonic()
    text, terminal = collect(svc.submit(cid, "q", "k"))  # 快读者
    fast_elapsed = time.monotonic() - start
    assert isinstance(terminal, Completed)
    assert text == "".join(f"c{i}" for i in range(30))
    assert fast_elapsed < 2, "快读者不能被慢读者拖住"

    # 慢读者慢慢读完，内容依然完整一致
    parts = ["c0"]
    terminal = None
    for event in slow_iter:
        if isinstance(event, Chunk):
            parts.append(event.text)
        else:
            terminal = event
    assert isinstance(terminal, Completed)
    assert "".join(parts) == text
    svc.close()


def test_conversations_generate_in_parallel(tmp_path):
    def model(ctx):
        time.sleep(0.2)
        yield "done"

    svc = make_service(tmp_path, model)
    cids = [svc.create_conversation() for _ in range(20)]
    start = time.monotonic()

    def run(cid, i):
        text, terminal = collect(svc.submit(cid, "q", f"k{i}"))
        assert isinstance(terminal, Completed) and text == "done"

    threads = [threading.Thread(target=run, args=(cid, i)) for i, cid in enumerate(cids)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(10)
        assert not t.is_alive()
    elapsed = time.monotonic() - start
    assert elapsed < 2, f"20 个对话并行生成应远快于串行，实际 {elapsed:.2f}s"
    svc.close()
