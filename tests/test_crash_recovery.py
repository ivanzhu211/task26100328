import os
import sqlite3
import subprocess
import sys
import threading
import time

from localmind_chat import ChatService, Completed

# 被 SIGKILL 的子进程：先完成一轮（k-done），再发起一条会卡住的生成（k-stuck）。
VICTIM_SCRIPT = """\
import sys, threading, time
from localmind_chat import ChatService

def model(ctx):
    msg = ctx[-1].content
    if msg == "fast":
        yield "已完成的回复"
    else:
        yield "半截输出"
        time.sleep(3600)

svc = ChatService(sys.argv[1], model)
cid = svc.create_conversation()
list(svc.submit(cid, "fast", "k-done"))

def stuck():
    for _ in svc.submit(cid, "slow", "k-stuck"):
        pass

threading.Thread(target=stuck, daemon=True).start()
time.sleep(2)  # 让 stuck 生成跑起来并写出 generating 状态
print("READY", cid, flush=True)
time.sleep(3600)
"""


def wait_for(predicate, timeout=10):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if predicate():
            return True
        time.sleep(0.05)
    return False


def test_kill_mid_generation_then_restart(tmp_path):
    db = str(tmp_path / "chat.db")
    env = dict(os.environ)
    env["PYTHONPATH"] = os.pathsep.join(
        [os.path.dirname(os.path.dirname(__file__)), env.get("PYTHONPATH", "")]
    )
    proc = subprocess.Popen(
        [sys.executable, "-c", VICTIM_SCRIPT, db],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        env=env,
    )
    try:
        line = proc.stdout.readline()
        assert line.startswith("READY"), proc.stderr.read()
        cid = int(line.split()[1])
        # 等 k-stuck 在库里呈 generating，然后杀掉进程
        def stuck_generating():
            conn = sqlite3.connect(db)
            try:
                row = conn.execute(
                    "SELECT status FROM submissions WHERE idempotency_key='k-stuck'"
                ).fetchone()
                return row is not None and row[0] == "generating"
            finally:
                conn.close()

        assert wait_for(stuck_generating)
    finally:
        proc.kill()
        proc.wait(10)
    assert proc.returncode != 0

    # 重启：新进程、新模型。已完成的回复必须还在且重放不调模型。
    model_calls = []

    def new_model(ctx):
        model_calls.append(ctx[-1].content)
        yield "重启后的新回复"

    svc = ChatService(db, new_model)
    try:
        history = svc.history(cid)
        assert len(history) == 1, "半截的轮次不能进历史"
        assert history[0].user_message == "fast"
        assert history[0].assistant_reply == "已完成的回复"

        # 原键重放已完成回复，不调用模型
        events = list(svc.submit(cid, "fast", "k-done"))
        assert isinstance(events[-1], Completed)
        assert events[-1].reply == "已完成的回复"
        assert model_calls == []

        # 被杀时正在生成的键：对话不卡死，同键可重新生成
        events = list(svc.submit(cid, "slow", "k-stuck"))
        assert isinstance(events[-1], Completed)
        assert events[-1].reply == "重启后的新回复"
        assert model_calls == ["slow"]

        # 对话彻底恢复，可继续新提交
        events = list(svc.submit(cid, "再来", "k-new"))
        assert isinstance(events[-1], Completed)
        assert len(svc.history(cid)) == 3
    finally:
        svc.close()
