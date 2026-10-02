from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

from localmind_chat import ChatService

from .conftest import read_stream

REPO_ROOT = Path(__file__).resolve().parent.parent


def test_kill_mid_generation_then_restart(tmp_path):
    """验收场景三：生成中杀掉进程，再重启。

    重启后：对话不卡在生成中；已完成的回复一条不丢，
    用原键提交拿到原回复且不调用模型；进行中的键按新提交重新生成。
    """
    db_path = str(tmp_path / "chat.db")
    env = dict(os.environ, PYTHONPATH=str(REPO_ROOT))
    child = subprocess.Popen(
        [sys.executable, "-m", "tests.crash_child", db_path],
        stdout=subprocess.PIPE,
        text=True,
        cwd=REPO_ROOT,
        env=env,
    )
    try:
        conv_id = child.stdout.readline().strip()
        assert conv_id
        assert child.stdout.readline().strip() == "READY"
        child.kill()
        child.wait(timeout=10)
        assert child.returncode == -9  # 确实是在生成中被 SIGKILL 杀掉的
    finally:
        if child.poll() is None:
            child.kill()
            child.wait(timeout=10)

    calls = 0

    def model(context):
        nonlocal calls
        calls += 1
        yield f"regenerated-{context[-1].content}"

    service = ChatService(db_path, model)
    try:
        # 已完成的回复：原键重放，不调用模型
        replay = read_stream(service.submit_message(conv_id, "fast", "done-key"))
        assert replay["done"]
        assert replay["text"] == "fast-reply"
        assert calls == 0

        # 被杀时正在生成的键：按新提交重新生成，对话没有卡住
        result = read_stream(service.submit_message(conv_id, "slow", "inflight-key"))
        assert result["done"]
        assert result["text"] == "regenerated-slow"
        assert calls == 1

        # 新键也能立即提交（对话不处于“生成中”）
        another = read_stream(service.submit_message(conv_id, "hello", "new-key"))
        assert another["done"]
        assert calls == 2

        # 历史里只有完整的轮次：fast + slow + hello
        history = service.get_history(conv_id)
        assert [t.user_message for t in history] == ["fast", "slow", "hello"]
        assert history[0].assistant_reply == "fast-reply"
    finally:
        service.close()
