"""被 test_restart 以子进程方式运行：完成一条回复后，在第二条生成中等待被杀。"""

from __future__ import annotations

import sys
import time

from localmind_chat import ChatService


def model(context):
    if context[-1].content == "fast":
        yield "fast-reply"
        return
    index = 0
    while True:
        time.sleep(0.01)
        yield f"c{index}"
        index += 1


def main() -> None:
    db_path = sys.argv[1]
    service = ChatService(db_path, model)
    conv = service.create_conversation()
    # 先完成一条回复：它必须在进程被杀后仍然可查
    for _ in service.submit_message(conv.id, "fast", "done-key"):
        pass
    print(conv.id, flush=True)
    # 第二条回复开始生成，输出 READY 后保持运行，等待父进程 kill -9
    for event in service.submit_message(conv.id, "slow", "inflight-key"):
        if event.kind.value == "chunk":
            print("READY", flush=True)
            break
    time.sleep(600)


if __name__ == "__main__":
    main()
