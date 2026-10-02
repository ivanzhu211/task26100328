# localmind_chat

LocalMind 对话消息提交与流式回复库。纯 Python + SQLite，无外部服务依赖，
供上层对话服务直接调用（无接口层）。

## 用法

```python
from localmind_chat import ChatService
from localmind_chat.models import ModelMessage

def my_model(context: list[ModelMessage]):
    # 上层注入的本地模型：接收上下文，分多段产出回复文本
    for piece in run_local_llm(context):
        yield piece

service = ChatService("chat.db", model=my_model)
conv = service.create_conversation()

stream = service.submit_message(conv.id, "你好", idempotency_key="req-123")
for event in stream:
    if event.kind.value == "chunk":
        push_to_frontend(event.text)   # 边生成边推
    elif event.kind.value == "done":
        finish(event.text)             # 完整回复
    else:
        report_failure(event.error)    # 生成失败

history = service.get_history(conv.id)  # 只包含完整的问答轮次
```

## 行为规则

- **幂等提交**：同一幂等键重复提交不会重复调用模型。已完成的直接返回完整回复；
  生成中的先补发已生成片段再实时接续。同键配不同对话或不同内容，抛
  `IdempotencyConflictError`。键保留 24 小时，过期按新提交处理。
- **单生成**：一个对话同一时刻只有一条回复在生成，期间其他键的提交抛
  `ConversationBusyError`；不同对话的生成完全并行。
- **轮次上限**：对话最多 200 轮完整问答，满后抛 `ConversationFullError`。
- **失败处理**：模型中途出错时，所有读者收到已生成部分 + `failed` 事件；
  该轮不进历史、不占额度，同键可重试。
- **断连安全**：调用方中途停止读取不影响生成，回复照常生成完毕并落库。
- **崩溃恢复**：生成中进程被杀，重启后对话不会卡住；已完成的回复一条不丢，
  原键重放不调用模型。

## 结构

- `localmind_chat/service.py` — `ChatService`：幂等判定、并发控制、后台生成编排
- `localmind_chat/generation.py` — 内存扇出：一次生成，任意多读者按偏移量读取
- `localmind_chat/store.py` — SQLite：对话与完整轮次
- `localmind_chat/keys.py` — SQLite：幂等键状态机（generating/completed/failed）
- `localmind_chat/stream.py` — `ReplyStream` 读者句柄
- `localmind_chat/models.py` / `errors.py` — 数据类型与异常

## 测试

```bash
python3 -m pytest tests/ -q
```

覆盖：多调用方不同时刻接入与中途断连、同键并发提交撞上生成出错、
生成中杀进程后重启、慢读者隔离、多对话并行、轮次上限、键过期等。
