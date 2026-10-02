# LocalMind 对话消息提交与流式回复库

完全离线部署的对话助手 LocalMind 的消息提交与流式回复模块。以库的形式提供，
由上层对话服务直接调用，不含接口层；对话与消息持久化在 SQLite。

## 用法

```python
from localmind_chat import ChatService, Chunk, Completed, Failed

def my_model(context):
    # context: [Message(role, content), ...]，最近 20 轮已完成问答 + 本次消息
    for piece in call_local_llm(context):
        yield piece  # 分多段吐出回复文本；抛异常即视为生成失败

svc = ChatService("chat.db", my_model)

cid = svc.create_conversation()

# 提交一条消息，边生成边拿到片段；幂等键由上层生成（如客户端 UUID）
for event in svc.submit(cid, "你好", idempotency_key="msg-uuid-1"):
    if isinstance(event, Chunk):
        push_to_frontend(event.text)   # 增量片段
    elif isinstance(event, Completed):
        finish(event.reply)            # 完整回复
    else:  # Failed
        report_error(event.error)      # 之前的 Chunk 是已生成的部分

history = svc.history(cid)  # 只包含完整问答轮次
```

## 行为规则

- **幂等**：同一幂等键重复提交不再调用模型。已完成的直接重放完整回复；
  仍在生成的先补发已生成片段再实时接续。同一键配不同对话或消息内容，
  抛 `IdempotencyConflictError`。键自首次提交保留 24 小时，过期按新提交处理。
- **单生成**：一个对话同一时刻只生成一条回复；生成期间其他键的提交抛
  `ConversationBusyError`。对话最多 200 轮，满后抛 `TurnLimitExceededError`。
- **失败**：模型中途出错时，所有在读调用方收到已生成部分 + `Failed`；
  该轮不进历史、不占轮次额度；同键再次提交会重新生成。
- **断开安全**：调用方中途停止消费不影响生成，生成照常完成并落库。
- **崩溃恢复**：生成中进程被杀，重启后该对话不会卡住；已完成的回复
  用原键提交仍拿到原回复，不调用模型。
- **并发**：不同对话的生成并行进行；慢读者不会拖慢生成或其他读者。

## 测试

```bash
python3 -m pytest tests/ -q
```

覆盖：多调用方不同时刻接入同一条生成（部分中途断开）且模型只调用一次、
同键并发提交撞上生成中途出错、生成中杀进程后重启恢复、对话忙/键冲突/
轮次上限/键过期、上下文窗口、慢读者隔离、多对话并行。
