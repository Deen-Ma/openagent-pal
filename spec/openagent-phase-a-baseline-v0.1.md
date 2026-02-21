# OpenAgent Phase A Baseline (v0.1)

## 1. 目标与范围
本文件用于冻结 OpenClaw 与 PAL(Control Plane) 的协议边界，作为后续实现的唯一对齐基线。

包含内容：
1. 架构边界与职责。
2. PAL 对 OpenClaw 的接口契约。
3. Canonical Event 字段与约束。
4. 确定性状态机语义。
5. 治理策略和错误码。
6. Phase A 验收标准。

不包含内容：
1. libp2p 具体工程参数。
2. UI 交互细节。
3. 高级对抗安全策略（留到后续版本）。

## 2. 架构边界
### 2.1 OpenClaw (Cognitive Plane)
职责：
1. 用户交互、任务理解、工具调用、计划编排。
2. 决定是否发起开放网络协作。
3. 处理用户授权（是否允许广播）。

约束：
1. 不直接生成网络事件签名。
2. 不直接维护 `seq`。

### 2.2 PAL (Control Plane)
职责：
1. 语义编译：意图归一化、taxonomy 标注、置信度输出。
2. 主题映射：taxonomy -> topics。
3. 事件构建：`task_id/seq/op/expires_at/...`。
4. 本地状态机：版本化状态视图、收敛规则。
5. 治理策略：大小限制、阈值、限流、反污染。

约束：
1. PAL 是发网前规则执行点。
2. PAL 管理签名身份。

### 2.3 libp2p (Data Plane)
职责：
1. GossipSub 广播。
2. peer discovery。
3. direct session 建链。

约束：
1. 不做语义决策。
2. 不改写 Canonical Event 业务字段。

## 3. OpenClaw <-> PAL 接口契约
接口风格：先以 in-process SDK 形态实现，后续可平移到 gRPC/HTTP，语义保持一致。

### 3.1 `prepare_broadcast`
用途：生成可审阅草案，不直接上网。

请求：
```json
{
  "intent_text": "中文图片标注500张，预算200元，48小时交付",
  "context": {
    "locale": "zh-CN",
    "budget_cny": 200
  },
  "requested_ttl_sec": 172800
}
```

响应：
```json
{
  "draft_id": "draft_01J...",
  "taxonomy": "crowd.data_labeling",
  "conf": 920,
  "topics": ["agentnet/v1/crowd", "agentnet/v1/crowd/data_labeling"],
  "summary": "中文图片标注500张 预算200元 48小时交付",
  "expires_at": 1766000000000,
  "policy": {
    "allowed": true,
    "blocks": []
  },
  "preview_event": {
    "v": "0.2",
    "op": "UPSERT"
  }
}
```

### 3.2 `publish_draft`
用途：确认后正式广播 `UPSERT`。

请求：
```json
{
  "draft_id": "draft_01J...",
  "approval": {
    "mode": "user_confirmed",
    "actor": "user:local"
  }
}
```

响应：
```json
{
  "accepted": true,
  "task_id": "b64:K3c7...",
  "seq": 1,
  "published_at": 1765900000000
}
```

### 3.3 `update_task`
用途：对 ACTIVE 任务更新，并广播更高 `seq` 的 `UPSERT`。

### 3.4 `complete_task`
用途：广播 `COMPLETE`，进入终态。

### 3.5 `withdraw_task`
用途：广播 `WITHDRAW`，进入终态。

### 3.6 `query_tasks`
用途：local-first 查询本地状态视图。

### 3.7 回调事件
1. `on_task_match(task_id, score, peer_hint)`。
2. `on_task_event(task_id, op, seq, from_peer)`。
3. `on_session_request(task_id, peer_id, detail_ref)`。
4. `on_policy_block(draft_id, reason_code)`。

## 4. Canonical Event 规范
文件：`spec/canonical-event.v0.1.schema.json`

核心字段：
1. `v`: 协议版本，当前固定 `"0.2"`。
2. `origin`: 任务归属 DID。
3. `publisher`: 当前发布者 DID（默认同 origin）。
4. `task_id`: 稳定任务标识，前缀 `b64:`。
5. `seq`: 单调递增版本号，`>= 1`。
6. `op`: `UPSERT | COMPLETE | WITHDRAW`。
7. `expires_at`: 毫秒时间戳。
8. `topics`: 路由主题列表。
9. `taxonomy`: 语义分类节点。
10. `conf`: 置信度（0-1000）。
11. `summary`: 简述。
12. `detail_ref`: 详细信息或会话入口。
13. `sig`: 签名。

硬约束：
1. 事件 JSON 需最小化编码后满足 `<= 1024` 字节。
2. 所有必填字段必须存在，且 `additionalProperties=false`。
3. `seq` 仅 PAL 分配；OpenClaw 不可写。

## 5. 确定性语义与状态机
状态键：`K = (origin, task_id)`。

处理顺序（对单条入站事件）：
1. 若 `now > expires_at`，丢弃。
2. 若本地 `K` 已终态，丢弃（吸收态）。
3. 若 `seq <= last_seq[K]`，丢弃。
4. 其余情况应用事件并更新 `last_seq[K] = seq`。
5. 若 `op=UPSERT`，状态为 `ACTIVE`。
6. 若 `op=COMPLETE`，状态为 `TERMINAL_COMPLETE`。
7. 若 `op=WITHDRAW`，状态为 `TERMINAL_WITHDRAW`。

合法发布轨迹约束（由 PAL 保证）：
1. 同一 `K` 的发布序列必须严格递增 `seq`。
2. 一旦发布 `COMPLETE/WITHDRAW`，不得再发布该 `K` 的后续事件。

注：上述“吸收态”语义在合法轨迹下保证与 PPT 对齐。

## 6. 语义编译与主题映射规则
语义编译输出：
1. `canonical_intent`（内部结构化结果）。
2. `taxonomy`。
3. `conf`。
4. `summary`。

主题映射规则：
1. taxonomy 采用点分层级，例如 `crowd.data_labeling`。
2. topic 路径采用斜杠层级，例如 `agentnet/v1/crowd/data_labeling`。
3. 默认同时包含父级和叶子级 topic。
4. topic 总数上限默认 `<= 8`。

确定性要求：
1. 模型版本固定。
2. 温度固定为 `0`。
3. 编译后必须通过规则校验；不通过则拒绝发布。

## 7. 治理策略 (Policy)
默认策略：
1. `conf_min = 700`。
2. `ttl_sec` 范围 `[300, 604800]`。
3. `summary` 长度 `<= 140`（建议）。
4. 发布速率限制（每 origin）。
5. topic 白名单与深度约束。

策略执行点：
1. `prepare_broadcast` 时执行预检并返回 block 原因。
2. `publish_draft` 时二次校验，防止草案失效或被篡改。

## 8. 错误码
1. `PAL-VAL-001`: Schema 校验失败。
2. `PAL-VAL-002`: 字段超限（大小/长度/topic 数量）。
3. `PAL-POL-001`: 置信度低于阈值。
4. `PAL-POL-002`: 速率限制触发。
5. `PAL-POL-003`: topic 不在允许集合。
6. `PAL-STA-001`: 非法状态转移（终态后更新）。
7. `PAL-STA-002`: 旧版本事件（`seq <= last_seq`）。
8. `PAL-EXP-001`: 事件已过期。
9. `PAL-SIG-001`: 签名验证失败。
10. `PAL-DRAFT-001`: 草案不存在或已过期。

## 9. Phase A 验收标准
1. 对同一输入，`prepare_broadcast` 结果在固定配置下稳定一致。
2. `canonical-event.v0.1.schema.json` 可用于自动校验并阻断非法消息。
3. 状态机回放测试覆盖：乱序、重放、过期、终态吸收。
4. OpenClaw 可走通：准备草案 -> 确认发布 -> 查询状态。
5. 关键失败场景均返回标准错误码。
