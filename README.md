# 交换机转发表迁移服务（纯后端）

科研园区骨干交换机从旧转发表迁移到新转发表的协调服务。即使旧、终态都能到达出口
`DELIVER`，逐台更新的中间状态仍可能产生转发环路或黑洞。本服务：

- 校验初态与终态安全，规划出一条**每个前缀都安全**的完整更新排列；
- 持久化不可变计划、协调租约、严格递增的 epoch 与设备代次、设备命令与确认；
- 多个 API 实例共享一个 PostgreSQL，所有并发正确性都由数据库保证；
- 协调者失联 / 接管 / 重复拉取命令 / 迟到确认都能幂等收敛。

无前端、无固定结果、无"仅进程内"持久化、无单实例互斥冒充并发控制。

---

## 1. 快速开始

需要 Docker（含 Compose v2）。克隆后**不需要**宿主机上的 Python、PostgreSQL
或任何其它运行时。

```bash
# 启动 PostgreSQL + 两个共享数据库的 API 实例
docker compose up --build -d

# 健康检查
curl -s http://localhost:8080/health

# 一次性验收（在一次性 verify 容器中跑全部集成测试）
docker compose build verify
docker compose run --rm verify
```

- API 实例 1：宿主机端口由 `API_PORT` 配置，默认 `8080`。
- API 实例 2：由 `API2_PORT` 配置，默认 `8081`。
- 例：`API_PORT=9000 docker compose up -d`。

停止与清理：

```bash
docker compose down            # 保留数据卷
docker compose down -v         # 连数据卷一并删除
```

---

## 2. 数据模型与安全定义

- 交换机：1～60 个，ID 唯一；另有一个或多个入口（必须是已声明交换机）。
- 每台交换机有 `old_next` 与 `new_next`，取值为：已声明交换机、特殊出口
  `DELIVER`、黑洞 `DROP`。自环按普通下一跳处理。
- 一次更新把一台交换机从 `old_next` 原子切换为 `new_next`。
- 只有 `old_next != new_next` 的交换机会进入计划，数量上限 **22**。

**安全判定**：对任意"已更新集合"，从每个入口沿当前下一跳行走，必须在
**不超过交换机总数步**内到达 `DELIVER`。遇到 `DROP`、指向缺失节点、
重复节点（环路）、超出步数都判为不安全。

创建计划时：

1. 校验所有引用、规模（1～60）、入口、下一跳合法性；
2. 校验初态与终态都安全，否则分别返回 `initial_state_unsafe` /
   `final_state_unsafe`（HTTP 422）；
3. 调用回溯规划器寻找完整排列。

### 规划器：可回溯 + 字典序最小

规划器（`app/planner.py`，通用搜索在 `app/search.py`）做**穷尽回溯搜索**：

- 按交换机 ID 的**原始 UTF-8 字节序**逐个尝试候选；第一条走通的完整分支即
  字典序最小的完整排列；
- 用记忆化记录被证明走不通的死状态；
- **绝不**使用"每步选当前编号最小且暂时安全节点"这类不可回溯的贪心；
- **绝不**把多个不安全步骤当作一次原子批量更新。

找不到完整排列时，计划状态为 `proven_impossible`，并持久化该明确结论。

> 关于 `proven_impossible` 的一个事实性说明（已用穷举与证明验证）：在本题
> 字面模型下——更新只会把下一跳从 old 换成 new，且唯一目标是所有入口到达
> `DELIVER`——可以证明"初态、终态都安全 ⇒ 必存在安全的完整更新排列"：
> 按**新拓扑到 `DELIVER` 的距离由近到远**更新即可。证明思路：对任一已更新
> 集合，若从入口 F-walk 首次进入"已更新"节点 v，则其后在新边上行走时，
> `d_new` 在"仍停留在已更新集合内"的每一步严格递减（旧边只可能更早退出该
> 集合），故必在环形成前到达 `DELIVER`。这意味着该字面模型不会自然产生
> impossible 实例；我们仍如实实现了穷尽搜索与 `proven_impossible` 结论，并对
> 搜索核心的"安全死路回溯"与"穷尽失败"用合成安全谓词做了直接单元测试
> （`tests/test_planner.py`）。终态安全的真实实例上搜索为亚毫秒级。

**计划摘要**：对"规范化拓扑 + 入口 + 排列"计算稳定的 SHA-256 `plan_digest`
（见 `compute_plan_digest`）。规范化消除了请求中数组/键顺序差异。

---

## 3. 执行期一致性（全部由 PostgreSQL 保证）

- **协调租约**：基于数据库时钟（`now()`），TTL 5～60 秒。
- **Epoch**：每次被新的接管者取得租约时，从全局序列
  `coordinator_epoch_seq` 取 `nextval`，严格递增、永不复用。只有当前未过期
  epoch 能推进；旧 epoch 即使恢复也不能创建命令（`stale_epoch`）。
- **幂等操作标识**：续租 / 接管 / 推进都必须携带 `operation_id`，重复返回首次
  结果。
- **推进**：一次最多为计划中的下一个步骤创建**一条**设备命令；命令先持久化再
  返回，固化 `rollout_id / step / switch_id / plan_digest /
  device_generation / command_id`。同一步骤已有命令时，响应丢失后的重试、
  进程重启或接管都只返回原命令：不换 ID、不换代次、不额外递增。
- **设备代次**：每台交换机全局严格递增；低于设备已接受代次的迟到确认被拒绝。
- **确认**：交换机可重复提交相同 `APPLIED` 确认，返回首次结果；交换机、
  `command_id`、步骤、摘要、代次不匹配一律拒绝且不改变状态。确认接口
  **不依赖原协调租约仍有效**（接管期间对已签发命令的合法迟到确认仍可收敛），
  但确认只关闭当前步骤，不会代替协调者创建下一步。
- **单飞**：任意时刻每个 rollout 最多一条未确认命令（部分唯一索引
  `device_commands_one_pending` 兜底）；只有其 `APPLIED` 后协调者才能推进；
  最后一步确认后置 `COMPLETED`。
- **恢复**：服务重启后完全由数据库状态恢复，不依赖进程内锁、内存队列或本地
  时钟维持正确性。

关键事务均用 `SELECT ... FOR UPDATE` 行锁 + 唯一约束 + 部分唯一索引串行化，
在多个 API 容器共享同一数据库时成立。

---

## 4. HTTP / JSON API

所有请求/响应均为 JSON。错误响应结构稳定：

```json
{ "error": { "code": "stale_epoch", "message": "...", "details": { } } }
```

幂等键可放在请求体 `idempotency_key`，或 `Idempotency-Key` 头。

### 计划

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/api/plans` | 创建计划（幂等）。体：`switches, ingresses, old_next, new_next, idempotency_key` |
| GET  | `/api/plans/{plan_id}` | 查询不可变计划 |

创建成功返回 `201`（首次）或 `200`（同参重试）；同一 `idempotency_key`
异参复用返回 `409 idempotency_conflict`。

计划对象含：`status`（`feasible` / `proven_impossible`）、`update_order`、
`plan_digest`、`changed_switches`、`step_count` 等。

### Rollout

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/api/rollouts` | 基于计划创建执行（幂等），体：`plan_id, idempotency_key` |
| GET  | `/api/rollouts/{id}` | 完整执行状态（含每步命令、当前租约、未决命令） |
| GET  | `/api/rollouts/{id}/audit` | 审计轨迹（有序事件） |

### 协调者

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/api/rollouts/{id}/coordinator/acquire` | 取得 / 接管租约 |
| POST | `/api/rollouts/{id}/coordinator/renew` | 续租（同 epoch 同 holder，且未过期） |
| POST | `/api/rollouts/{id}/advance` | 推进一步 |

请求体：`holder_id`、`epoch`（renew/advance）、`operation_id`、
`duration_seconds`（acquire/renew，5～60）。

### 设备侧

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| GET  | `/api/rollouts/{id}/devices/{switch_id}/command` | 查询本机待执行命令（无则 `command: null`） |
| POST | `/api/rollouts/{id}/devices/{switch_id}/ack` | 提交 `APPLIED` 确认 |

确认体：`command_id, step, plan_digest, device_generation, result="APPLIED"`。

### 健康检查

`GET /health` → `{ "status": "ok", "database": "ok" }`。容器亦配置了
`HEALTHCHECK`（`scripts/healthcheck.sh`）。

### 主要错误码（稳定 4xx）

`invalid_body` / `invalid_json` / `invalid_content_type`、
`invalid_switches` / `topology_out_of_bounds` / `duplicate_switch_id` /
`unknown_ingress` / `unknown_next_hop` / `unknown_switch_reference` /
`reserved_switch_id`、`too_many_updates`、`initial_state_unsafe` /
`final_state_unsafe`、`missing_idempotency_key` / `idempotency_conflict`、
`plan_not_found` / `rollout_not_found` / `command_not_found` /
`switch_not_in_plan`、`no_lease` / `lease_held_by_other` /
`not_lease_holder` / `lease_expired`(410) / `stale_epoch`、
`invalid_lease_duration`、`ack_switch_mismatch` / `ack_step_mismatch` /
`ack_digest_mismatch` / `ack_generation_mismatch` / `ack_stale_generation`。

---

## 5. 一次完整调用示例

```bash
BASE=http://localhost:8080

PLAN=$(curl -s -XPOST $BASE/api/plans -H 'Content-Type: application/json' -d '{
  "idempotency_key":"plan-1",
  "switches":["s0","s1","s2","tail"],
  "ingresses":["s0"],
  "old_next":{"s0":"s1","s1":"s2","s2":"tail","tail":"DELIVER"},
  "new_next":{"s0":"DELIVER","s1":"s0","s2":"s1","tail":"DELIVER"}
}')
PID=$(echo $PLAN | python3 -c 'import sys,json;print(json.load(sys.stdin)["plan_id"])')

RID=$(curl -s -XPOST $BASE/api/rollouts -H 'Content-Type: application/json' \
  -d "{\"idempotency_key\":\"r1\",\"plan_id\":\"$PID\"}" \
  | python3 -c 'import sys,json;print(json.load(sys.stdin)["rollout_id"])')

curl -s -XPOST $BASE/api/rollouts/$RID/coordinator/acquire \
  -H 'Content-Type: application/json' \
  -d '{"holder_id":"c1","operation_id":"op-a1","duration_seconds":30}'
# 然后循环：advance -> 设备 GET command -> POST ack -> advance ...
```

---

## 6. 仓库结构

```
app/
  planner.py   # 校验、安全模拟、计划摘要（纯逻辑，可独立单测）
  search.py    # 通用穷尽回溯搜索（字典序最小 / proven_impossible）
  db.py        # 连接、迁移（advisory lock 串行化）
  service.py   # 事务化业务逻辑：幂等、租约、epoch、代次、命令、确认
  api.py       # Flask JSON API 与结构化错误
  wsgi.py      # gunicorn 入口
migrations/0001_init.sql
scripts/       # entrypoint / healthcheck / migrate
tests/         # 规划器单元测试 + 端到端集成/并发测试
Dockerfile  docker-compose.yml  requirements.txt  run.py  pytest.ini
```

## 7. 测试

- `docker compose run --rm verify`：在一次性容器内对**两个**真实 API 容器跑
  全部集成测试（含跨实例并发、接管、重复确认、重启恢复）。
- 本地（需自行提供 PostgreSQL，主要用于开发）：

```bash
pip install -r requirements.txt
export DATABASE_URL=postgresql://user@host:5432/dbname
python -m pytest -q
```

测试通过 `API1_URL` / `API2_URL` 环境变量在"Flask 进程内客户端"与
"真实 HTTP 双实例"两种模式间切换；`verify` 服务使用后者。

## 8. 配置（环境变量）

| 变量 | 默认 | 说明 |
| --- | --- | --- |
| `API_PORT` | `8080` | 容器内监听端口；compose 用它映射宿主机端口 |
| `API2_PORT` | `8081` | 第二个实例的宿主机映射端口 |
| `DATABASE_URL` | （见 compose） | PostgreSQL DSN |
| `POSTGRES_HOST/PORT/DB/USER/PASSWORD` | `db/5432/...` | 未设 `DATABASE_URL` 时拼装 |
| `GUNICORN_WORKERS` / `GUNICORN_THREADS` | `2` / `4` | 进程/线程数 |
