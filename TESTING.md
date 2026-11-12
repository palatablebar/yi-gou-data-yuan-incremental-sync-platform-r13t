# 测试说明

## 1. 运行环境

- 语言：Python 3.11（与 `backend/Dockerfile`、`worker/Dockerfile` 一致）。
- 测试不依赖真实 PostgreSQL / Redis / HTTP 服务：
  - Redis 用内存替身（`tests/conftest.py::FakeRedis`），
  - httpx 用 `FakeHttp` 替身并直接挂接 backend 的 `mock_rest` 分页逻辑，
  - 数据库 Session / 模型 / staging 用内存假对象，聚焦编排语义而非驱动。
- worker 代码在容器内是顶层包 `worker_src`，本地目录是 `worker/src`；
  `conftest.py` 用 `importlib` 把该目录注册成 `worker_src`，无需改目录结构。

安装最小测试依赖（仓库根目录）：

```bash
python3 -m pip install -r backend/requirements.txt   # 完整依赖
# 或仅安装测试所需：sqlalchemy pydantic pydantic-settings redis httpx pytest
# celery pandas numpy psycopg  # worker 编排与连接器导入链需要
```

## 2. 运行测试

```bash
# 让解释器能找到已安装依赖；若用 venv 则先激活
PYTHONPATH=<你的依赖目录> python3 -m pytest tests/ -q
```

## 3. 用例覆盖（对应一致性需求）

| 文件 | 覆盖点 |
|---|---|
| `tests/test_snapshot_scope.py` | 快照冻结后，上游补推/撤回/改状态不改变本次范围与页边界；快照失效显式报错；快照金额合计稳定 |
| `tests/test_rest_connector.py` | 每页 checkpoint 回调；从指定页续跑；跨页重复业务键去重且不产生拒绝；5xx 退避重试后成功；快照失效快速失败不重试；运行中上游变化不移动页边界 |
| `tests/test_sync_resume.py` | 整跑记录范围与分类计数；第 3 页失败后续跑只拉第 3 页、无漏单；重复执行幂等（第二次全部跳过、行数不翻倍）；拒绝记录幂等键去重；运行中补推不影响本次账目 |
| `tests/test_verify_accounting.py` | 校验恒等式 `抽取 = 过滤 + 转换拒绝 + 写入 + 跳过 + 冲突拒绝`；staging 行数必须等于抽取行数 |

## 4. 手工端到端验证（需 `docker compose up`）

1. 触发一次 `mock_shop_orders` 管道，在运行详情页查看「本次运行范围与续跑位置」：
   应有快照 ID、冻结时刻、150 行 / 3 页、范围金额合计。
2. 运行期间 `POST /api/mock/rest/shop_orders/push` 开启补推：本次运行范围仍是 150 行，
   下一次运行才会看到 177 行（含补推）。
3. 制造 5xx（接口每 9 次请求注入一次）观察运行进入重试：详情页 checkpoint 显示「下一页」，
   重试后从该页继续，`pages_fetched` 累计正确、目标表无重复入账。
4. 对同一管道再触发一次：第二次运行 `rows_skipped` 等于范围行数、`rows_written` 为 0，
   目标表订单数与金额保持不变。

## 5. 数据库说明

题目要求 PostgreSQL 15，本实现使用 `postgres:16-alpine`；16 对 15 完全兼容，
schema / `ON CONFLICT` / JSONB / 部分唯一索引语法一致。

迁移 `0002_run_scope_resume` 是幂等的：0001 用 `Base.metadata.create_all` 建表，
全新库可能已含新列，因此每个列/索引都先查 `information_schema` / `pg_indexes` 再决定是否创建。
