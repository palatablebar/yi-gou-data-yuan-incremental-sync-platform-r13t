"""sync_task 编排测试：断点续跑、重复执行幂等、分类计数、变化不漏单。

用内存假对象代替 SQLAlchemy Session / 模型，聚焦编排语义而非数据库驱动。
staging 用 worker_src.core.storage 的内存替身，验证页级追加与跨尝试去重。
"""
from __future__ import annotations

from types import SimpleNamespace

import pytest

from src.constants import error_codes as codes
from src.utils.errors import AppError
from worker_src.connectors import rest_connector
from worker_src.tasks import sync_task


class FakeRun:
    def __init__(self) -> None:
        import uuid

        self.id = uuid.uuid4()
        self.pipeline_id = uuid.uuid4()
        self.status = "running"
        self.retry_count = 0
        self.counters: dict = {}
        self.sync_scope: dict | None = None
        self.resume_state: dict | None = None
        self.watermark_from = None
        self.watermark_value = None
        self.rows_read = 0
        self.rows_written = 0
        self.rows_rejected = 0
        self.rows_skipped = 0
        self.stage = "probe"
        self.stage_history: list = []
        self.error_code = None
        self.error_message = None


class FakeDB:
    def __init__(self) -> None:
        self.commits = 0
        self.rejected: list[dict] = []

    def commit(self) -> None:
        self.commits += 1

    def execute(self, stmt) -> None:
        # pg_insert(...).on_conflict_do_nothing() 的极简模拟：按幂等键去重
        if not hasattr(stmt, "_fake_values"):
            return
        existing = {(r["run_id"], r["idempotency_key"]) for r in self.rejected}
        for row in stmt._fake_values:
            key = (row["run_id"], row["idempotency_key"])
            if key not in existing:
                self.rejected.append(row)
                existing.add(key)


class FakeStaging:
    def __init__(self) -> None:
        self.rows: list[dict] = []
        self.prepared_reuse: list[bool] = []

    def prepare(self, pipeline_id, *, reuse: bool = False) -> str:
        if not reuse:
            self.rows = []
        self.prepared_reuse.append(reuse)
        return "sync_test"

    def write(self, pipeline_id, table, records, start_index: int = 0) -> int:
        self.rows.extend(records)
        return len(records)

    def count(self, pipeline_id, table) -> int:
        return len(self.rows)

    def fetch_rows(self, pipeline_id, table) -> list[dict]:
        return list(self.rows)

    def fetch_keys(self, pipeline_id, table, key_field) -> set:
        return {row[key_field] for row in self.rows if row.get(key_field) is not None}


@pytest.fixture
def orchestration(monkeypatch, deterministic_mock):
    """组装 sync_task 的全部替身。"""
    mock = deterministic_mock.mock_rest
    run = FakeRun()
    db = FakeDB()
    staging = FakeStaging()
    pipeline = SimpleNamespace(
        id=run.pipeline_id,
        sync_mode="full",
        watermark_column=None,
        conflict_key="order_id",
        conflict_strategy="skip",
        target_schema="target",
        target_table="channel_orders",
        last_watermark=None,
    )
    source = SimpleNamespace(
        source_type="mock_rest",
        config_json={"endpoint": "/api/mock/rest/shop_orders", "page_size": 50},
    )
    mappings = [
        {"source_field": "order_id", "target_column": "order_id"},
        {"source_field": "member_id", "target_column": "member_id"},
        {"source_field": "amount", "target_column": "amount"},
        {"source_field": "channel", "target_column": "channel"},
        {"source_field": "order_status", "target_column": "order_status"},
        {"source_field": "updated_at", "target_column": "updated_at"},
    ]

    # 目标表用内存集合模拟，记录已入账的 order_id
    target_keys: set = set()

    monkeypatch.setattr(sync_task.mapping_service, "mapping_dicts", lambda db_, pid: mappings)
    monkeypatch.setattr(sync_task.load, "target_max_watermark", lambda *a, **k: None)

    monkeypatch.setattr(sync_task.storage, "prepare_staging", staging.prepare)
    monkeypatch.setattr(sync_task.storage, "write_staging", staging.write)
    monkeypatch.setattr(sync_task.storage, "count_staging", staging.count)
    monkeypatch.setattr(sync_task.storage, "fetch_staging_rows", staging.fetch_rows)
    monkeypatch.setattr(sync_task.storage, "fetch_staging_keys", staging.fetch_keys)

    def fake_load_to_target(**kwargs):
        # 每次调用返回全新 outcome（与真实 load_service.insert_rows 一致）
        outcome = SimpleNamespace(written=0, skipped=0, rejected=[])
        for record in kwargs["records"]:
            key = record["order_id"]
            if key in target_keys:
                outcome.skipped += 1
            else:
                target_keys.add(key)
                outcome.written += 1
        return outcome

    monkeypatch.setattr(sync_task.load, "load_to_target", fake_load_to_target)
    monkeypatch.setattr(sync_task.progress, "set_stage", lambda *a, **k: None)
    monkeypatch.setattr(sync_task.progress, "bump_counters", lambda *a, **k: None)
    monkeypatch.setattr(sync_task.progress, "finish", lambda *a, **k: None)

    # pg_insert 替身：保留参数供 FakeDB 去重
    class FakeInsert:
        def __init__(self, values):
            self._fake_values = values

        @classmethod
        def values(cls, payloads):
            return cls(payloads)

        def on_conflict_do_nothing(self, index_elements=None):
            return self

    monkeypatch.setattr(sync_task, "pg_insert", lambda model: FakeInsert)

    return SimpleNamespace(
        run=run, db=db, staging=staging, pipeline=pipeline, source=source,
        target_keys=target_keys, mock=mock,
    )


def test_full_run_records_scope_and_counters(orchestration):
    ctx = orchestration
    result = sync_task._run(ctx.db, ctx.run, ctx.pipeline, ctx.source)

    assert result["ok"] is True
    assert ctx.run.sync_scope["scope_type"] == "upstream_snapshot"
    assert ctx.run.sync_scope["total_rows"] == 150
    assert ctx.run.sync_scope["total_pages"] == 3
    assert ctx.run.sync_scope["total_amount"] > 0
    # 运行结束 checkpoint 清空
    assert ctx.run.resume_state is None
    counters = ctx.run.counters
    assert counters["snapshot_total"] == 150
    assert counters["pages_total"] == 3
    assert counters["pages_fetched"] == 3
    assert counters["rows_extracted"] == 150
    assert counters["rows_read"] == 150
    assert counters["rows_written"] == 150
    assert counters["duplicate_rows"] == 0
    assert len(ctx.target_keys) == 150


def test_resume_after_page_failure_continues_from_checkpoint(orchestration, monkeypatch):
    ctx = orchestration
    config = ctx.source.config_json
    original_read_paged = rest_connector.read_paged
    calls: list[int] = []

    def flaky_read_paged(cfg, *, snapshot_id, start_page=1, key_field=None, on_page=None):
        calls.append(start_page)
        if start_page != 1:
            return original_read_paged(
                cfg, snapshot_id=snapshot_id, start_page=start_page,
                key_field=key_field, on_page=on_page,
            )

        # 首次尝试：前 2 页正常回调 checkpoint，第 3 页拉取前抛出可重试错误
        def partial_callback(page, payload):
            if page == 3:
                raise AppError(codes.SOURCE_UNREACHABLE, "模拟第 3 页 5xx", "page=3")
            on_page(page, payload)

        return original_read_paged(
            cfg, snapshot_id=snapshot_id, start_page=1,
            key_field=key_field, on_page=partial_callback,
        )

    monkeypatch.setattr(rest_connector, "read_paged", flaky_read_paged)

    # 第一次尝试：在第 3 页失败
    with pytest.raises(AppError):
        sync_task._run(ctx.db, ctx.run, ctx.pipeline, ctx.source)

    assert ctx.run.resume_state is not None
    assert ctx.run.resume_state["next_page"] == 3
    assert ctx.run.resume_state["pages_fetched"] == 2
    assert len(ctx.staging.rows) == 100  # 前两页已落地

    # 模拟 Celery 重试：状态置回 queued，retry_count 增加。
    # flaky_read_paged 对 start_page != 1 自动委托原连接器，故续跑走正常路径。
    ctx.run.status = "queued"
    ctx.run.retry_count = 1

    result = sync_task._run(ctx.db, ctx.run, ctx.pipeline, ctx.source)
    assert result["ok"] is True
    assert calls == [1, 3]  # 续跑只请求第 3 页
    assert ctx.run.resume_state is None
    assert len(ctx.target_keys) == 150  # 没有漏单
    assert ctx.run.counters["rows_extracted"] == 150
    assert ctx.staging.prepared_reuse == [False, True]  # 第二次复用 staging


def test_rerun_same_scope_does_not_double_post(orchestration):
    """重复执行（新 run、同一目标表）：幂等入账，第二次全部跳过，金额/行数不翻倍。"""
    ctx = orchestration
    sync_task._run(ctx.db, ctx.run, ctx.pipeline, ctx.source)
    assert len(ctx.target_keys) == 150

    # 全新运行：无 resume_state，staging 重建，目标表已有数据
    second_run = FakeRun()
    second_db = FakeDB()
    ctx.staging.rows = []
    result = sync_task._run(second_db, second_run, ctx.pipeline, ctx.source)

    assert result["ok"] is True
    assert len(ctx.target_keys) == 150  # 仍是 150 个订单
    assert second_run.counters["rows_written"] == 0
    assert second_run.counters["rows_skipped"] == 150


def test_rejected_records_are_idempotent_across_attempts(orchestration, monkeypatch):
    """同一运行重试后重新进入 load：拒绝记录按幂等键只落一次，不重复隔离。"""
    ctx = orchestration

    # 让转换阶段产出固定拒绝行
    def fake_transform(rows, mappings, *a, **k):
        failures = [
            {
                "reason_code": "required_missing",
                "reason_detail": "member_id empty",
                "raw_payload": rows[0],
                "source_row_index": 0,
            }
        ]
        # 其余行正常
        mapped = []
        for idx, row in enumerate(rows[1:], start=1):
            mapped.append(
                {"order_id": row["order_id"], "__raw_payload__": row, "__source_row_index__": idx}
            )
        return mapped, failures

    monkeypatch.setattr(sync_task.transform, "transform_batch", fake_transform)

    sync_task._run(ctx.db, ctx.run, ctx.pipeline, ctx.source)
    first_count = len(ctx.db.rejected)
    assert first_count == 1

    # 模拟同一 run 对象再次执行持久化（幂等键相同）
    sync_task._persist_rejected(
        ctx.db, ctx.run, ctx.pipeline,
        [
            {
                "reason_code": "required_missing",
                "reason_detail": "member_id empty",
                "raw_payload": {},
                "source_row_index": 0,
            }
        ],
    )
    assert len(ctx.db.rejected) == first_count


def test_upstream_push_during_run_does_not_change_scope(orchestration):
    """运行途中上游补推：本次范围仍是快照里的 150 行，补推行不影响本次账目。"""
    ctx = orchestration
    original = rest_connector.read_paged

    def push_midway(cfg, *, snapshot_id, start_page=1, key_field=None, on_page=None):
        def wrapped(page, payload):
            if page == 2:
                ctx.mock.set_push_enabled(True)
            on_page(page, payload)

        return original(
            cfg, snapshot_id=snapshot_id, start_page=start_page,
            key_field=key_field, on_page=wrapped,
        )

    import worker_src.connectors.rest_connector as rc
    rc.read_paged = push_midway
    try:
        result = sync_task._run(ctx.db, ctx.run, ctx.pipeline, ctx.source)
    finally:
        rc.read_paged = original

    assert result["rows_read"] == 150
    assert ctx.run.sync_scope["total_rows"] == 150
    assert len(ctx.target_keys) == 150
