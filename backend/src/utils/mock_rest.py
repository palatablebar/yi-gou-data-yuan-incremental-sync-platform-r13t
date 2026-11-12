"""内置 Mock REST 数据源：分页、限流、偶发 5xx，以及『电商业补推批次』开关。

数据是确定性生成的，seed、前端提示与 README 引用的是同一套数值：
- 基础数据 3 页 × 50 行 = 150 行（order_id 1001..1150）
- 补推批次 27 行：4 个 order_id 各重复 4 次（12 行冲突）+ 3 行 member_id 为空 + 8 行正常

同步运行通过 create_snapshot() 在开始时冻结一份数据，之后所有分页都带
snapshot_id 读取同一快照，运行期间上游再新增/改状态/撤回都不会改变本次范围。
"""
from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone
from typing import Any

from src.constants.limits import (
    MOCK_REST_BASE_ROWS,
    MOCK_REST_FAILURE_EVERY,
    MOCK_REST_PAGE_SIZE,
    MOCK_REST_SNAPSHOT_TTL_SECONDS,
)
from src.utils.redis_client import get_redis

PUSH_FLAG_KEY = "mock:shop_orders:push"
REQ_COUNTER_KEY = "mock:shop_orders:req"
RATE_KEY = "mock:shop_orders:rate"
SNAPSHOT_KEY_PREFIX = "mock:shop_orders:snapshot:"

VALID_MEMBERS = ["M0001", "M0002", "M0003", "M0007", "M0012", "M0031", "M0044", "M0102"]
CHANNELS = ["mall", "mini", "app"]
ORDER_STATUS = ["paid", "shipped", "refunded"]


def _base_order(index: int) -> dict[str, Any]:
    order_id = 1001 + index
    return {
        "order_id": order_id,
        "member_id": VALID_MEMBERS[index % len(VALID_MEMBERS)],
        "amount": f"{(index % 47) * 12.5 + 19.9:.2f}",
        "channel": CHANNELS[index % len(CHANNELS)],
        "order_status": ORDER_STATUS[index % len(ORDER_STATUS)],
        "updated_at": f"2026-11-{8 + (index % 2):02d} {9 + (index % 9):02d}:{(index * 7) % 60:02d}:00",
    }


def base_rows() -> list[dict[str, Any]]:
    return [_base_order(i) for i in range(MOCK_REST_BASE_ROWS)]


def push_batch_rows() -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    # 12 行冲突：4 个 order_id 各补推 4 次，第一次落库，其余 3 次命中 batch 内重复键 → conflict_rejected
    for offset, order_id in enumerate([1151, 1152, 1153, 1154]):
        for repeat in range(4):
            rows.append(
                {
                    "order_id": order_id,
                    "member_id": VALID_MEMBERS[(offset + repeat) % len(VALID_MEMBERS)],
                    "amount": f"{88.0 + offset + repeat / 10:.2f}",
                    "channel": "mall",
                    "order_status": "paid",
                    "updated_at": "2026-11-10 01:12:00",
                }
            )
    # 3 行必填缺失：member_id 为空 → required_missing
    for offset, order_id in enumerate([1155, 1156, 1157]):
        rows.append(
            {
                "order_id": order_id,
                "member_id": "",
                "amount": f"{120.0 + offset:.2f}",
                "channel": "mini",
                "order_status": "paid",
                "updated_at": "2026-11-10 01:20:00",
            }
        )
    # 8 行正常新增
    for offset, order_id in enumerate(range(1158, 1166)):
        rows.append(
            {
                "order_id": order_id,
                "member_id": VALID_MEMBERS[(offset + 3) % len(VALID_MEMBERS)],
                "amount": f"{260.0 + offset * 3:.2f}",
                "channel": "app",
                "order_status": "paid",
                "updated_at": "2026-11-10 01:24:00",
            }
        )
    return rows


def push_enabled() -> bool:
    try:
        return get_redis().get(PUSH_FLAG_KEY) == "1"
    except Exception:
        return False


def set_push_enabled(enabled: bool) -> None:
    client = get_redis()
    if enabled:
        client.set(PUSH_FLAG_KEY, "1")
    else:
        client.delete(PUSH_FLAG_KEY)


def all_rows() -> list[dict[str, Any]]:
    rows = base_rows()
    if push_enabled():
        rows.extend(push_batch_rows())
    return rows


def _snapshot_key(snapshot_id: str) -> str:
    return f"{SNAPSHOT_KEY_PREFIX}{snapshot_id}"


def create_snapshot() -> dict[str, Any]:
    """冻结当前时刻的全量数据到 Redis，返回快照元信息（稳定范围的凭证）。"""
    snapshot_id = uuid.uuid4().hex
    rows = all_rows()
    created_at = datetime.now(timezone.utc).isoformat()
    payload = {
        "snapshot_id": snapshot_id,
        "created_at": created_at,
        "total": len(rows),
        "total_amount": _sum_amount(rows),
        "push_batch": push_enabled(),
        "rows": rows,
    }
    client = get_redis()
    client.set(
        _snapshot_key(snapshot_id),
        json.dumps(payload, ensure_ascii=False),
        ex=MOCK_REST_SNAPSHOT_TTL_SECONDS,
    )
    return {
        "snapshot_id": snapshot_id,
        "created_at": created_at,
        "total": len(rows),
        "total_amount": payload["total_amount"],
        "push_batch": push_enabled(),
    }


def _sum_amount(rows: list[dict[str, Any]]) -> float:
    """范围金额合计：用于和目标表同一口径对账（无 amount 字段时返回 0）。"""
    total = 0.0
    for row in rows:
        try:
            total += float(row.get("amount") or 0)
        except (TypeError, ValueError):
            continue
    return round(total, 2)


def get_snapshot(snapshot_id: str) -> dict[str, Any] | None:
    try:
        raw = get_redis().get(_snapshot_key(snapshot_id))
    except Exception:
        return None
    if raw is None:
        return None
    return json.loads(raw)


def touch_snapshot(snapshot_id: str) -> bool:
    """续跑/翻页时给快照续命，返回快照是否仍然存在。"""
    try:
        return bool(get_redis().expire(_snapshot_key(snapshot_id), MOCK_REST_SNAPSHOT_TTL_SECONDS))
    except Exception:
        return False


def delete_snapshot(snapshot_id: str) -> None:
    try:
        get_redis().delete(_snapshot_key(snapshot_id))
    except Exception:
        pass


def paginate(page: int, page_size: int = MOCK_REST_PAGE_SIZE, snapshot_id: str | None = None) -> dict[str, Any]:
    if snapshot_id:
        snapshot = get_snapshot(snapshot_id)
        if snapshot is None:
            raise SnapshotExpired(snapshot_id)
        rows = snapshot["rows"]
        frozen = True
    else:
        rows = all_rows()
        frozen = False
    total = len(rows)
    page = max(int(page), 1)
    page_size = max(min(int(page_size), 200), 1)
    total_pages = max((total + page_size - 1) // page_size, 1)
    start = (page - 1) * page_size
    return {
        "page": page,
        "page_size": page_size,
        "total": total,
        "total_pages": total_pages,
        "items": rows[start : start + page_size],
        "push_batch": push_enabled(),
        "snapshot_id": snapshot_id,
        "snapshot_frozen": frozen,
    }


class SnapshotExpired(Exception):
    """快照已过期或不存在（Redis 被清空 / 超过 TTL），本次范围无法保证一致性。"""

    def __init__(self, snapshot_id: str) -> None:
        self.snapshot_id = snapshot_id
        super().__init__(f"snapshot expired or missing: {snapshot_id}")


def should_fail_once() -> bool:
    """每 N 次请求返回一次 500，用于验证重试链路（Redis 计数）。"""
    try:
        counter = get_redis().incr(REQ_COUNTER_KEY)
        return counter % MOCK_REST_FAILURE_EVERY == 0
    except Exception:
        return False
