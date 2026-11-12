"""元信息 controller：健康检查、枚举字典、空态文案、内置 Mock REST 端点。"""
from __future__ import annotations

import time

from fastapi import Depends, Query, Request
from fastapi.responses import JSONResponse
from sqlalchemy.orm import Session

from src.config.database import get_db, ping_db
from src.constants import error_codes as codes
from src.constants.enums import ENUM_LABELS
from src.constants.limits import MOCK_REST_RATE_LIMIT_PER_MINUTE
from src.constants.messages import CONFLICT_STRATEGY_HELP, EMPTY_STATES, RUN_STATUS_HINT
from src.constants.roles import ROLE_LABELS
from src.constants.transforms import TRANSFORM_PARAM_SPECS
from src.utils import mock_rest
from src.utils.errors import AppError
from src.utils.redis_client import get_redis, redis_ping


def health(db: Session = Depends(get_db)) -> dict:
    db_ok = ping_db()
    redis_ok = redis_ping()
    return {
        "status": "ok" if db_ok and redis_ok else "degraded",
        "service": "syncmesh-backend",
        "db": db_ok,
        "redis": redis_ok,
        "time": int(time.time()),
    }


def enums() -> dict:
    return {
        "labels": ENUM_LABELS,
        "roles": ROLE_LABELS,
        "transform_params": TRANSFORM_PARAM_SPECS,
        "conflict_help": CONFLICT_STRATEGY_HELP,
        "run_status_hint": RUN_STATUS_HINT,
    }


def ui_text() -> dict:
    return {
        "empty_states": EMPTY_STATES,
        "conflict_help": CONFLICT_STRATEGY_HELP,
        "run_status_hint": RUN_STATUS_HINT,
        "no_new_data": "本批无新数据",
        "pipeline_locked": "这条管道正在跑，去看当前运行",
        "quota_exceeded": "并发运行数已达上限，请稍后再试",
    }


def _mock_rate_limit(request: Request) -> bool:
    try:
        client = get_redis()
        bucket = int(time.time() // 60)
        key = f"{mock_rest.RATE_KEY}:{bucket}"
        count = client.incr(key)
        client.expire(key, 70)
        return count <= MOCK_REST_RATE_LIMIT_PER_MINUTE
    except Exception:
        return True


def mock_shop_orders(
    request: Request,
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=50, ge=1, le=200),
    snapshot_id: str | None = Query(default=None),
) -> JSONResponse:
    """内置 Mock REST：分页 + 限流 + 偶发 500（用于验证 worker 的重试退避链路）。

    带 snapshot_id 时读取运行开始时冻结的快照，页边界与总数在整个运行期间稳定，
    上游新增/改状态/撤回不会造成跨页漂移（重复出现或消失）。
    """
    if not _mock_rate_limit(request):
        return JSONResponse(
            status_code=429,
            content={
                "code": codes.MOCK_REST_RATE_LIMITED,
                "message": "Mock REST 触发限流（每分钟 120 次）",
                "detail": f"page={page}",
                "retry_after": 60,
            },
        )
    if mock_rest.should_fail_once():
        return JSONResponse(
            status_code=500,
            content={
                "code": "MOCK_REST_UPSTREAM_ERROR",
                "message": "模拟电商渠道偶发 5xx，请客户端重试",
                "detail": "injected failure every 9th request",
            },
        )
    try:
        payload = mock_rest.paginate(page, page_size, snapshot_id=snapshot_id)
    except mock_rest.SnapshotExpired:
        return JSONResponse(
            status_code=409,
            content={
                "code": codes.SNAPSHOT_EXPIRED,
                "message": "同步快照已过期或不存在，本次运行的数据范围无法保证一致性",
                "detail": f"snapshot_id={snapshot_id}",
            },
        )
    payload["endpoint"] = "/api/mock/rest/shop_orders"
    payload["has_next"] = page < payload["total_pages"]
    return JSONResponse(status_code=200, content=payload)


def mock_create_snapshot() -> dict:
    return mock_rest.create_snapshot()


def mock_snapshot_state(snapshot_id: str) -> dict:
    snapshot = mock_rest.get_snapshot(snapshot_id)
    if snapshot is None:
        raise AppError(codes.SNAPSHOT_EXPIRED, "快照不存在或已过期", f"snapshot_id={snapshot_id}")
    return {
        "snapshot_id": snapshot["snapshot_id"],
        "created_at": snapshot["created_at"],
        "total": snapshot["total"],
        "total_amount": snapshot.get("total_amount", 0),
        "push_batch": snapshot["push_batch"],
    }


def mock_delete_snapshot(snapshot_id: str) -> dict:
    mock_rest.delete_snapshot(snapshot_id)
    return {"snapshot_id": snapshot_id, "deleted": True}


def mock_push_state() -> dict:
    return {"push_batch": mock_rest.push_enabled()}


def mock_set_push(enabled: bool) -> dict:
    mock_rest.set_push_enabled(enabled)
    return {
        "push_batch": enabled,
        "note": "已开启电商补推批次（12 行重复 order_id + 3 行 member_id 为空）" if enabled
        else "已重置为干净批次（150 行 / 3 页）",
    }
