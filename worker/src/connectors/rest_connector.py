"""Mock REST 连接器：运行快照 + 页级断点续跑 + 5xx 退避重试 + 限流尊重。

一致性保证：
- 运行开始先创建快照（POST .../snapshot），此后所有分页都带 snapshot_id，
  读取的是同一份冻结数据；上游新增 / 改状态 / 撤回不会改变本次范围与页边界。
- 每页成功返回后调用方把 checkpoint 落库（resume_state.next_page），
  失败重试从下一页继续，已读页不会再拉第二遍。
- 快照内同一业务键（order_id）若在多页重复出现，由 read_paged 去重并计入
  duplicate_rows，绝不把快照内部重复当成冲突拒绝。
"""
from __future__ import annotations

import time
from typing import Any

import httpx

from src.constants import error_codes as codes
from src.utils.errors import AppError
from src.utils.logger import get_logger
from worker_src.constants.tuning import REST_PAGE_RETRY_ATTEMPTS, REST_TIMEOUT_SECONDS
from worker_src.config.settings import settings

logger = get_logger("syncmesh.worker.rest")


def _endpoint(config: dict[str, Any]) -> str:
    endpoint = config.get("endpoint") or "/api/mock/rest/shop_orders"
    base = settings.mock_rest_base_url.rstrip("/")
    if endpoint.startswith("http"):
        return endpoint
    if not endpoint.startswith("/api"):
        endpoint = f"/api{endpoint}"
    return f"{base}{endpoint}"


def create_snapshot(config: dict[str, Any]) -> dict[str, Any]:
    """运行开始时冻结一份上游数据，返回 {snapshot_id, created_at, total, push_batch}。"""
    url = f"{_endpoint(config)}/snapshot"
    response = httpx.post(url, timeout=REST_TIMEOUT_SECONDS)
    if response.status_code >= 400:
        raise AppError(
            codes.SOURCE_UNREACHABLE,
            "无法创建上游数据快照",
            f"POST {url} status={response.status_code} body={response.text[:300]}",
        )
    payload = response.json()
    logger.info(
        "rest snapshot created id=%s total=%s push_batch=%s",
        payload.get("snapshot_id"), payload.get("total"), payload.get("push_batch"),
    )
    return payload


def delete_snapshot(config: dict[str, Any], snapshot_id: str) -> None:
    try:
        httpx.delete(f"{_endpoint(config)}/snapshot/{snapshot_id}", timeout=REST_TIMEOUT_SECONDS)
    except Exception as exc:  # noqa: BLE001 - 清理失败只告警，快照有 TTL 兜底
        logger.warning("rest snapshot cleanup failed id=%s error=%s", snapshot_id, exc)


def touch_snapshot(config: dict[str, Any], snapshot_id: str) -> None:
    """续跑时确认快照还在（GET 会触发后端检查；TTL 由后端在翻页时自动续期）。"""
    response = httpx.get(
        f"{_endpoint(config)}/snapshot/{snapshot_id}", timeout=REST_TIMEOUT_SECONDS
    )
    if response.status_code == 409 or response.status_code == 404:
        raise AppError(
            codes.SNAPSHOT_EXPIRED,
            "同步快照已过期或不存在，本次运行的数据范围无法保证一致性",
            f"snapshot_id={snapshot_id}",
        )
    response.raise_for_status()


def read_paged(
    config: dict[str, Any],
    *,
    snapshot_id: str,
    start_page: int = 1,
    key_field: str | None = "order_id",
    on_page: Any = None,
) -> dict[str, Any]:
    """从 start_page 开始读快照分页，逐页回调 on_page(page, payload) 以便落 checkpoint。

    返回 {rows, pages_fetched, duplicate_rows, total, total_pages, page_size}。
    跨页重复的业务键只保留第一次出现的行，重复次数计入 duplicate_rows。
    """
    url = _endpoint(config)
    page_size = int(config.get("page_size", 50))
    page = max(int(start_page), 1)
    seen_keys: set[Any] = set()
    rows: list[dict[str, Any]] = []
    duplicate_rows = 0
    pages_fetched = 0
    total_pages = page
    total = 0

    while True:
        payload = _fetch_with_retry(url, page, page_size, snapshot_id)
        total_pages = int(payload.get("total_pages", 1))
        total = int(payload.get("total", 0))
        for row in payload.get("items", []):
            key = row.get(key_field) if key_field else None
            if key is not None and key in seen_keys:
                duplicate_rows += 1
                continue
            if key is not None:
                seen_keys.add(key)
            rows.append(row)
        pages_fetched += 1
        logger.info(
            "rest page fetched snapshot=%s page=%s/%s rows_so_far=%s dup=%s",
            snapshot_id, page, total_pages, len(rows), duplicate_rows,
        )
        if on_page is not None:
            on_page(page, payload)
        if page >= total_pages:
            break
        page += 1

    return {
        "rows": rows,
        "pages_fetched": pages_fetched,
        "duplicate_rows": duplicate_rows,
        "total": total,
        "total_pages": total_pages,
        "page_size": page_size,
    }


def read(config: dict[str, Any], columns: list[str] | None = None) -> list[dict[str, Any]]:
    """兼容旧调用：建快照 → 一次读完 → 删快照（无 checkpoint 需求的探测类场景）。"""
    snapshot = create_snapshot(config)
    try:
        result = read_paged(config, snapshot_id=snapshot["snapshot_id"])
        return result["rows"]
    finally:
        delete_snapshot(config, snapshot["snapshot_id"])


def _fetch_with_retry(url: str, page: int, page_size: int, snapshot_id: str) -> dict[str, Any]:
    last_error: Exception | None = None
    for attempt in range(1, REST_PAGE_RETRY_ATTEMPTS + 1):
        try:
            response = httpx.get(
                url,
                params={"page": page, "page_size": page_size, "snapshot_id": snapshot_id},
                timeout=REST_TIMEOUT_SECONDS,
            )
            if response.status_code == 429:
                retry_after = int(response.headers.get("retry-after", "1"))
                time.sleep(min(retry_after, 5))
                raise AppError(
                    codes.MOCK_REST_RATE_LIMITED, "Mock REST 触发限流", f"page={page}"
                )
            if response.status_code in (409, 404):
                # 快照失效是不可重试错误：重试也拿不回同一范围，必须失败待人工处理
                body = response.text[:300]
                raise AppError(
                    codes.SNAPSHOT_EXPIRED,
                    "同步快照已过期或不存在，本次运行的数据范围无法保证一致性",
                    f"page={page} body={body}",
                )
            if response.status_code >= 500:
                raise AppError(
                    codes.SOURCE_UNREACHABLE,
                    "Mock REST 返回 5xx",
                    f"page={page} status={response.status_code}",
                )
            response.raise_for_status()
            return response.json()
        except AppError as exc:
            if exc.code == codes.SNAPSHOT_EXPIRED:
                raise
            last_error = exc
            logger.warning("rest page retry page=%s attempt=%s error=%s", page, attempt, exc)
            time.sleep(min(0.4 * (2 ** (attempt - 1)), 4))
        except httpx.HTTPError as exc:
            last_error = exc
            logger.warning("rest page retry page=%s attempt=%s error=%s", page, attempt, exc)
            time.sleep(min(0.4 * (2 ** (attempt - 1)), 4))
    raise AppError(
        codes.SOURCE_UNREACHABLE,
        "Mock REST 多次重试仍失败",
        f"page={page} error={last_error}",
    )
