"""连接器：快照分页 + 页级 checkpoint + 跨页去重 + 5xx 重试 + 快照失效快速失败。"""
from __future__ import annotations

import pytest

from src.constants import error_codes as codes
from src.utils import mock_rest
from src.utils.errors import AppError
from worker_src.connectors import rest_connector

CONFIG = {"endpoint": "/api/mock/rest/shop_orders", "page_size": 50}


def test_read_paged_emits_checkpoint_per_page(deterministic_mock):
    mock = deterministic_mock.mock_rest
    snapshot = rest_connector.create_snapshot(CONFIG)
    checkpoints: list[tuple[int, int]] = []

    result = rest_connector.read_paged(
        CONFIG,
        snapshot_id=snapshot["snapshot_id"],
        on_page=lambda page, payload: checkpoints.append((page, payload["total_pages"])),
    )

    assert checkpoints == [(1, 3), (2, 3), (3, 3)]
    assert result["pages_fetched"] == 3
    assert result["total"] == 150
    assert len(result["rows"]) == 150
    assert result["duplicate_rows"] == 0


def test_resume_starts_from_checkpoint_page(deterministic_mock):
    """模拟前两页已成功、第三页失败：续跑只拉第 3 页。"""
    mock = deterministic_mock.mock_rest
    snapshot = rest_connector.create_snapshot(CONFIG)
    seen_pages: list[int] = []

    result = rest_connector.read_paged(
        CONFIG,
        snapshot_id=snapshot["snapshot_id"],
        start_page=3,
        on_page=lambda page, payload: seen_pages.append(page),
    )

    assert seen_pages == [3]
    assert result["pages_fetched"] == 1
    assert len(result["rows"]) == 50  # 只有第三页的 50 行


def test_cross_page_duplicate_keys_are_deduped_not_rejected(deterministic_mock, monkeypatch):
    """上游快照里同一 order_id 在多页重复：只保留第一条，重复计数但不产生拒绝。"""
    mock = deterministic_mock.mock_rest
    snapshot = rest_connector.create_snapshot(CONFIG)

    # 构造一个跨页重复的分页替身：第 1 页含 order 1001，第 2 页又出现一次 1001
    original_paginate = mock.paginate

    def patched(page, page_size=50, snapshot_id=None):
        payload = original_paginate(page, page_size, snapshot_id=snapshot_id)
        if page == 2 and payload["items"]:
            payload["items"][0] = dict(payload["items"][0], order_id=1001)
        return payload

    monkeypatch.setattr(mock, "paginate", patched)

    result = rest_connector.read_paged(
        CONFIG, snapshot_id=snapshot["snapshot_id"], start_page=1
    )
    order_ids = [row["order_id"] for row in result["rows"]]
    assert order_ids.count(1001) == 1
    assert result["duplicate_rows"] == 1


def test_5xx_is_retried_then_succeeds(deterministic_mock):
    deterministic_mock.http.fail_times = 1  # 第一次请求 500
    snapshot = rest_connector.create_snapshot(CONFIG)
    result = rest_connector.read_paged(CONFIG, snapshot_id=snapshot["snapshot_id"])
    assert len(result["rows"]) == 150


def test_snapshot_expired_is_not_retried(deterministic_mock):
    mock = deterministic_mock.mock_rest
    snapshot = rest_connector.create_snapshot(CONFIG)
    mock.delete_snapshot(snapshot["snapshot_id"])

    with pytest.raises(AppError) as exc:
        rest_connector.read_paged(CONFIG, snapshot_id=snapshot["snapshot_id"])
    assert exc.value.code == codes.SNAPSHOT_EXPIRED
    # 快速失败：没有进入退避重试循环（快照失效重试也拿不回同一范围）
    get_calls = [c for c in deterministic_mock.http.calls if c["method"] == "GET"]
    assert len(get_calls) == 1


def test_upstream_changes_during_run_do_not_move_page_boundaries(deterministic_mock):
    """运行读到一半时上游补推：快照的 total_pages 始终稳定。"""
    mock = deterministic_mock.mock_rest
    snapshot = rest_connector.create_snapshot(CONFIG)
    pages_seen: list[int] = []

    def toggle_midway(page, payload):
        pages_seen.append(payload["total_pages"])
        if page == 2:
            mock.set_push_enabled(True)  # 实时数据变成 177 行 / 4 页

    result = rest_connector.read_paged(
        CONFIG, snapshot_id=snapshot["snapshot_id"], on_page=toggle_midway
    )
    assert pages_seen == [3, 3, 3]
    assert result["total_pages"] == 3
    assert len(result["rows"]) == 150
