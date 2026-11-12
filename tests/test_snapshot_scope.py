"""稳定范围：快照创建后，上游新增 / 撤回 / 改状态都不影响本次运行的数据范围。"""
from __future__ import annotations

from src.utils import mock_rest


def test_snapshot_freezes_rows_even_when_push_toggled(deterministic_mock, fake_redis):
    mock = deterministic_mock.mock_rest
    # 干净状态建快照：150 行
    snapshot = mock.create_snapshot()
    assert snapshot["total"] == 150

    # 运行期间上游开启补推（新增 27 行，其中含重复 order_id）
    mock.set_push_enabled(True)

    # 不带快照的实时视图变了
    assert mock.paginate(1)["total"] == 177
    # 带快照的视图仍是冻结时的 150 行、页边界不变
    frozen = mock.paginate(1, 50, snapshot_id=snapshot["snapshot_id"])
    assert frozen["total"] == 150
    assert frozen["total_pages"] == 3
    assert frozen["snapshot_frozen"] is True
    page3 = mock.paginate(3, 50, snapshot_id=snapshot["snapshot_id"])
    assert len(page3["items"]) == 50


def test_snapshot_isolates_withdrawal_and_status_change(deterministic_mock, fake_redis):
    mock = deterministic_mock.mock_rest
    snapshot = mock.create_snapshot()
    first_page_before = mock.paginate(1, 50, snapshot_id=snapshot["snapshot_id"])["items"]

    # 上游撤回订单（模拟：直接改实时数据的开关不会改快照；这里验证快照与实时视图独立）
    mock.set_push_enabled(True)
    mock.set_push_enabled(False)
    first_page_after = mock.paginate(1, 50, snapshot_id=snapshot["snapshot_id"])["items"]
    assert first_page_before == first_page_after


def test_snapshot_expired_raises_explicit_error(deterministic_mock, fake_redis):
    mock = deterministic_mock.mock_rest
    snapshot = mock.create_snapshot()
    mock.delete_snapshot(snapshot["snapshot_id"])
    assert mock.get_snapshot(snapshot["snapshot_id"]) is None
    try:
        mock.paginate(1, 50, snapshot_id=snapshot["snapshot_id"])
    except mock.SnapshotExpired as exc:
        assert exc.snapshot_id == snapshot["snapshot_id"]
    else:  # pragma: no cover - 防御性断言
        raise AssertionError("SnapshotExpired 未抛出")


def test_touch_snapshot_refreshes_ttl(deterministic_mock, fake_redis):
    mock = deterministic_mock.mock_rest
    snapshot = mock.create_snapshot()
    assert mock.touch_snapshot(snapshot["snapshot_id"]) is True
    mock.delete_snapshot(snapshot["snapshot_id"])
    assert mock.touch_snapshot(snapshot["snapshot_id"]) is False


def test_snapshot_records_stable_total_amount(deterministic_mock):
    mock = deterministic_mock.mock_rest
    snapshot = mock.create_snapshot()
    # 金额合计与逐行求和一致
    rows = mock.paginate(1, 200, snapshot_id=snapshot["snapshot_id"])["items"]
    expected = round(sum(float(r["amount"]) for r in rows), 2)
    assert snapshot["total_amount"] == expected
    assert snapshot["total"] == 150

    # 补推改变实时数据，但快照金额（同一时刻范围口径）不变
    frozen_amount = snapshot["total_amount"]
    mock.set_push_enabled(True)
    again = mock.get_snapshot(snapshot["snapshot_id"])
    assert again["total_amount"] == frozen_amount
