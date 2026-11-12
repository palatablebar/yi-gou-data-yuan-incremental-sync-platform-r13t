"""校验阶段账目恒等式：每一类行都有明确去向，不漏不重。"""
from __future__ import annotations

from worker_src.pipelines import verify  # conftest 已把 worker/src 注册为 worker_src


def _rejected(codes_counts: dict[str, int]) -> list[dict]:
    return [
        {"reason_code": code, "raw_payload": {}, "source_row_index": i}
        for code, count in codes_counts.items()
        for i in range(count)
    ]


def test_accounting_identity_balances_when_every_row_has_destination():
    # 150 抽取 = 10 过滤 + 5 转换拒绝 + 120 写入 + 8 跳过 + 7 冲突拒绝
    rejected = _rejected({"required_missing": 5, "conflict_rejected": 7})
    result = verify.verify_run(
        rows_read=140,  # 抽取 150 - 过滤 10 = 进入处理的行数
        staging_rows=150,
        written=120,
        skipped=8,
        rejected=rejected,
        rows_extracted=150,
        rows_filtered=10,
    )
    assert result["ok"] is True
    assert result["conflict_rejects"] == 7
    assert result["rows_filtered"] == 10


def test_accounting_fails_when_a_row_has_no_destination():
    rejected = _rejected({"conflict_rejected": 2})
    result = verify.verify_run(
        rows_read=10,
        staging_rows=10,
        written=5,  # 5 + 1 跳过 + 2 拒绝 = 8，少 2 行没去向
        skipped=1,
        rejected=rejected,
        rows_extracted=10,
        rows_filtered=0,
    )
    assert result["ok"] is False


def test_staging_count_must_equal_extracted_rows():
    result = verify.verify_run(
        rows_read=10,
        staging_rows=9,  # staging 丢了一行
        written=10,
        skipped=0,
        rejected=[],
        rows_extracted=10,
        rows_filtered=0,
    )
    assert result["ok"] is False


def test_legacy_path_without_extracted_keeps_old_rule():
    # 不传 rows_extracted 时沿用旧口径：staging == written + rejected
    result = verify.verify_run(
        rows_read=7, staging_rows=7, written=5, skipped=0,
        rejected=_rejected({"conflict_rejected": 2}),
    )
    assert result["ok"] is True
