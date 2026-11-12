"""校验：行数账目 + 必填已由 transform 保证 + staging 与写入数对账。

账目恒等式（每一类行都有明确去向，不漏不重）：
    抽取行 rows_extracted
      = 水位线过滤 rows_filtered
      + 转换拒绝 rows_transform_failed
      + 写入 rows_written
      + 跳过 rows_skipped
      + 冲突拒绝 rows_conflict_rejected
其中 staging 存的是全部抽取原始行，所以 staging_rows == rows_extracted。
"""
from __future__ import annotations

from typing import Any

from src.services import verify_service
from worker_src.constants.reason_codes import (
    REASON_CONFLICT_REJECTED,
    REASON_TRANSFORM_FAILED,
)


def verify_run(
    *,
    rows_read: int,
    staging_rows: int,
    written: int,
    skipped: int,
    rejected: list[dict[str, Any]],
    rows_extracted: int | None = None,
    rows_filtered: int = 0,
) -> dict[str, Any]:
    totals = verify_service.verify_totals(rows_read, written, skipped, len(rejected))
    codes: dict[str, int] = {}
    for item in rejected:
        key = item.get("reason_code") or REASON_TRANSFORM_FAILED
        codes[key] = codes.get(key, 0) + 1
    conflict_rejects = codes.get(REASON_CONFLICT_REJECTED, 0)
    transform_rejects = len(rejected) - conflict_rejects

    # 新口径：staging 是全部抽取行，账面上每一行都能对上一个去向
    if rows_extracted is not None:
        accounted = rows_filtered + transform_rejects + written + skipped + conflict_rejects
        scope_ok = staging_rows == rows_extracted and accounted == rows_extracted
    else:
        # 兼容旧口径
        scope_ok = staging_rows == written + len(rejected)

    return {
        "ok": totals["ok"] and scope_ok,
        "totals": totals,
        "rejected_codes": codes,
        "conflict_rejects": conflict_rejects,
        "staging_rows": staging_rows,
        "written": written,
        "skipped": skipped,
        "rejected": len(rejected),
        "rows_extracted": rows_extracted,
        "rows_filtered": rows_filtered,
    }


def final_status(rejected_count: int, error: str | None) -> str:
    from src.constants.enums import RunStatus

    if error:
        return RunStatus.FAILED.value
    if rejected_count > 0:
        return RunStatus.PARTIAL.value
    return RunStatus.SUCCEEDED.value
