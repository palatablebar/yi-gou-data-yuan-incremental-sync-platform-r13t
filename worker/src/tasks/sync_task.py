"""单次同步编排：probe → extract → transform → load → verify 全在 worker 里跑。

对 REST 类数据源提供三条一致性保证：
1. 稳定范围：运行开始先冻结上游快照（sync_scope），整个运行只读这一份数据，
   上游新增 / 改状态 / 撤回都不会改变本次范围或页边界。
2. 断点续跑：每页抽取后立即落 staging 并把下一页位置写入 resume_state；
   失败重试从 checkpoint 继续，已读页不重抽。
3. 幂等入账：目标表按冲突键 upsert/跳过、拒绝记录带运行内幂等键，
   重复执行（含重试、手动再跑）不重复入账、不重复隔离。
"""
from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import Any

from sqlalchemy.dialects.postgresql import insert as pg_insert

from src.config.database import session_scope
from src.constants import error_codes as codes
from src.constants.enums import RunStatus, Stage
from src.models.data_source import DataSource
from src.models.rejected_record import RejectedRecord
from src.models.sync_pipeline import SyncPipeline
from src.models.sync_run import SyncRun
from src.services import mapping_service
from src.utils.errors import AppError
from src.utils.logger import get_logger
from worker_src.config.celery_app import celery_app
from worker_src.connectors import rest_connector
from worker_src.core import lock, progress, storage
from worker_src.core.retry import next_delay, should_retry
from worker_src.pipelines import extract, incremental, load, transform, verify

logger = get_logger("syncmesh.worker.sync")

# 计数器字段的固定顺序，保证详情页每类处理数量口径一致
COUNTER_KEYS = (
    "snapshot_total",
    "pages_total",
    "pages_fetched",
    "duplicate_rows",
    "rows_extracted",
    "rows_filtered",
    "rows_read",
    "rows_transform_failed",
    "rows_written",
    "rows_skipped",
    "rows_conflict_rejected",
    "rows_rejected",
)


def _empty_counters() -> dict[str, int]:
    return {key: 0 for key in COUNTER_KEYS}


@celery_app.task(bind=True, name="syncmesh.sync_pipeline", max_retries=0)
def sync_pipeline(self, run_id: str) -> dict:
    with session_scope() as db:
        run = db.get(SyncRun, uuid.UUID(str(run_id)))
        if run is None:
            logger.warning("run not found run=%s", run_id)
            return {"ok": False, "reason": "run_not_found"}
        if run.status == RunStatus.CANCELED.value:
            return {"ok": False, "reason": "canceled"}

        pipeline = db.get(SyncPipeline, run.pipeline_id)
        source = db.get(DataSource, pipeline.source_id) if pipeline else None
        if pipeline is None or source is None:
            progress.finish(
                db, run, status=RunStatus.FAILED.value,
                error_message="管道或数据源已被删除", error_code=codes.NOT_FOUND,
            )
            return {"ok": False, "reason": "pipeline_missing"}

        if not lock.acquire(db, pipeline.id, run.id):
            progress.finish(
                db, run, status=RunStatus.FAILED.value,
                error_message="管道锁冲突：同一条管道已有运行在跑", error_code=codes.PIPELINE_LOCKED,
            )
            return {"ok": False, "reason": "locked"}

        progress.mark_running(db, run)

        try:
            return _run(db, run, pipeline, source)
        except AppError as exc:
            return _handle_failure(db, run, pipeline, source, exc.code, exc.message, exc.detail)
        except Exception as exc:  # noqa: BLE001
            logger.error("run crashed run=%s error=%s", run.id, exc, exc_info=True)
            return _handle_failure(db, run, pipeline, source, codes.INTERNAL_ERROR, str(exc), "")
        finally:
            lock.release(db, pipeline.id, run.id)


def _run(db, run: SyncRun, pipeline: SyncPipeline, source: DataSource) -> dict:
    mappings = mapping_service.mapping_dicts(db, pipeline.id)
    if not mappings:
        raise AppError(
            codes.PIPELINE_INVALID_MAPPING, "管道没有字段映射",
            f"SyncPipeline[id={pipeline.id}] run failed: mapping empty",
        )

    counters = dict(_empty_counters(), **(run.counters or {}))
    is_rest = source.source_type == "mock_rest"

    # 水位线起点：优先用上次成功推进的值，否则取目标表当前最大值
    watermark_column = pipeline.watermark_column
    target_max = load.target_max_watermark(
        pipeline.target_schema, pipeline.target_table, watermark_column
    )
    wm_from = incremental.watermark_from(pipeline.last_watermark, target_max)
    run.watermark_from = None if wm_from is None else str(wm_from)
    if run.counters is None:
        run.counters = counters
    db.commit()

    progress.set_stage(db, run, Stage.EXTRACT.value, watermark_from=run.watermark_from)
    logger.info("extract start run=%s source=%s", run.id, source.source_type)
    source_columns = [mapping["source_field"] for mapping in mappings]

    staging_table = storage.prepare_staging(pipeline.id, reuse=bool(run.resume_state))

    if is_rest:
        raw_rows, scope = _extract_rest_with_resume(
            db, run, pipeline, source, staging_table, counters
        )
        run.sync_scope = scope
    else:
        # CSV / JSONL / PG：文件与表本身即静态范围，记录范围类型即可
        raw_rows = extract.read_rows(source.source_type, source.config_json or {}, columns=source_columns)
        storage.write_staging(pipeline.id, staging_table, raw_rows)
        run.sync_scope = {
            "scope_type": "static_source",
            "source_type": source.source_type,
            "frozen_at": datetime.now(timezone.utc).isoformat(),
            "total_rows": len(raw_rows),
        }
        counters["rows_extracted"] = len(raw_rows)
    db.commit()

    kept, filtered, next_watermark = incremental.filter_rows(
        raw_rows,
        sync_mode=pipeline.sync_mode,
        watermark_column=watermark_column,
        watermark=wm_from,
    )
    counters["rows_filtered"] = filtered
    counters["rows_read"] = len(kept)
    run.counters = counters
    progress.bump_counters(db, run, rows_read=len(kept))
    logger.info(
        "extract done run=%s extracted=%s kept=%s filtered=%s dup=%s",
        run.id, counters["rows_extracted"], len(kept), filtered, counters["duplicate_rows"],
    )

    if not kept:
        if next_watermark is not None:
            pipeline.last_watermark = str(next_watermark)
        run.watermark_value = None if next_watermark is None else str(next_watermark)
        run.resume_state = None
        run.counters = counters
        db.commit()
        _release_snapshot(run, source)
        progress.set_stage(db, run, Stage.VERIFY.value, note="no_new_data")
        progress.finish(db, run, status=RunStatus.SUCCEEDED.value)
        logger.info("run succeeded (no new data) run=%s", run.id)
        return {"ok": True, "rows_read": 0, "no_new_data": True, "counters": counters}

    progress.set_stage(db, run, Stage.TRANSFORM.value)
    mapped_records, transform_failures = transform.transform_batch(kept, mappings)
    counters["rows_transform_failed"] = len(transform_failures)

    progress.set_stage(db, run, Stage.LOAD.value)
    outcome = load.load_to_target(
        schema=pipeline.target_schema,
        table=pipeline.target_table,
        records=mapped_records,
        conflict_strategy=pipeline.conflict_strategy,
        conflict_key=pipeline.conflict_key,
        run_id=run.id,
    )
    conflict_rejected = sum(
        1 for item in outcome.rejected if item.get("reason_code") == "conflict_rejected"
    )
    counters["rows_written"] = outcome.written
    counters["rows_skipped"] = outcome.skipped
    counters["rows_conflict_rejected"] = conflict_rejected

    rejected_items = list(transform_failures) + list(outcome.rejected)
    _persist_rejected(db, run, pipeline, rejected_items)
    counters["rows_rejected"] = len(rejected_items)
    run.counters = counters
    db.commit()

    progress.set_stage(db, run, Stage.VERIFY.value)
    staging_rows = storage.count_staging(pipeline.id, staging_table)
    verification = verify.verify_run(
        rows_read=len(kept),
        staging_rows=staging_rows,
        written=outcome.written,
        skipped=outcome.skipped,
        rejected=rejected_items,
        rows_extracted=counters["rows_extracted"],
        rows_filtered=counters["rows_filtered"],
    )
    status = verify.final_status(len(rejected_items), None)

    progress.bump_counters(
        db, run,
        rows_written=outcome.written,
        rows_rejected=len(rejected_items),
        rows_skipped=outcome.skipped,
        watermark_value=None if next_watermark is None else str(next_watermark),
    )
    if next_watermark is not None:
        pipeline.last_watermark = str(next_watermark)
    run.watermark_value = None if next_watermark is None else str(next_watermark)
    # 正常结束：checkpoint 清空，快照主动清理（另有 Redis TTL 兜底）
    run.resume_state = None
    run.counters = counters
    db.commit()

    _release_snapshot(run, source)
    progress.finish(db, run, status=status)
    if status == RunStatus.PARTIAL.value:
        logger.info(
            "run partial run=%s read=%s written=%s rejected=%s codes=%s",
            run.id, len(kept), outcome.written, len(rejected_items), verification["rejected_codes"],
        )
    else:
        logger.info("run succeeded run=%s read=%s written=%s", run.id, len(kept), outcome.written)
    return {
        "ok": True,
        "status": status,
        "rows_read": len(kept),
        "rows_written": outcome.written,
        "rows_rejected": len(rejected_items),
        "counters": counters,
        "verification": verification,
    }


def _extract_rest_with_resume(
    db,
    run: SyncRun,
    pipeline: SyncPipeline,
    source: DataSource,
    staging_table: str,
    counters: dict[str, int],
) -> tuple[list[dict], dict]:
    """REST 抽取：快照 + 页级 checkpoint + staging 追加，返回 (全部原始行, 范围描述)。"""
    config = source.config_json or {}
    resume = run.resume_state or {}
    snapshot_id = resume.get("snapshot_id")
    key_field = pipeline.conflict_key or "order_id"

    if snapshot_id:
        # 续跑：确认快照仍在，范围与首次尝试完全一致
        rest_connector.touch_snapshot(config, snapshot_id)
        scope = dict(resume.get("scope") or {})
        scope["resumed_from_attempt"] = run.retry_count + 1
        next_page = int(resume.get("next_page", 1))
        prior_rows = storage.fetch_staging_rows(pipeline.id, staging_table)
        prior_keys = storage.fetch_staging_keys(pipeline.id, staging_table, key_field)
        # 续跑尝试里已落地的页数（counters 是累计值，不能直接当本次起点）
        prior_pages = int(resume.get("pages_fetched", 0))
        logger.info(
            "rest resume run=%s snapshot=%s next_page=%s prior_rows=%s prior_pages=%s",
            run.id, snapshot_id, next_page, len(prior_rows), prior_pages,
        )
    else:
        # 首次尝试：冻结快照，确定本次稳定范围
        snapshot = rest_connector.create_snapshot(config)
        snapshot_id = snapshot["snapshot_id"]
        next_page = 1
        prior_rows = []
        prior_keys = set()
        prior_pages = 0
        page_size = int(config.get("page_size", 50))
        total = int(snapshot["total"])
        total_pages = max((total + page_size - 1) // page_size, 1)
        scope = {
            "scope_type": "upstream_snapshot",
            "source_type": source.source_type,
            "snapshot_id": snapshot_id,
            "frozen_at": snapshot["created_at"],
            "total_rows": total,
            "total_amount": float(snapshot.get("total_amount") or 0),
            "total_pages": total_pages,
            "page_size": page_size,
            "push_batch": bool(snapshot.get("push_batch")),
        }
        counters["snapshot_total"] = total
        counters["pages_total"] = total_pages
        run.sync_scope = scope
        run.resume_state = {
            "snapshot_id": snapshot_id,
            "next_page": 1,
            "pages_fetched": 0,
            "scope": scope,
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }
        run.counters = counters
        db.commit()

    appended: list[dict[str, Any]] = []
    seen_new_keys: set = set()
    pages_done = {"value": prior_pages}

    def _on_page(page: int, payload: dict[str, Any]) -> None:
        # 连接器已按业务键跨页去重；这里只把本页原始行追加进 staging 并推进 checkpoint
        page_rows = payload.get("items", [])
        storage.write_staging(
            pipeline.id,
            staging_table,
            page_rows,
            start_index=len(prior_rows) + len(appended),
        )
        appended.extend(page_rows)
        pages_done["value"] += 1
        counters["pages_fetched"] = pages_done["value"]
        run.resume_state = {
            "snapshot_id": snapshot_id,
            "next_page": page + 1,
            "pages_fetched": pages_done["value"],
            "scope": scope,
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }
        run.counters = counters
        db.commit()

    result = rest_connector.read_paged(
        config,
        snapshot_id=snapshot_id,
        start_page=next_page,
        key_field=key_field,
        on_page=_on_page,
    )

    # 跨尝试去重：极端情况下（页已拉取但 checkpoint 提交前崩溃）同一页可能被重拉，
    # 用 staging 里已有的业务键兜底，重复行只计数、不进入后续处理，绝不变成冲突拒绝。
    new_rows: list[dict[str, Any]] = []
    cross_attempt_dupes = 0
    for row in result["rows"]:
        key = row.get(key_field)
        if key is not None and (key in prior_keys or key in seen_new_keys):
            cross_attempt_dupes += 1
            continue
        if key is not None:
            seen_new_keys.add(key)
        new_rows.append(row)

    counters["duplicate_rows"] = int(counters.get("duplicate_rows", 0)) + int(result["duplicate_rows"]) + cross_attempt_dupes
    all_rows = prior_rows + new_rows
    counters["rows_extracted"] = len(all_rows)
    scope["total_pages"] = result["total_pages"]
    scope["total_rows"] = result["total"]
    scope["cross_attempt_duplicates"] = cross_attempt_dupes
    run.sync_scope = scope
    run.counters = counters
    db.commit()
    return all_rows, scope


def _release_snapshot(run: SyncRun, source: DataSource) -> None:
    """运行走到终态时主动释放快照；清理失败只告警，快照另有 Redis TTL 兜底。"""
    scope = run.sync_scope or {}
    snapshot_id = scope.get("snapshot_id")
    if snapshot_id:
        rest_connector.delete_snapshot(source.config_json or {}, snapshot_id)


def _persist_rejected(db, run: SyncRun, pipeline: SyncPipeline, rejected_items: list[dict[str, Any]]) -> None:
    """拒绝记录按运行内幂等键写入：续跑/重试不会产生重复隔离行。"""
    if not rejected_items:
        return
    payloads = []
    for item in rejected_items:
        row_index = int(item.get("source_row_index") or 0)
        reason_code = item.get("reason_code") or "transform_failed"
        payloads.append(
            {
                "run_id": run.id,
                "pipeline_id": pipeline.id,
                "raw_payload_json": _json_safe(item.get("raw_payload") or {}),
                "reason_code": reason_code,
                "reason_detail": str(item.get("reason_detail") or "")[:2000],
                "source_row_index": row_index,
                "idempotency_key": f"{row_index}:{reason_code}",
                "created_at": datetime.now(timezone.utc),
            }
        )
    stmt = pg_insert(RejectedRecord).values(payloads)
    stmt = stmt.on_conflict_do_nothing(
        index_elements=["run_id", "idempotency_key"],
    )
    db.execute(stmt)


def _handle_failure(db, run: SyncRun, pipeline: SyncPipeline, source: DataSource, code: str, message: str, detail: str) -> dict:
    logger.error("run failed run=%s code=%s error=%s detail=%s", run.id, code, message, detail)
    retryable = should_retry(run.retry_count, code)
    if retryable:
        run.retry_count = run.retry_count + 1
        run.status = RunStatus.QUEUED.value
        run.error_code = code
        run.error_message = f"{message}（第 {run.retry_count} 次重试已排期，将从断点续跑）"
        # 失败时保留 resume_state / sync_scope / staging，重试直接复用，快照不释放
        db.commit()
        delay = next_delay(run.retry_count)
        logger.warning(
            "run retry scheduled run=%s retry=%s delay=%s resume=%s",
            run.id, run.retry_count, delay, run.resume_state,
        )
        sync_pipeline.apply_async(args=[str(run.id)], countdown=delay)
        return {"ok": False, "retry": run.retry_count, "delay": delay, "resume_state": run.resume_state}
    # 终态失败：不再续跑，释放快照（TTL 仍兜底）
    _release_snapshot(run, source)
    progress.finish(db, run, status=RunStatus.FAILED.value, error_message=f"{message} {detail}".strip(), error_code=code)
    return {"ok": False, "code": code, "error": message}


def _json_safe(payload: dict) -> dict:
    from datetime import date

    safe = {}
    for key, value in payload.items():
        if isinstance(value, (str, int, float, bool)) or value is None:
            safe[key] = value
        elif isinstance(value, (datetime, date)):
            safe[key] = value.isoformat()
        else:
            safe[key] = str(value)
    return safe
