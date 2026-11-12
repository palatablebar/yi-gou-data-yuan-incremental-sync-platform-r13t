"""SyncRun 增加稳定范围 / 续跑位置 / 分类计数；RejectedRecord 增加运行内幂等键。

注意：0001 用 Base.metadata.create_all 建表，全新库建出的是「当前模型」结构，
可能已包含这些列，因此本迁移对每个对象先查 information_schema 再决定是否创建，
保证 0001→0002 在全新库与旧库升级两条路径上都能成功。

Revision ID: 0002_run_scope_resume
"""
from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "0002_run_scope_resume"
down_revision = "0001_initial"
branch_labels = None
depends_on = None

META = "meta"


def _column_exists(bind, table: str, column: str) -> bool:
    return bool(
        bind.execute(
            sa.text(
                "SELECT 1 FROM information_schema.columns "
                "WHERE table_schema = :schema AND table_name = :table AND column_name = :column"
            ),
            {"schema": META, "table": table, "column": column},
        ).first()
    )


def _index_exists(bind, index: str) -> bool:
    return bool(
        bind.execute(
            sa.text(
                "SELECT 1 FROM pg_indexes WHERE schemaname = :schema AND indexname = :index"
            ),
            {"schema": META, "index": index},
        ).first()
    )


def upgrade() -> None:
    bind = op.get_bind()

    if not _column_exists(bind, "sync_runs", "sync_scope"):
        op.add_column(
            "sync_runs",
            sa.Column("sync_scope", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
            schema=META,
        )
    if not _column_exists(bind, "sync_runs", "resume_state"):
        op.add_column(
            "sync_runs",
            sa.Column("resume_state", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
            schema=META,
        )
    if not _column_exists(bind, "sync_runs", "counters"):
        op.add_column(
            "sync_runs",
            sa.Column(
                "counters",
                postgresql.JSONB(astext_type=sa.Text()),
                nullable=False,
                server_default=sa.text("'{}'::jsonb"),
            ),
            schema=META,
        )
    if not _column_exists(bind, "rejected_records", "idempotency_key"):
        op.add_column(
            "rejected_records",
            sa.Column("idempotency_key", sa.String(length=180), nullable=True),
            schema=META,
        )

    # 同一次运行里同一来源行同一原因只保留一条拒绝记录：续跑/重试不重复隔离
    if not _index_exists(bind, "uq_rejected_run_idempotency"):
        op.create_index(
            "uq_rejected_run_idempotency",
            "rejected_records",
            ["run_id", "idempotency_key"],
            unique=True,
            schema=META,
            postgresql_where=sa.text("idempotency_key IS NOT NULL"),
        )


def downgrade() -> None:
    bind = op.get_bind()
    if _index_exists(bind, "uq_rejected_run_idempotency"):
        op.drop_index("uq_rejected_run_idempotency", table_name="rejected_records", schema=META)
    if _column_exists(bind, "rejected_records", "idempotency_key"):
        op.drop_column("rejected_records", "idempotency_key", schema=META)
    if _column_exists(bind, "sync_runs", "counters"):
        op.drop_column("sync_runs", "counters", schema=META)
    if _column_exists(bind, "sync_runs", "resume_state"):
        op.drop_column("sync_runs", "resume_state", schema=META)
    if _column_exists(bind, "sync_runs", "sync_scope"):
        op.drop_column("sync_runs", "sync_scope", schema=META)
