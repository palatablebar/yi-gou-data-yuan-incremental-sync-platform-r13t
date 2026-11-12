import type { RunCounters, ResumeState, SyncScope } from '../../types/models';
import { formatTimestamp } from '../../utils/formatCount';

interface Props {
  scope: SyncScope | null;
  resume: ResumeState | null;
  counters: RunCounters;
  retryCount: number;
  status: string;
}

const CounterLabel: Record<string, string> = {
  snapshot_total: '快照总行数',
  pages_total: '快照总页数',
  pages_fetched: '已抓取页数',
  duplicate_rows: '快照内重复行（已去重）',
  rows_extracted: '抽取行数',
  rows_filtered: '水位线过滤',
  rows_read: '本批处理行',
  rows_transform_failed: '转换拒绝',
  rows_written: '成功写入',
  rows_skipped: '冲突跳过',
  rows_conflict_rejected: '冲突拒绝',
  rows_rejected: '拒绝合计',
};

const CounterOrder = [
  'snapshot_total',
  'pages_total',
  'pages_fetched',
  'duplicate_rows',
  'rows_extracted',
  'rows_filtered',
  'rows_read',
  'rows_transform_failed',
  'rows_written',
  'rows_skipped',
  'rows_conflict_rejected',
  'rows_rejected',
];

export const RunScopePanel = ({ scope, resume, counters, retryCount, status }: Props) => {
  const isSnapshot = scope?.scope_type === 'upstream_snapshot';
  const running = status === 'running' || status === 'queued';
  return (
    <section className="border-b px-4 py-3" style={{ borderColor: 'var(--color-wire)' }}>
      <div className="label-caps mb-2">本次运行范围与续跑位置</div>
      <div className="grid grid-cols-1 gap-3 md:grid-cols-2">
        <div className="border p-3" style={{ borderColor: 'var(--color-wire)' }}>
          <div className="mb-1 text-micro text-dim">稳定范围</div>
          {!scope ? (
            <p className="text-dense text-dim">尚未建立范围</p>
          ) : isSnapshot ? (
            <dl className="space-y-1 text-micro">
              <div className="flex justify-between gap-2">
                <dt className="text-dim">范围类型</dt>
                <dd className="num text-ink">上游快照（冻结）</dd>
              </div>
              <div className="flex justify-between gap-2">
                <dt className="text-dim">快照 ID</dt>
                <dd className="num truncate text-ink" title={scope.snapshot_id}>
                  {scope.snapshot_id?.slice(0, 12)}…
                </dd>
              </div>
              <div className="flex justify-between gap-2">
                <dt className="text-dim">冻结时刻</dt>
                <dd className="num text-ink">{formatTimestamp(scope.frozen_at ?? null)}</dd>
              </div>
              <div className="flex justify-between gap-2">
                <dt className="text-dim">范围行数 / 页数</dt>
                <dd className="num text-ink">
                  {scope.total_rows} 行 / {scope.total_pages} 页（每页 {scope.page_size}）
                </dd>
              </div>
              <div className="flex justify-between gap-2">
                <dt className="text-dim">范围金额合计</dt>
                <dd className="num text-ink">
                  {scope.total_amount !== undefined ? Number(scope.total_amount).toFixed(2) : '—'}
                </dd>
              </div>
              <div className="flex justify-between gap-2">
                <dt className="text-dim">补推批次</dt>
                <dd className="num text-ink">{scope.push_batch ? '含补推' : '不含'}</dd>
              </div>
              {scope.resumed_from_attempt ? (
                <div className="flex justify-between gap-2">
                  <dt className="text-dim">续跑来源</dt>
                  <dd className="num" style={{ color: 'var(--color-warn)' }}>
                    第 {scope.resumed_from_attempt} 次尝试续跑
                  </dd>
                </div>
              ) : null}
            </dl>
          ) : (
            <dl className="space-y-1 text-micro">
              <div className="flex justify-between gap-2">
                <dt className="text-dim">范围类型</dt>
                <dd className="num text-ink">静态源（{scope.source_type}）</dd>
              </div>
              <div className="flex justify-between gap-2">
                <dt className="text-dim">范围行数</dt>
                <dd className="num text-ink">{scope.total_rows} 行</dd>
              </div>
            </dl>
          )}
        </div>

        <div className="border p-3" style={{ borderColor: 'var(--color-wire)' }}>
          <div className="mb-1 text-micro text-dim">续跑位置（checkpoint）</div>
          {running && resume?.snapshot_id ? (
            <dl className="space-y-1 text-micro">
              <div className="flex justify-between gap-2">
                <dt className="text-dim">下一页</dt>
                <dd className="num text-ink">
                  第 {resume.next_page} 页 / 共 {resume.scope?.total_pages ?? '—'} 页
                </dd>
              </div>
              <div className="flex justify-between gap-2">
                <dt className="text-dim">已完成页</dt>
                <dd className="num text-ink">{resume.pages_fetched} 页</dd>
              </div>
              <div className="flex justify-between gap-2">
                <dt className="text-dim">checkpoint 更新于</dt>
                <dd className="num text-ink">{formatTimestamp(resume.updated_at ?? null)}</dd>
              </div>
              <p className="pt-1 text-micro text-dim">
                失败重试将从该页继续，已落地页不会重复抓取。
              </p>
            </dl>
          ) : (
            <p className="text-dense text-dim">
              {retryCount > 0
                ? `本次运行经历 ${retryCount} 次重试后完成，checkpoint 已清空。`
                : '运行已结束（或尚未开始抽取），无待续跑位置。'}
            </p>
          )}
        </div>
      </div>

      <div className="mt-3">
        <div className="label-caps mb-1">每类处理数量</div>
        <div className="flex flex-wrap gap-2">
          {CounterOrder.filter((key) => counters[key as keyof RunCounters] !== undefined).map((key) => {
            const value = counters[key as keyof RunCounters] ?? 0;
            const emphasize = ['rows_written', 'rows_rejected', 'duplicate_rows'].includes(key);
            return (
              <div
                key={key}
                className="border px-2 py-1 text-micro"
                style={{ borderColor: 'var(--color-wire)' }}
                title={CounterLabel[key]}
              >
                <span className="text-dim">{CounterLabel[key]}</span>{' '}
                <span
                  className="num"
                  style={{
                    color: emphasize && value > 0 ? 'var(--color-signal)' : 'var(--color-ink)',
                  }}
                >
                  {value}
                </span>
              </div>
            );
          })}
        </div>
      </div>
    </section>
  );
};
