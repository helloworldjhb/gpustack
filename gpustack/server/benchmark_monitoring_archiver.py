"""Leader-scheduled monitoring capture with shared leases and fenced publication."""

import asyncio
from copy import deepcopy
from hashlib import sha256
import logging
import math
import json
import re
import time
from decimal import Decimal, ROUND_CEILING
from types import SimpleNamespace
from uuid import uuid4

from sqlalchemy import delete, func, update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import defer
from sqlmodel import select

from gpustack.artifacts import ArtifactStorageError, get_artifact_store
from gpustack.artifacts.capacity import CapacityManager
from gpustack.artifacts.managed import delete_managed_object, put_managed_json
from gpustack.artifacts.manifest import ArchiveManifestWriter
from gpustack.monitoring import benchmarks as collectors
from gpustack.monitoring.archive import (
    _object,
    _read_entry,
    execution_run_id,
    ensure_archive_for_benchmark,
)
from gpustack.monitoring.time_range import MonitoringInterval
from gpustack.monitoring.config import get_query_config
from gpustack.schemas.benchmark import Benchmark
from gpustack.schemas.benchmark_archive import (
    BenchmarkMonitoringArchive,
    BenchmarkMonitoringChunk,
    BenchmarkMonitoringRevision,
    BenchmarkMonitoringScope,
)
from gpustack.schemas.benchmark_monitoring import (
    BenchmarkModelMetrics,
    BenchmarkResourceMetrics,
    BenchmarkMonitoringContext,
    BenchmarkMonitoringMember,
    BenchmarkMonitoringWindow,
    BenchmarkRunSnapshot,
)
from gpustack.schemas.model_metrics import (
    ModelMetric,
    ModelMonitoringCapabilities,
    ResourceWorker,
)
from gpustack.server.db import async_session

logger = logging.getLogger(__name__)
_TERMINAL = {'completed', 'stopped', 'error', 'unreachable'}
_LEASE_SECONDS = 180
_BLOCK_SECONDS = 300
_MAX_SCOPES = 2048


class ArchiveLeaseLost(RuntimeError):
    pass


class ArchiveCaptureError(RuntimeError):
    def __init__(self, reason):
        self.reason = reason
        super().__init__(reason)


def _object_filename(scope: str, object_id: str) -> str:
    """Describe the scope without replacing its immutable object identity."""
    parts = scope.split('/')
    if len(parts) == 4:
        interval, kind, role, view = parts
        parts = ([] if interval == 'run' else [interval]) + [kind]
        if role != 'all':
            parts.append(role)
        if kind != 'resources' or view != 'aggregate':
            parts.append(view)
    parts = [
        re.sub(r'[^a-z0-9]+', '-', part.lower()).strip('-')[:48].rstrip('-')
        or 'unknown'
        for part in parts[:4]
    ]
    return f'{"-".join(parts)}-{object_id}.json.gz'


def _value(value):
    return getattr(value, 'value', value)


def _metadata(response):
    data = response.model_dump(mode='json', exclude_none=False)
    for metric in data['metrics'].values():
        metric['series'] = []
    return data


def _capture_incomplete(response):
    """Query failures and invalidated identities leave capture incomplete."""
    if getattr(response, '_archive_empty_population', False):
        return False
    return any(
        metric.status == 'query_error'
        or metric.reason == 'historical_association_changed'
        for metric in response.metrics.values()
    )


def _data_partial(response):
    if getattr(response, '_archive_empty_population', False):
        return False
    expected = getattr(response, '_archive_expected_samples', None)
    missing_trends = expected is not None and any(
        len(line.points) < expected
        for metric in response.metrics.values()
        if metric.status in ('available', 'stale')
        for line in metric.series
    )
    return (
        missing_trends
        or any(
            metric.status in ('query_error', 'no_data', 'restricted')
            or metric.reason == 'insufficient_phase_rate_window'
            or bool(metric.coverage and metric.coverage.partial)
            for metric in response.metrics.values()
        )
        or any(
            s.coverage < 1
            for values in response.sample_statistics.values()
            for s in values
        )
    )


def _retains_saved_points(before, response):
    """A retry with source gaps retains every previously saved observation."""
    for key, old_metric in before.get('metrics', {}).items():
        metric = response.metrics.get(key)
        series = {}
        for line in metric.series if metric else []:
            identity = (
                line.statistic,
                json.dumps(line.dimensions.model_dump(), sort_keys=True),
            )
            series[identity] = (dict(line.points), dict(line.lower_bounds or []))
        for line in old_metric.get('series', []):
            identity = (
                line['statistic'],
                json.dumps(line.get('dimensions', {}), sort_keys=True),
            )
            points, bounds = series.get(identity, ({}, {}))
            if any(
                value is not None and points.get(at) is None
                for at, value in line['points']
            ):
                return False
            if any(
                points.get(at) is None and bounds.get(at, -1) < bound
                for at, bound in line.get('lower_bounds', [])
            ):
                return False
    return True


def _combine_samples(before, after):
    """Merge sampled gauges; native histogram summaries stay independent."""
    combined = deepcopy(after)
    for key in set(before) | set(after):
        merged = {}
        for sample in before.get(key, []) + after.get(key, []):
            identity = json.dumps(sample['dimensions'], sort_keys=True)
            if identity not in merged:
                merged[identity] = deepcopy(sample)
                continue
            old = merged[identity]
            left, right = old['observed_samples'], sample['observed_samples']
            count = left + right
            for field in ('average', 'threshold_fraction'):
                values = [(old.get(field), left), (sample.get(field), right)]
                total = sum(weight for value, weight in values if value is not None)
                old[field] = (
                    sum(value * weight for value, weight in values if value is not None)
                    / total
                    if total
                    else None
                )
            if sample.get('peak') is not None and (
                old.get('peak') is None or sample['peak'] > old['peak']
            ):
                old['peak'], old['peak_at'] = sample['peak'], sample['peak_at']
            if sample.get('last_at') is not None and (
                old.get('last_at') is None or sample['last_at'] > old['last_at']
            ):
                old['last'], old['last_at'] = sample['last'], sample['last_at']
            old['observed_samples'] = count
            old['expected_samples'] += sample['expected_samples']
            old['coverage'] = (
                min(1, count / old['expected_samples'])
                if old['expected_samples']
                else 0
            )
        combined[key] = list(merged.values())
    return combined


def _sample_summaries(metadata):
    for key, values in metadata.get('sample_statistics', {}).items():
        if key in metadata['metrics'] and metadata['metrics'][key]['kind'] == 'gauge':
            metadata['metrics'][key]['summaries'] = [
                dict(statistic='current', dimensions=s['dimensions'], value=s['last'])
                for s in values
            ]


def _retain_successful_metrics(response, before):
    """A failed retry cannot erase observations already durably captured."""
    if before.get('start') != response.start or before.get('end') != response.end:
        return response
    for key, metric in response.metrics.items():
        previous = before.get('metrics', {}).get(key, {})
        if metric.status in ('query_error', 'no_data', 'restricted') and previous.get(
            'status'
        ) in ('available', 'stale', 'no_observations'):
            response.metrics[key] = ModelMetric.model_validate(previous)
    return response


def frozen_history(benchmark, snapshot, results, *, now=None):
    """Build collector input solely from the execution snapshot, never current rows."""
    recorded = BenchmarkRunSnapshot.model_validate(snapshot['monitoring'])
    stages, window = collectors.benchmark_windows(results, mode=snapshot['load_mode'])
    if window is None and recorded.confirmed_at is not None:
        available = [s for s in stages if s.traffic_start is not None]
        start = min((s.traffic_start for s in available), default=recorded.confirmed_at)
        end = max(
            (s.traffic_end for s in available),
            default=(
                snapshot.get('observed_at', now or time.time())
                if _value(benchmark.state) in _TERMINAL
                else now or time.time()
            ),
        )
        if end > start:
            window = BenchmarkMonitoringWindow(start=start, end=end)
    if window is not None and _value(benchmark.state) not in _TERMINAL:
        window.end = max(window.end, now or time.time())
    if window is not None and _value(benchmark.state) in (
        'stopped',
        'error',
        'unreachable',
    ):
        window.end = max(window.end, snapshot.get('observed_at', window.end))
    workers = {w.id: w for w in recorded.workers}
    context = BenchmarkMonitoringContext(
        benchmark_id=benchmark.id,
        model_id=recorded.model_id,
        available=recorded.confirmed_at is not None and window is not None,
        reason=None if window is not None else 'run_timing_unavailable',
        run_window=window,
        stages=stages,
        instances=[
            BenchmarkMonitoringMember.model_validate(m.model_dump())
            for m in recorded.instances
        ],
        workers=[ResourceWorker(id=w.id, name=w.name) for w in recorded.workers],
        devices=[
            collectors._public_device(d, workers)
            for d in recorded.devices
            if d.worker_id in workers
        ],
        events=recorded.events,
        resource_status='available' if workers else 'unavailable',
        resource_reason=None if workers else 'historical_association_unavailable',
        capabilities=ModelMonitoringCapabilities(
            model_id=recorded.model_id, min_query_seconds=1, supports_custom_range=False
        ),
    )
    return collectors.BenchmarkHistory(
        context=context,
        snapshot=recorded,
        model=SimpleNamespace(
            id=recorded.model_id,
            cluster_id=recorded.cluster_id,
            backend=recorded.backend,
            roles=recorded.roles,
            disaggregation=recorded.disaggregation,
        ),
        instances=recorded.instances,
        workers=workers,
        devices=recorded.devices,
        target_mode=snapshot['target_mode'],
        gateway_url='captured' if snapshot['via_gateway'] else None,
    )


class BenchmarkMonitoringArchiver:
    """Poll persisted runs; each committed revision can be read by every Server."""

    def __init__(self, config):
        self.config = config
        self.worker_id = str(uuid4())
        self.flush_interval = getattr(
            config, 'benchmark_archive_flush_interval_seconds', 60
        )
        self.step = getattr(config, 'benchmark_archive_sample_step_seconds', 15)
        self.settle = getattr(config, 'benchmark_archive_settle_seconds', 30)
        self.max_bytes = getattr(
            config, 'benchmark_archive_max_run_bytes', 256 * 1024 * 1024
        )
        self.manifests = ArchiveManifestWriter(
            config,
            session_factory=lambda: async_session(),
            store_factory=lambda *args, **kwargs: get_artifact_store(*args, **kwargs),
        )

    async def run(self):
        if get_artifact_store(self.config) is None:
            return
        manifests = asyncio.create_task(self.manifests.run())
        try:
            while True:
                try:
                    await self.tick()
                except asyncio.CancelledError:
                    raise
                except Exception:
                    # Exception text can contain selectors or credentials.
                    logger.error('Benchmark monitoring archive scheduler failed')
                await asyncio.sleep(min(self.flush_interval, 60))
        finally:
            manifests.cancel()
            await asyncio.gather(manifests, return_exceptions=True)

    async def tick(self):
        await self._discover()
        await self._expire()
        await self._clean_orphans()
        async with async_session() as session:
            result = await session.exec(
                select(BenchmarkMonitoringArchive.id)
                .where(
                    BenchmarkMonitoringArchive.retry_at <= time.time(),
                    BenchmarkMonitoringArchive.state.notin_(
                        ['ready', 'partial', 'no_data', 'expired']
                    ),
                )
                .limit(100)
            )
            ids = result.all()
        for archive_id in ids:
            token = await self.claim(archive_id)
            if token is None:
                continue
            heartbeat = asyncio.create_task(self._heartbeat(archive_id, token))
            try:
                await self._capture(archive_id, token)
            except asyncio.CancelledError:
                raise
            except ArchiveLeaseLost:
                pass
            except Exception as exc:
                await self._fail(
                    archive_id,
                    token,
                    (
                        exc.reason
                        if isinstance(exc, (ArtifactStorageError, ArchiveCaptureError))
                        else type(exc).__name__
                    ),
                )
            finally:
                heartbeat.cancel()
                await asyncio.gather(heartbeat, return_exceptions=True)

    async def _discover(self):
        store = get_artifact_store(self.config)
        if store is None:
            return
        async with async_session() as session:
            result = await session.exec(
                select(Benchmark)
                .options(defer(Benchmark.raw_metrics))
                .where(Benchmark.deleted_at.is_(None))
                .order_by(Benchmark.updated_at.desc(), Benchmark.id.desc())
                .limit(100)
            )
            benchmarks = result.all()
            existing = await session.exec(
                select(
                    BenchmarkMonitoringArchive.benchmark_id,
                    BenchmarkMonitoringArchive.run_id,
                ).where(
                    BenchmarkMonitoringArchive.benchmark_id.in_(
                        [b.id for b in benchmarks]
                    )
                )
            )
            known = set(existing.all())
            for benchmark in benchmarks:
                monitoring = getattr(benchmark.snapshot, 'monitoring', None)
                run_id = execution_run_id(benchmark)
                if (
                    run_id is None
                    or monitoring is None
                    or (benchmark.id, run_id) in known
                ):
                    continue
                if (
                    monitoring.confirmed_at is None
                    and _value(benchmark.state) not in _TERMINAL
                ):
                    continue
                try:
                    async with session.begin_nested():
                        await ensure_archive_for_benchmark(session, benchmark)
                        # Keep insert races inside this candidate's savepoint.
                        await session.flush()
                except IntegrityError:
                    continue
            await session.commit()

    async def claim(self, archive_id):
        now = time.time()
        async with async_session() as session:
            result = await session.execute(
                update(BenchmarkMonitoringArchive)
                .where(
                    BenchmarkMonitoringArchive.id == archive_id,
                    BenchmarkMonitoringArchive.lease_until <= now,
                )
                .values(
                    lease_owner=self.worker_id,
                    lease_until=now + _LEASE_SECONDS,
                    fencing_token=BenchmarkMonitoringArchive.fencing_token + 1,
                )
            )
            await session.commit()
            if result.rowcount != 1:
                return None
            archive = await session.get(BenchmarkMonitoringArchive, archive_id)
            return archive.fencing_token

    async def _heartbeat(self, archive_id, token):
        while True:
            await asyncio.sleep(30)
            async with async_session() as session:
                result = await session.execute(
                    update(BenchmarkMonitoringArchive)
                    .where(*self._fence(archive_id, token))
                    .values(lease_until=time.time() + _LEASE_SECONDS)
                )
                await session.commit()
                if result.rowcount != 1:
                    raise ArchiveLeaseLost()

    def _fence(self, archive_id, token):
        return (
            BenchmarkMonitoringArchive.id == archive_id,
            BenchmarkMonitoringArchive.lease_owner == self.worker_id,
            BenchmarkMonitoringArchive.fencing_token == token,
            BenchmarkMonitoringArchive.lease_until > time.time(),
        )

    async def _fail(self, archive_id, token, reason):
        async with async_session() as session:
            archive = await session.get(BenchmarkMonitoringArchive, archive_id)
            if archive is None:
                return
            await session.execute(
                update(BenchmarkMonitoringArchive)
                .where(*self._fence(archive_id, token))
                .values(
                    state=(
                        'expiring'
                        if archive.state == 'expiring'
                        else 'deleting' if archive.delete_requested else 'failed'
                    ),
                    last_error=reason[:128],
                    attempts=archive.attempts + 1,
                    retry_at=time.time()
                    + (
                        60
                        if reason == 'storage_capacity_exceeded'
                        else min(3600, 30 * 2 ** min(archive.attempts, 7))
                    ),
                    lease_owner=None,
                    lease_until=0,
                )
            )
            await session.commit()

    async def _capture(self, archive_id, token):  # noqa: C901
        # Capture coordinates immutable scopes, watermarks and native summaries;
        # publication remains a single fenced transaction after all I/O succeeds.
        async with async_session() as session:
            archive = await session.get(BenchmarkMonitoringArchive, archive_id)
            if archive.state == 'expiring':
                await self._delete(archive, token, expired=True)
                return
            benchmark = await session.get(Benchmark, archive.benchmark_id)
            if (
                archive.delete_requested
                or benchmark is None
                or benchmark.deleted_at is not None
            ):
                await self._delete(archive, token)
                return
            if execution_run_id(benchmark) != archive.run_id:
                # The previous run uses only its durable checkpoint, including
                # phase bounds; no results from the replacement run are queried.
                benchmark = SimpleNamespace(
                    id=archive.benchmark_id,
                    state=archive.snapshot.get('state', 'error'),
                    updated_at=None,
                )
            results = [
                SimpleNamespace(**p) for p in archive.snapshot.get('results', [])
            ]
            previous = (
                await session.exec(
                    select(BenchmarkMonitoringRevision).where(
                        BenchmarkMonitoringRevision.archive_id == archive_id,
                        BenchmarkMonitoringRevision.revision == archive.revision,
                    )
                )
            ).first()
        store = get_artifact_store(self.config, profile_id=archive.profile_id)
        if store.profile_fingerprint != archive.profile_fingerprint:
            raise ArchiveCaptureError('storage_profile_changed')
        query_version = sha256(
            json.dumps(
                get_query_config().model_dump(mode='json'), sort_keys=True
            ).encode()
        ).hexdigest()
        if archive.plan.get('query_version', query_version) != query_version:
            raise ArchiveCaptureError('monitoring_query_configuration_changed')
        now = time.time()
        terminal = _value(benchmark.state) in _TERMINAL
        observed_at = archive.snapshot.get('observed_at', 0)
        terminal = terminal and now - observed_at >= self.settle
        history = frozen_history(
            benchmark, archive.snapshot, results, now=now - self.settle
        )
        scopes = {}
        if previous:
            async with async_session() as session:
                ids = [
                    value
                    for value in previous.scopes.values()
                    if isinstance(value, str)
                ]
                rows = (
                    (
                        await session.exec(
                            select(BenchmarkMonitoringScope).where(
                                BenchmarkMonitoringScope.id.in_(ids)
                            )
                        )
                    ).all()
                    if ids
                    else []
                )
                stored_scopes = {stored.id: stored for stored in rows}
                for key, scope_id in previous.scopes.items():
                    if isinstance(scope_id, dict):
                        scopes[key] = deepcopy(scope_id)
                    else:
                        stored = stored_scopes.get(scope_id)
                        if stored is None:
                            raise ArchiveCaptureError('archive_summary_missing')
                        scopes[key] = {**deepcopy(stored.data), '_scope_id': scope_id}
        window = history.context.run_window
        previous_start = archive.plan.get('scope_start')
        if previous_start is None and previous and previous.context.get('run_window'):
            previous_start = previous.context['run_window']['start']
        reset_run = window is not None and (
            (previous_start is not None and window.start != previous_start)
            or (
                archive.captured_until is not None
                and window.end < archive.captured_until
            )
        )
        if window is not None:
            archive.plan = {**archive.plan, 'scope_start': window.start}
        if reset_run:
            # Measured bounds can exclude launch or completion tails. Replace
            # provisional coverage while published revisions retain their objects.
            scopes = {
                key: entry
                for key, entry in scopes.items()
                if not key.startswith('run/')
            }
        await self._upgrade_scope_flags(archive, scopes)
        chunks = []
        partial = False
        chunks.extend(
            await self._repair_run_scopes(archive, store, history, scopes, terminal)
        )
        # Capture each completed phase once. Result replacement is detectable and
        # produces a new immutable scope while pinned revisions keep the old one.
        for phase in history.context.stages:
            fingerprint = sha256(phase.model_dump_json().encode()).hexdigest()
            key = f'phase:{phase.id}'
            if phase.end is not None and phase.end > now - self.settle:
                continue
            if scopes.get(f'{key}/runtime/all/aggregate', {}).get(
                'phase_fingerprint'
            ) == fingerprint and not any(
                entry.get('partial', False)
                for name, entry in scopes.items()
                if name.startswith(key + '/')
            ):
                continue
            outputs = await self._collect_scope(history, key, phase.id, archive.plan)
            new, is_partial = await self._store_outputs(
                archive, store, outputs, fingerprint=fingerprint
            )
            chunks.extend(outputs_chunk for outputs_chunk in new.pop('_chunks', []))
            scopes.update(new)
            partial |= is_partial
        captured_until = None if reset_run else archive.captured_until
        if window is not None and window.end > window.start:
            end = window.end if terminal else min(window.end, now - self.settle)
            # The scope's first timestamp anchors every subsequent evaluation grid.
            anchor = archive.plan.get('anchor', window.start)
            archive.plan = {**archive.plan, 'anchor': anchor}
            cursor = captured_until if captured_until is not None else window.start
            while Decimal(str(end)) - Decimal(str(cursor)) >= Decimal('.001'):
                block_end = min(end, cursor + archive.plan['block_seconds'])
                # A partial final block is not reopened: the next object starts at
                # its exact watermark and aligns back to the frozen global grid.
                outputs = await self._collect_scope(
                    history, 'run', None, archive.plan, start=cursor, end=block_end
                )
                new, is_partial = await self._store_outputs(archive, store, outputs)
                chunks.extend(new.pop('_chunks', []))
                for key, entry in new.items():
                    old = scopes.get(key)
                    if old:
                        entry['objects'] = old['objects'] + entry['objects']
                        entry['block_partial'] |= old.get(
                            'block_partial', old.get('partial', False)
                        )
                        entry['partial'] = entry['block_partial']
                        entry['block_data_partial'] |= old.get(
                            'block_data_partial', old.get('data_partial', False)
                        )
                        entry['data_partial'] = entry['block_data_partial']
                        entry['metadata']['sample_statistics'] = _combine_samples(
                            old['metadata']['sample_statistics'],
                            entry['metadata']['sample_statistics'],
                        )
                    entry['metadata']['start'] = window.start
                    entry['metadata']['end'] = block_end
                    _sample_summaries(entry['metadata'])
                    scopes[key] = entry
                partial |= is_partial
                cursor = block_end
            if cursor < end:
                # Prometheus has millisecond evaluation precision; a shorter
                # tail contains no additional scheduled grid tick.
                cursor = end
            captured_until = cursor
            if captured_until is not None and captured_until > window.start:
                # Native full-interval queries supply summaries. Block summaries
                # and per-block quantiles are never combined into run summaries.
                outputs = await self._collect_scope(
                    history,
                    'run',
                    None,
                    archive.plan,
                    start=window.start,
                    end=captured_until,
                    summary_only=True,
                )
                for key, response in outputs.items():
                    entry = scopes.setdefault(key, {'objects': []})
                    if entry.get('metadata_scope') == 'native_full_scope':
                        _retain_successful_metrics(response, entry.get('metadata', {}))
                    metadata = _metadata(response)
                    metadata['sample_statistics'] = entry.get('metadata', {}).get(
                        'sample_statistics', {}
                    )
                    _sample_summaries(metadata)
                    entry['metadata'] = metadata
                    entry['metadata_scope'] = 'native_full_scope'
                    entry.pop('_scope_id', None)
                    native_partial = _capture_incomplete(response)
                    entry['native_partial'] = native_partial
                    entry['native_data_partial'] = _data_partial(response)
                    entry['data_partial'] = (
                        entry.get(
                            'block_data_partial', entry.get('data_partial', False)
                        )
                        or entry['native_data_partial']
                    )
                    entry['partial'] = (
                        entry.get('block_partial', entry.get('partial', False))
                        or native_partial
                    )
                    partial |= native_partial
                if not terminal:
                    history.context.run_window.end = captured_until
        if len(scopes) > _MAX_SCOPES:
            raise ArchiveCaptureError('archive_scope_budget_exceeded')
        partial |= any(entry.get('partial', False) for entry in scopes.values())
        if terminal and not scopes:
            state = 'no_data'
        elif terminal:
            state = (
                'partial'
                if partial
                or any(e.kind == 'generation_changed' for e in history.context.events)
                else 'ready'
            )
        else:
            state = 'capturing'
        # Missing stage bounds remain explicit, even if other phases are saved.
        if terminal and any(s.status != 'available' for s in history.context.stages):
            state = 'partial' if scopes else 'no_data'
        await self._publish(
            archive, token, history.context, scopes, chunks, captured_until, state
        )

    async def _collect_scope(
        self,
        history,
        scope,
        phase_id,
        plan,
        *,
        start=None,
        end=None,
        summary_only=False,
    ):
        window = (
            next((s for s in history.context.stages if s.id == phase_id), None)
            if phase_id is not None
            else history.context.run_window
        )
        start = start if start is not None else getattr(window, 'start', None)
        end = end if end is not None else getattr(window, 'end', None)
        query_interval = None
        if start is not None and end is not None:
            anchor = plan.get('anchor', start) if phase_id is None else start
            anchor = float(
                (Decimal(str(anchor)) * 1000).to_integral_value(rounding=ROUND_CEILING)
                / 1000
            )
            first = (
                anchor
                + math.ceil(round((start - anchor) / plan['step'], 9)) * plan['step']
            )
            last = end - 0.001
            query_interval = MonitoringInterval(
                first, max(first, last), max(1, math.ceil(end - start))
            )
        roles = ['all', 'prefill', 'decode', 'router']
        outputs = {}
        for role in roles:
            for view in ('aggregate', 'instances'):
                outputs[f'{scope}/runtime/{role}/{view}'] = (
                    await collectors.collect_benchmark_model_metrics(
                        history,
                        phase_id=phase_id,
                        start=start,
                        end=end,
                        role=role,
                        view=view,
                        statistic='avg,p50,p95,p99',
                        section='all',
                        query_step=plan['step'],
                        query_interval=query_interval,
                        rate_window_seconds=plan['rate_window'],
                        summary_only=summary_only,
                        source_not_before=(
                            start
                            if phase_id is not None
                            else plan.get('scope_start', plan.get('anchor', start))
                        ),
                    )
                )
        if not summary_only:
            outputs[f'{scope}/resources/all/aggregate'] = (
                await collectors.collect_benchmark_resource_metrics(
                    history,
                    phase_id=phase_id,
                    start=start,
                    end=end,
                    query_step=plan['step'],
                    query_interval=query_interval,
                    source_not_before=(
                        start
                        if phase_id is not None
                        else plan.get('scope_start', plan.get('anchor', start))
                    ),
                )
            )
        cutoff = min(
            (e.at for e in history.context.events if e.kind == 'generation_changed'),
            default=None,
        )
        if cutoff is not None:
            for response in outputs.values():
                self._isolate_generation(response, cutoff)
        for key, response in outputs.items():
            role = key.split('/')[-2]
            if role != 'all' and not any(i.role == role for i in history.instances):
                object.__setattr__(response, '_archive_empty_population', True)
            response.step = plan['step']
            if hasattr(response, 'rate_window_seconds'):
                response.rate_window_seconds = plan['rate_window']
            if query_interval is not None and not summary_only:
                expected = max(
                    0,
                    math.floor(
                        round((end - 0.001 - query_interval.start) / plan['step'], 9)
                    )
                    + 1,
                )
                object.__setattr__(response, '_archive_expected_samples', expected)
                for samples in response.sample_statistics.values():
                    for sample in samples:
                        sample.expected_samples = expected
                        sample.coverage = (
                            min(1, sample.observed_samples / expected)
                            if expected
                            else 1
                        )
        return outputs

    async def _upgrade_scope_flags(self, archive, scopes):
        """Classify existing publications without rewriting their pinned metadata."""
        for key, entry in scopes.items():
            if 'data_partial' in entry:
                continue
            response_type = (
                BenchmarkModelMetrics
                if '/runtime/' in key
                else BenchmarkResourceMetrics
            )
            response = response_type.model_validate(entry['metadata'])
            role = key.split('/')[-2]
            members = archive.snapshot.get('monitoring', {}).get('instances', [])
            if (
                '/runtime/' in key
                and role != 'all'
                and not any(member.get('role') == role for member in members)
            ):
                object.__setattr__(response, '_archive_empty_population', True)
            block_incomplete = _capture_incomplete(response)
            if key.startswith('run/') and entry.get('objects'):
                async with async_session() as session:
                    chunks = (
                        await session.exec(
                            select(BenchmarkMonitoringChunk).where(
                                BenchmarkMonitoringChunk.archive_id == archive.id,
                                BenchmarkMonitoringChunk.id.in_(entry['objects']),
                                BenchmarkMonitoringChunk.published.is_(True),
                            )
                        )
                    ).all()
                if len(chunks) != len(entry['objects']):
                    raise ArchiveCaptureError('archive_object_index_incomplete')
                block_incomplete = False
                for chunk in chunks:
                    captured = response_type.model_validate(await _object(chunk))
                    block_incomplete |= _capture_incomplete(captured)
            entry['data_partial'] = entry.get('partial', False) or _data_partial(
                response
            )
            entry['block_data_partial'] = entry.get(
                'block_partial', entry['data_partial']
            )
            entry['native_data_partial'] = entry.get('native_partial', False)
            entry['block_partial'] = block_incomplete
            entry['native_partial'] = entry.get(
                'metadata_scope'
            ) == 'native_full_scope' and _capture_incomplete(response)
            entry['partial'] = entry['block_partial'] or entry['native_partial']
            entry.pop('_scope_id', None)

    async def _repair_run_scopes(self, archive, store, history, scopes, terminal):
        """Retry exact full scopes without replacing durable data with source gaps."""
        if (
            not terminal
            or not archive.plan.get('repair_scopes')
            or history.context.run_window is None
        ):
            return []
        archive.plan = {**archive.plan, 'repair_scopes': False}
        keys = {
            key
            for key, entry in scopes.items()
            if key.startswith('run/') and entry.get('partial', False)
        }
        if not keys:
            return []
        outputs = await self._collect_scope(history, 'run', None, archive.plan)
        recovered = {}
        for key, response in outputs.items():
            if key not in keys or _capture_incomplete(response):
                continue
            if _data_partial(response):
                async with async_session() as session:
                    before = await _read_entry(session, archive, scopes[key])
                if not _retains_saved_points(before, response):
                    continue
            recovered[key] = response
        if not recovered:
            return []
        entries, _ = await self._store_outputs(archive, store, recovered)
        chunks = entries.pop('_chunks', [])
        for entry in entries.values():
            entry['metadata_scope'] = 'native_full_scope'
        scopes.update(entries)
        return chunks

    @staticmethod
    def _isolate_generation(response, cutoff):
        if response.end is None or response.end <= cutoff:
            return
        for metric in response.metrics.values():
            for line in metric.series:
                line.points = [(t, v if t < cutoff else None) for t, v in line.points]
                if line.lower_bounds:
                    line.lower_bounds = [
                        (t, v) for t, v in line.lower_bounds if t < cutoff
                    ]
            for summary in metric.summaries:
                summary.value, summary.lower_bound, summary.sample_count = (
                    None,
                    None,
                    None,
                )
            if metric.status not in ('unsupported', 'not_applicable', 'query_error'):
                metric.reason = 'historical_association_changed'
                metric.status = 'no_data'
        collectors._statistics(response)

    async def _store_outputs(self, archive, store, outputs, *, fingerprint=None):
        entries, chunks, partial = {}, [], False
        # Bootstrap legacy objects before adding a new upload intent. The
        # shared ledger must distinguish existing bytes from this reservation.
        await CapacityManager(self.config, session_factory=async_session).usage()
        async with async_session() as session:
            used = (
                await session.exec(
                    select(
                        func.sum(BenchmarkMonitoringChunk.uncompressed_size_bytes)
                    ).where(
                        BenchmarkMonitoringChunk.archive_id == archive.id,
                        ~BenchmarkMonitoringChunk.scope.like('manifest:%'),
                    )
                )
            ).one() or 0
        for key, response in outputs.items():
            data = response.model_dump(mode='json')
            raw = await asyncio.to_thread(
                json.dumps,
                data,
                ensure_ascii=False,
                sort_keys=True,
                separators=(',', ':'),
                allow_nan=False,
            )
            size = len(raw.encode())
            if used + size > self.max_bytes:
                raise ArchiveCaptureError('archive_byte_budget_exceeded')
            chunk_id = str(uuid4())
            object_key = (
                f'installations/{archive.plan["installation_id"]}/'
                f'owners/{archive.owner_principal_id or "global"}/'
                f'benchmarks/{archive.benchmark_id}/runs/{archive.run_id}/'
                f'monitoring/v1/objects/{_object_filename(key, chunk_id)}'
            )
            chunk = BenchmarkMonitoringChunk(
                id=chunk_id,
                archive_id=archive.id,
                scope=key,
                profile_id=store.profile_id,
                object_key=object_key,
                sha256=sha256(raw.encode()).hexdigest(),
                size_bytes=0,
                uncompressed_size_bytes=size,
                start=response.start,
                end=response.end,
                created_at=time.time(),
            )
            # Track the intended object before uploading so a process death at
            # any point leaves a durable, installation-scoped cleanup reference.
            async with async_session() as session:
                session.add(chunk)
                await session.commit()
            try:
                metadata = await put_managed_json(
                    store,
                    object_key,
                    data,
                    config=self.config,
                    archive_id=archive.id,
                    session_factory=async_session,
                )
            except ArtifactStorageError as exc:
                if exc.reason == 'storage_capacity_exceeded':
                    # Reservation rejection occurs before any S3 attempt. This
                    # intent cannot hold an object or consume the run budget.
                    async with async_session() as session:
                        await session.execute(
                            delete(BenchmarkMonitoringChunk).where(
                                BenchmarkMonitoringChunk.id == chunk_id,
                                BenchmarkMonitoringChunk.published.is_(False),
                            )
                        )
                        await session.commit()
                    await self._clean_capacity_attempt(archive)
                raise
            async with async_session() as session:
                await session.execute(
                    update(BenchmarkMonitoringChunk)
                    .where(BenchmarkMonitoringChunk.id == chunk_id)
                    .values(size_bytes=metadata.size_bytes)
                )
                await session.commit()
            used += size
            chunks.append(chunk)
            entries[key] = {
                'metadata': _metadata(response),
                'metadata_scope': (
                    'phase_scope' if key.startswith('phase:') else 'block'
                ),
                'objects': [chunk_id],
                'partial': _capture_incomplete(response),
                'block_partial': _capture_incomplete(response),
                'data_partial': _data_partial(response),
                'block_data_partial': _data_partial(response),
            }
            if fingerprint is not None:
                entries[key]['phase_fingerprint'] = fingerprint
            partial |= _capture_incomplete(response)
        entries['_chunks'] = chunks
        return entries, partial

    async def _publish(self, archive, token, context, scopes, chunks, until, state):
        async with async_session() as session:
            # CAS also serializes publication against cleanup/retry and a newer
            # lease. An uploaded object alone can never become visible.
            result = await session.execute(
                update(BenchmarkMonitoringArchive)
                .where(
                    *self._fence(archive.id, token),
                    BenchmarkMonitoringArchive.revision == archive.revision,
                    BenchmarkMonitoringArchive.checkpoint == archive.checkpoint,
                    BenchmarkMonitoringArchive.delete_requested.is_(False),
                )
                .values(
                    revision=archive.revision + 1,
                    state=state,
                    captured_until=until,
                    plan=archive.plan,
                    last_error=None,
                    attempts=0,
                    lease_owner=None,
                    lease_until=0,
                    retry_at=time.time() + self.flush_interval,
                )
            )
            if result.rowcount != 1:
                await session.rollback()
                raise ArchiveLeaseLost()
            index = {}
            for key, entry in scopes.items():
                scope_id = entry.pop('_scope_id', None)
                if scope_id is None:
                    scope_id = str(uuid4())
                    session.add(
                        BenchmarkMonitoringScope(
                            id=scope_id, archive_id=archive.id, data=entry
                        )
                    )
                index[key] = scope_id
            for chunk in chunks:
                await session.execute(
                    update(BenchmarkMonitoringChunk)
                    .where(BenchmarkMonitoringChunk.id == chunk.id)
                    .values(published=True)
                )
            session.add(
                BenchmarkMonitoringRevision(
                    id=str(uuid4()),
                    archive_id=archive.id,
                    revision=archive.revision + 1,
                    state=state,
                    captured_until=until,
                    context=context.model_dump(mode='json'),
                    scopes=index,
                    data_coverage=(
                        'none'
                        if state == 'no_data'
                        else (
                            'partial'
                            if any(
                                e.get('data_partial', e.get('partial', False))
                                for e in scopes.values()
                            )
                            else 'complete'
                        )
                    ),
                    created_at=time.time(),
                )
            )
            await session.commit()

    async def _seal_previous(self, archive, token):
        async with async_session() as session:
            await session.execute(
                update(BenchmarkMonitoringArchive)
                .where(*self._fence(archive.id, token))
                .values(
                    state='partial' if archive.revision else 'no_data',
                    lease_owner=None,
                    lease_until=0,
                    last_error='execution_replaced',
                )
            )
            await session.commit()

    async def _delete(self, archive, token, *, expired=False):
        async with async_session() as session:
            chunks = (
                await session.exec(
                    select(BenchmarkMonitoringChunk).where(
                        BenchmarkMonitoringChunk.archive_id == archive.id
                    )
                )
            ).all()
        for chunk in chunks:
            store = get_artifact_store(self.config, profile_id=chunk.profile_id)
            if (
                chunk.profile_id == archive.profile_id
                and store.profile_fingerprint != archive.profile_fingerprint
            ):
                raise ArchiveCaptureError('storage_profile_changed')
            await delete_managed_object(
                store,
                chunk.object_key,
                config=self.config,
                session_factory=async_session,
            )
        from gpustack.artifacts.maintenance import cleanup_archive_copies

        await cleanup_archive_copies(
            archive, self.config, session_factory=async_session
        )
        async with async_session() as session:
            statement = (
                update(BenchmarkMonitoringArchive).values(
                    state='expired', lease_owner=None, lease_until=0
                )
                if expired
                else delete(BenchmarkMonitoringArchive)
            )
            result = await session.execute(
                statement.where(*self._fence(archive.id, token))
            )
            if result.rowcount != 1:
                await session.rollback()
                raise ArchiveLeaseLost()
            if not expired:
                await session.execute(
                    delete(BenchmarkMonitoringRevision).where(
                        BenchmarkMonitoringRevision.archive_id == archive.id
                    )
                )
                await session.execute(
                    delete(BenchmarkMonitoringScope).where(
                        BenchmarkMonitoringScope.archive_id == archive.id
                    )
                )
            await session.execute(
                delete(BenchmarkMonitoringChunk).where(
                    BenchmarkMonitoringChunk.archive_id == archive.id
                )
            )
            await session.commit()

    async def _expire(self):
        days = getattr(self.config, 'benchmark_archive_retention_days', None)
        if days is None:
            return
        async with async_session() as session:
            rows = (
                await session.exec(
                    select(BenchmarkMonitoringArchive)
                    .join(
                        BenchmarkMonitoringRevision,
                        (
                            BenchmarkMonitoringRevision.archive_id
                            == BenchmarkMonitoringArchive.id
                        )
                        & (
                            BenchmarkMonitoringRevision.revision
                            == BenchmarkMonitoringArchive.revision
                        ),
                    )
                    .where(
                        BenchmarkMonitoringArchive.state.in_(
                            ['ready', 'partial', 'no_data']
                        ),
                        BenchmarkMonitoringRevision.created_at
                        < time.time() - days * 86400,
                    )
                    .limit(100)
                )
            ).all()
            for archive in rows:
                archive.state, archive.retry_at = 'expiring', 0
                session.add(archive)
            await session.commit()

    async def _clean_orphans(self):
        async with async_session() as session:
            chunks = (
                await session.exec(
                    select(BenchmarkMonitoringChunk)
                    .join(
                        BenchmarkMonitoringArchive,
                        BenchmarkMonitoringArchive.id
                        == BenchmarkMonitoringChunk.archive_id,
                    )
                    .where(
                        BenchmarkMonitoringChunk.published.is_(False),
                        ~BenchmarkMonitoringChunk.scope.like('manifest:%'),
                        BenchmarkMonitoringChunk.created_at < time.time() - 3600,
                        BenchmarkMonitoringArchive.lease_until <= time.time(),
                    )
                    .limit(100)
                )
            ).all()
        for archive_id in dict.fromkeys(chunk.archive_id for chunk in chunks):
            token = await self.claim(archive_id)
            if token is None:
                continue
            heartbeat = asyncio.create_task(self._heartbeat(archive_id, token))
            try:
                await self._delete_orphans(archive_id, token)
            finally:
                heartbeat.cancel()
                await asyncio.gather(heartbeat, return_exceptions=True)
                async with async_session() as session:
                    await session.exec(
                        update(BenchmarkMonitoringArchive)
                        .where(*self._fence(archive_id, token))
                        .values(lease_owner=None, lease_until=0)
                    )
                    await session.commit()

    async def _delete_orphans(self, archive_id, token):
        # Recheck candidates after acquiring the shared publication lease. A
        # manifest retry may reuse its immutable object key, so GC cannot race it.
        async with async_session() as session:
            chunks = (
                await session.exec(
                    select(BenchmarkMonitoringChunk)
                    .join(
                        BenchmarkMonitoringArchive,
                        BenchmarkMonitoringArchive.id
                        == BenchmarkMonitoringChunk.archive_id,
                    )
                    .where(
                        *self._fence(archive_id, token),
                        BenchmarkMonitoringChunk.published.is_(False),
                        ~BenchmarkMonitoringChunk.scope.like('manifest:%'),
                        BenchmarkMonitoringChunk.created_at < time.time() - 3600,
                    )
                    .limit(100)
                )
            ).all()
        for chunk in chunks:
            try:
                store = get_artifact_store(self.config, profile_id=chunk.profile_id)
                await delete_managed_object(
                    store,
                    chunk.object_key,
                    config=self.config,
                    session_factory=async_session,
                )
            except Exception:
                logger.warning('Benchmark archive orphan cleanup is pending')
                continue
            async with async_session() as session:
                await session.exec(
                    delete(BenchmarkMonitoringChunk).where(
                        BenchmarkMonitoringChunk.id == chunk.id,
                        BenchmarkMonitoringChunk.published.is_(False),
                    )
                )
                await session.commit()

    async def _clean_capacity_attempt(self, archive):
        """Free confirmed unpublished uploads so a capacity retry can progress.

        Unknown uploads retain their intent and reservation for delayed orphan
        cleanup. Published objects are immutable and never evicted for capacity.
        """
        async with async_session() as session:
            chunks = (
                await session.exec(
                    select(BenchmarkMonitoringChunk)
                    .join(
                        BenchmarkMonitoringArchive,
                        BenchmarkMonitoringArchive.id
                        == BenchmarkMonitoringChunk.archive_id,
                    )
                    .where(
                        *self._fence(archive.id, archive.fencing_token),
                        BenchmarkMonitoringChunk.published.is_(False),
                        ~BenchmarkMonitoringChunk.scope.like('manifest:%'),
                        BenchmarkMonitoringChunk.size_bytes > 0,
                    )
                )
            ).all()
        for chunk in chunks:
            try:
                store = get_artifact_store(self.config, profile_id=chunk.profile_id)
                await delete_managed_object(
                    store,
                    chunk.object_key,
                    config=self.config,
                    session_factory=async_session,
                )
                async with async_session() as session:
                    await session.execute(
                        delete(BenchmarkMonitoringChunk).where(
                            BenchmarkMonitoringChunk.id == chunk.id,
                            BenchmarkMonitoringChunk.published.is_(False),
                        )
                    )
                    await session.commit()
            except ArtifactStorageError:
                logger.warning('Benchmark archive capacity cleanup is pending')
