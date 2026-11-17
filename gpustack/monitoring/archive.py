"""Pinned, tenant-authorized replay of immutable benchmark monitoring objects."""

import asyncio
from collections import OrderedDict
from copy import deepcopy
import json
import time
import math
import logging
from hashlib import sha256
from typing import Optional
from uuid import NAMESPACE_URL, uuid5

from sqlmodel import select
from sqlalchemy.exc import IntegrityError

from gpustack.api.exceptions import (
    ConflictException,
    ForbiddenException,
    InvalidException,
    NotFoundException,
    ServiceUnavailableException,
)
from gpustack.api.tenant import assert_resource_visible
from gpustack.artifacts import ArtifactStorageError, get_artifact_store
from gpustack.schemas.benchmark_archive import (
    BenchmarkMonitoringArchive,
    BenchmarkMonitoringChunk,
    BenchmarkMonitoringRevision,
    BenchmarkMonitoringInstallation,
    BenchmarkMonitoringScope,
)
from gpustack.schemas.benchmark_monitoring import (
    BenchmarkArchiveQueryCapabilities,
    BenchmarkModelMetrics,
    BenchmarkMonitoringContext,
    BenchmarkObservationStorage,
    BenchmarkResourceMetrics,
)
from gpustack.schemas.principals import OrgRole, PrincipalType

logger = logging.getLogger(__name__)

MAX_RESPONSE_BYTES = 64 * 1024 * 1024
MAX_RESPONSE_OBJECTS = 512
MAX_PHASE_BATCH = 100
_CACHE_BYTES = 32 * 1024 * 1024
_CACHE = OrderedDict()
_CACHE_SIZE = 0
_INFLIGHT = {}


def execution_run_id(benchmark):
    """Assign old snapshots a deterministic import identity without changing workers."""
    if getattr(benchmark, 'run_id', None):
        return benchmark.run_id
    monitoring = getattr(getattr(benchmark, 'snapshot', None), 'monitoring', None)
    if monitoring is None:
        return None
    return str(
        uuid5(
            NAMESPACE_URL, f'gpustack:benchmark:{benchmark.id}:{monitoring.captured_at}'
        )
    )


def _scheduler_timing(point):
    raw = point.raw_metrics if isinstance(point.raw_metrics, dict) else {}
    values = raw.get('scheduler_metrics', {})
    if not isinstance(values, dict):
        return {}
    return {
        key: value
        for key in (
            'measure_start_time',
            'measure_end_time',
            'request_start_time',
            'request_end_time',
        )
        if isinstance((value := values.get(key)), (int, float))
        and not isinstance(value, bool)
        and math.isfinite(value)
        and value >= 0
    }


def _authorize(ctx, benchmark) -> None:
    if ctx is None or getattr(ctx.user, 'kind', None) == PrincipalType.SYSTEM:
        raise ForbiddenException(
            message='Benchmark monitoring requires a user principal'
        )
    ctx.assert_org_role(OrgRole.OWNER)
    assert_resource_visible(ctx, benchmark, not_found_message='Benchmark not found')


def _hardware_allowed(archive, ctx) -> bool:
    snapshot = archive.snapshot.get('monitoring', {})
    owner = snapshot.get('owner_principal_id')
    if ctx.is_platform_admin and ctx.current_principal_id is None:
        return owner == archive.owner_principal_id
    # Infra shared from another organization is never implicitly granted by a
    # benchmark report. Platform admins may read it in its own owning context.
    return (
        owner is not None
        and owner == archive.owner_principal_id
        and (ctx.is_platform_admin or ctx.current_principal_id == owner)
        and all(
            w.get('owner_principal_id') == owner for w in snapshot.get('workers', [])
        )
    )


async def get_benchmark_archive(
    session,
    benchmark,
    *,
    run_id: Optional[str] = None,
    revision: Optional[int] = None,
    include_unavailable=False,
):
    current_run = execution_run_id(benchmark)
    run_id = run_id or current_run
    if run_id is None:
        if revision is not None:
            raise ConflictException(
                message='Archive revision has no execution identity'
            )
        return None
    result = await session.exec(
        select(BenchmarkMonitoringArchive).where(
            BenchmarkMonitoringArchive.benchmark_id == benchmark.id,
            BenchmarkMonitoringArchive.run_id == run_id,
        )
    )
    archive = result.first()
    if archive is None:
        if run_id != current_run or revision is not None:
            raise NotFoundException(
                message='Benchmark monitoring execution not archived'
            )
        return None
    if archive.owner_principal_id != benchmark.owner_principal_id:
        raise ForbiddenException(message='Benchmark archive ownership does not match')
    if not include_unavailable and (
        archive.delete_requested or archive.state in ('deleting', 'expiring', 'expired')
    ):
        raise ConflictException(
            message='Benchmark monitoring archive is no longer available',
            details={'archive_state': archive.state},
        )
    return archive


async def _publication(session, ctx, benchmark, *, run_id=None, revision=None):
    _authorize(ctx, benchmark)
    archive = await get_benchmark_archive(
        session, benchmark, run_id=run_id, revision=revision
    )
    if archive is None:
        return None
    number = archive.revision if revision is None else revision
    if number < 1:
        if revision is not None:
            raise ConflictException(
                message='Archive revision is not available; reload context'
            )
        return None
    result = await session.exec(
        select(BenchmarkMonitoringRevision).where(
            BenchmarkMonitoringRevision.archive_id == archive.id,
            BenchmarkMonitoringRevision.revision == number,
        )
    )
    publication = result.first()
    if publication is None:
        raise ConflictException(
            message='Archive revision is not available; reload context'
        )
    return archive, publication


def _decorate(response, archive, publication):
    response.observation_storage = BenchmarkObservationStorage(
        run_id=archive.run_id,
        archive_revision=publication.revision,
        archive_state=archive.state,
        last_error=archive.last_error,
        capture_complete=publication.state in ('ready', 'no_data'),
        data_coverage=publication.data_coverage,
        captured_until=publication.captured_until,
    )
    response.query_capabilities = BenchmarkArchiveQueryCapabilities()
    return response


def _restrict_context(context):
    context.workers, context.devices = [], []
    context.resource_status = 'restricted'
    context.resource_reason = 'resource_not_visible'
    for member in context.instances:
        member.worker_id, member.worker_ids, member.gpu_indexes = None, [], []
    return context


async def read_archive_context(session, ctx, benchmark, *, run_id=None, revision=None):
    selected = await _publication(
        session, ctx, benchmark, run_id=run_id, revision=revision
    )
    if selected is None:
        return None
    archive, publication = selected
    context = BenchmarkMonitoringContext.model_validate(publication.context)
    if not _hardware_allowed(archive, ctx):
        _restrict_context(context)
    context.capabilities.supports_custom_range = False
    return _decorate(context, archive, publication)


def _scope(publication, phase_id, start, end):
    context = publication.context
    if phase_id is not None:
        window = next(
            (s for s in context.get('stages', []) if s['id'] == phase_id), None
        )
        if window is None:
            raise NotFoundException(message='Benchmark phase not found')
    else:
        window = context.get('run_window')
    if (start is None) != (end is None):
        raise InvalidException(message='Both start and end are required')
    if start is not None and (
        window is None or start != window.get('start') or end != window.get('end')
    ):
        raise InvalidException(
            message='Archived summaries support only the complete run or a recorded phase',
            details={
                'supported_scopes': ['run', 'phase'],
                'custom_range_summary': False,
            },
        )
    return f'phase:{phase_id}' if phase_id is not None else 'run'


async def _entry(session, publication, scope, kind, role='all', view='aggregate'):
    key = f'{scope}/{kind}/{role}/{view}'
    entry = publication.scopes.get(key)
    if entry is None:
        raise ConflictException(
            message='This monitoring scope has not been archived',
            details={'scope': scope, 'archive_revision': publication.revision},
        )
    if isinstance(entry, dict):
        return entry
    stored = await session.get(BenchmarkMonitoringScope, entry)
    if stored is None or stored.archive_id != publication.archive_id:
        raise ServiceUnavailableException(message='Archive summary index is incomplete')
    return stored.data


async def _object(chunk):
    global _CACHE_SIZE
    key = (chunk.profile_id, chunk.object_key, chunk.sha256)
    if key in _CACHE:
        _CACHE.move_to_end(key)
        return deepcopy(_CACHE[key][0])
    if key not in _INFLIGHT:

        async def fetch():
            store = get_artifact_store(profile_id=chunk.profile_id)
            if store is None:
                raise ServiceUnavailableException(
                    message='Archive storage is unavailable'
                )
            return await store.get_json(chunk.object_key, expected_sha256=chunk.sha256)

        task = asyncio.create_task(fetch())
        _INFLIGHT[key] = task

        def completed(done):
            if _INFLIGHT.get(key) is done:
                _INFLIGHT.pop(key, None)
            if not done.cancelled():
                done.exception()

        task.add_done_callback(completed)
    task = _INFLIGHT[key]
    try:
        payload = await asyncio.shield(task)
    finally:
        if _INFLIGHT.get(key) is task and task.done():
            _INFLIGHT.pop(key, None)
    size = len(json.dumps(payload, separators=(',', ':')).encode())
    if size > MAX_RESPONSE_BYTES:
        raise ServiceUnavailableException(
            message='Archive object exceeds replay budget'
        )
    if size <= _CACHE_BYTES and key not in _CACHE:
        while _CACHE and _CACHE_SIZE + size > _CACHE_BYTES:
            _, (_, old_size) = _CACHE.popitem(last=False)
            _CACHE_SIZE -= old_size
        _CACHE[key] = (payload, size)
        _CACHE_SIZE += size
    return deepcopy(payload)


def _merge(metadata, payloads):
    data = deepcopy(metadata)
    identities = {}
    for payload in payloads:
        for key, metric in payload.get('metrics', {}).items():
            target = data['metrics'].setdefault(key, deepcopy(metric))
            target.setdefault('series', [])
            for line in metric.get('series', []):
                identity = (
                    key,
                    line['statistic'],
                    json.dumps(line.get('dimensions', {}), sort_keys=True),
                )
                if identity not in identities:
                    result = deepcopy(line)
                    result['points'] = []
                    result.pop('lower_bounds', None)
                    identities[identity] = result
                    target['series'].append(result)
                result = identities[identity]
                result['points'].extend(line['points'])
                if line.get('lower_bounds'):
                    result.setdefault('lower_bounds', []).extend(line['lower_bounds'])
    for line in identities.values():
        start, end = data.get('start'), data.get('end')
        line['points'] = sorted(
            (t, v)
            for t, v in dict(line['points']).items()
            if (start is None or t >= start) and (end is None or t < end)
        )
        if line.get('lower_bounds'):
            times = {t for t, v in line['points'] if v is None}
            line['lower_bounds'] = sorted(
                (t, v) for t, v in dict(line['lower_bounds']).items() if t in times
            )
    return data


async def _read_entry(session, archive, entry):
    ids = entry.get('objects', [])
    if len(ids) > MAX_RESPONSE_OBJECTS:
        raise InvalidException(message='Archive scope exceeds replay object budget')
    chunks = []
    if ids:
        result = await session.exec(
            select(BenchmarkMonitoringChunk).where(
                BenchmarkMonitoringChunk.archive_id == archive.id,
                BenchmarkMonitoringChunk.id.in_(ids),
                BenchmarkMonitoringChunk.published == True,  # noqa: E712
            )
        )
        chunks = result.all()
    if len(chunks) != len(ids):
        raise ServiceUnavailableException(message='Archive object index is incomplete')
    if sum(chunk.uncompressed_size_bytes for chunk in chunks) > MAX_RESPONSE_BYTES:
        raise InvalidException(message='Archive scope exceeds replay byte budget')
    try:
        store = get_artifact_store(profile_id=archive.profile_id)
        if store is None or store.profile_fingerprint != archive.profile_fingerprint:
            raise ArtifactStorageError('profile_binding_mismatch')
        # Four independent reads at a time bound remote I/O and decompressed memory.
        payloads = []
        for offset in range(0, len(chunks), 4):
            payloads.extend(
                await asyncio.gather(*(_object(c) for c in chunks[offset : offset + 4]))
            )
        return _merge(entry['metadata'], payloads)
    except ArtifactStorageError as exc:
        raise ServiceUnavailableException(
            message='Benchmark monitoring archive is temporarily unreadable',
            details={'reason': getattr(exc, 'reason', 'storage_unavailable')},
        ) from exc


def _filter_metrics(
    response, statistic, section, *, hide_hardware=False, sections=None
):
    from gpustack.monitoring.catalog import model_metric_definitions

    statistics = set(statistic.split(','))
    if not statistics or not statistics <= {'avg', 'p50', 'p95', 'p99'}:
        raise InvalidException(message='Unsupported latency statistic')
    if section not in ('all', 'llm', 'pd'):
        raise InvalidException(message='Unsupported monitoring section')
    if section != 'all':
        sections = sections or {m.key: m.section for m in model_metric_definitions('')}
        allowed = {key for key, value in sections.items() if value == section}
        response.metrics = {k: v for k, v in response.metrics.items() if k in allowed}
        response.sample_statistics = {
            k: v for k, v in response.sample_statistics.items() if k in allowed
        }
    for metric in response.metrics.values():
        metric.series = [
            line
            for line in metric.series
            if line.statistic in statistics or line.statistic in ('current', 'rate')
        ]
        if hide_hardware:
            for observation in list(metric.series) + list(metric.summaries):
                observation.dimensions.worker_id = None
                observation.dimensions.source_name = None
                observation.dimensions.device_id = None
                if hasattr(observation, 'name'):
                    observation.name = (
                        ' · '.join(
                            str(v)
                            for v in (
                                observation.dimensions.role,
                                observation.dimensions.model_instance_name,
                            )
                            if v
                        )
                        or 'Deployment'
                    )
    if hide_hardware:
        for samples in response.sample_statistics.values():
            for sample in samples:
                sample.dimensions.worker_id = None
                sample.dimensions.source_name = None
                sample.dimensions.device_id = None
    return response


async def read_archive_metrics(
    session,
    ctx,
    benchmark,
    *,
    phase_id=None,
    start=None,
    end=None,
    instance_ids=None,
    role='all',
    view='aggregate',
    statistic='avg,p50,p95,p99',
    section='all',
    run_id=None,
    revision=None,
):
    selected = await _publication(
        session, ctx, benchmark, run_id=run_id, revision=revision
    )
    if selected is None:
        return None
    archive, publication = selected
    if instance_ids is not None:
        raise InvalidException(
            message='Archived metrics do not support arbitrary instance subsets',
            details={'arbitrary_subset_summary': False},
        )
    if role not in ('all', 'prefill', 'decode', 'router') or view not in (
        'aggregate',
        'instances',
    ):
        raise InvalidException(message='Unsupported archive population')
    scope = _scope(publication, phase_id, start, end)
    entry = await _entry(session, publication, scope, 'runtime', role, view)
    response = BenchmarkModelMetrics.model_validate(
        await _read_entry(session, archive, entry)
    )
    _filter_metrics(
        response,
        statistic,
        section,
        hide_hardware=not _hardware_allowed(archive, ctx),
        sections=archive.plan.get('sections'),
    )
    return _decorate(response, archive, publication)


async def read_archive_resources(
    session,
    ctx,
    benchmark,
    *,
    phase_id=None,
    start=None,
    end=None,
    run_id=None,
    revision=None,
):
    selected = await _publication(
        session, ctx, benchmark, run_id=run_id, revision=revision
    )
    if selected is None:
        return None
    archive, publication = selected
    if not _hardware_allowed(archive, ctx):
        raise ForbiddenException(
            message='Historical hardware is not visible in this organization'
        )
    scope = _scope(publication, phase_id, start, end)
    entry = await _entry(session, publication, scope, 'resources')
    response = BenchmarkResourceMetrics.model_validate(
        await _read_entry(session, archive, entry)
    )
    return _decorate(response, archive, publication)


async def read_archive_phase_observations(
    session, ctx, benchmark, *, phase_ids=None, run_id=None, revision=None
):
    selected = await _publication(
        session, ctx, benchmark, run_id=run_id, revision=revision
    )
    if selected is None:
        return None
    archive, publication = selected
    ids = phase_ids.split(',') if isinstance(phase_ids, str) else phase_ids
    ids = (
        ids
        if ids is not None
        else [s['id'] for s in publication.context.get('stages', [])]
    )
    if len(ids) > MAX_PHASE_BATCH:
        raise InvalidException(
            message=f'At most {MAX_PHASE_BATCH} phases may be read together'
        )
    phases = {}
    for phase_id in dict.fromkeys(ids):
        scope = _scope(publication, phase_id, None, None)
        runtime = BenchmarkModelMetrics.model_validate(
            (await _entry(session, publication, scope, 'runtime', view='instances'))[
                'metadata'
            ]
        )
        _filter_metrics(
            runtime,
            'avg,p50,p95,p99',
            'all',
            hide_hardware=not _hardware_allowed(archive, ctx),
            sections=archive.plan.get('sections'),
        )
        resources = None
        if _hardware_allowed(archive, ctx):
            resources = BenchmarkResourceMetrics.model_validate(
                (await _entry(session, publication, scope, 'resources'))['metadata']
            )
            _decorate(resources, archive, publication)
        phases[phase_id] = {
            'runtime': _decorate(runtime, archive, publication),
            'resources': resources,
        }
    return {
        'run_id': archive.run_id,
        'archive_revision': publication.revision,
        'phases': phases,
    }


async def archive_status(session, ctx, benchmark, *, run_id=None, revision=None):
    from gpustack.artifacts.manifest import manifest_status

    _authorize(ctx, benchmark)
    archive = await get_benchmark_archive(
        session, benchmark, run_id=run_id, revision=revision, include_unavailable=True
    )
    if archive is None:
        return {
            'state': 'not_archived',
            'run_id': getattr(benchmark, 'run_id', None),
            'archive_revision': None,
        }
    return {
        'state': archive.state,
        'run_id': archive.run_id,
        'archive_revision': archive.revision or None,
        'captured_until': archive.captured_until,
        'last_error': archive.last_error,
        'attempts': archive.attempts,
        'manifest': await manifest_status(session, archive, revision=revision),
        'query_capabilities': BenchmarkArchiveQueryCapabilities(),
    }


async def retry_archive(session, ctx, benchmark, *, run_id=None):
    _authorize(ctx, benchmark)
    archive = await get_benchmark_archive(session, benchmark, run_id=run_id)
    if archive is None:
        if run_id is not None and run_id != execution_run_id(benchmark):
            raise ConflictException(message='This execution cannot be backfilled')
        archive = await ensure_archive_for_benchmark(session, benchmark)
        if archive is None:
            raise ConflictException(
                message='No reliable execution snapshot or storage exists to retry'
            )
        monitoring = archive.snapshot.get('monitoring', {})
        if monitoring.get('confirmed_at') is None:
            raise ConflictException(
                message='Unconfirmed execution cannot be backfilled'
            )
    archive.retry_at, archive.last_error = 0, None
    archive.state = 'capturing'
    archive.plan = {**archive.plan, 'repair_scopes': True}
    archive.checkpoint += 1
    session.add(archive)
    await session.commit()
    await session.refresh(archive)
    return await archive_status(session, ctx, benchmark, run_id=archive.run_id)


async def enqueue_archive_deletion(session, benchmark):
    """Mark cleanup in the caller's benchmark deletion transaction."""
    result = await session.exec(
        select(BenchmarkMonitoringArchive).where(
            BenchmarkMonitoringArchive.benchmark_id == benchmark.id
        )
    )
    for archive in result.all():
        archive.delete_requested, archive.state, archive.retry_at = True, 'deleting', 0
        # Revoke in-flight writers before the benchmark becomes inaccessible.
        archive.fencing_token += 1
        archive.lease_owner, archive.lease_until = None, 0
        session.add(archive)


async def ensure_archive_for_benchmark(session, benchmark, *, results=None):
    """Persist the capture job and phase timing in the caller's write transaction.

    The worker's result rows can subsequently be replaced or removed. Only the
    timing/navigation fields needed by the monitoring collector are retained.
    """
    from uuid import uuid4

    from gpustack.config.config import get_global_config
    from gpustack.monitoring.config import get_query_config
    from gpustack.schemas.benchmark import BenchmarkResult, benchmark_load_mode

    config = get_global_config()
    monitoring = getattr(getattr(benchmark, 'snapshot', None), 'monitoring', None)
    if monitoring is None:
        return None
    run_id = execution_run_id(benchmark)
    found = await session.exec(
        select(BenchmarkMonitoringArchive).where(
            BenchmarkMonitoringArchive.benchmark_id == benchmark.id,
            BenchmarkMonitoringArchive.run_id == run_id,
        )
    )
    archive = found.first()
    if archive is None:
        try:
            store = get_artifact_store(config)
        except ArtifactStorageError as exc:
            logger.warning('Benchmark archive setup is pending (%s)', exc.reason)
            return None
        if store is None:
            return None
    if results is None:
        results = (
            await session.exec(
                select(BenchmarkResult).where(
                    BenchmarkResult.benchmark_id == benchmark.id,
                    BenchmarkResult.deleted_at.is_(None),
                )
            )
        ).all()
    timing = [
        dict(
            input_tokens=p.input_tokens,
            sequence=p.sequence,
            rate=p.rate,
            raw_metrics={'scheduler_metrics': _scheduler_timing(p)},
        )
        for p in results
    ]
    recorded = monitoring.model_dump(mode='json')
    recorded['roles'] = [{'name': role.name} for role in monitoring.roles]
    snapshot = {
        'monitoring': recorded,
        'target_mode': getattr(benchmark.target_mode, 'value', benchmark.target_mode),
        'via_gateway': bool(getattr(benchmark.snapshot, 'gateway_url', None)),
        'load_mode': benchmark_load_mode(benchmark),
        'results': timing,
        'state': getattr(benchmark.state, 'value', benchmark.state),
        'observed_at': time.time(),
    }
    if archive is None:
        policy = get_query_config().time_windows
        installation = await session.get(BenchmarkMonitoringInstallation, 1)
        if installation is None:
            installation = BenchmarkMonitoringInstallation(
                id=1, namespace_id=str(uuid4())
            )
            try:
                async with session.begin_nested():
                    session.add(installation)
                    await session.flush()
            except IntegrityError:
                installation = await session.get(BenchmarkMonitoringInstallation, 1)
        archive = BenchmarkMonitoringArchive(
            id=str(uuid4()),
            benchmark_id=benchmark.id,
            run_id=run_id,
            owner_principal_id=benchmark.owner_principal_id,
            profile_id=store.profile_id,
            profile_fingerprint=store.profile_fingerprint,
            snapshot=snapshot,
            plan={
                'version': 1,
                'step': getattr(config, 'benchmark_archive_sample_step_seconds', 15),
                'rate_window': policy.llm.rate_window(policy.llm.min_step_seconds),
                'block_seconds': 300,
                'installation_id': installation.namespace_id,
                'sections': {
                    key: metric.section
                    for key, metric in get_query_config().metrics.items()
                },
                'query_version': sha256(
                    json.dumps(
                        get_query_config().model_dump(mode='json'), sort_keys=True
                    ).encode()
                ).hexdigest(),
            },
        )
    else:
        before = {k: v for k, v in archive.snapshot.items() if k != 'observed_at'}
        after = {k: v for k, v in snapshot.items() if k != 'observed_at'}
        if before == after:
            return archive
        archive.snapshot = snapshot
        archive.checkpoint += 1
        if archive.state in ('ready', 'partial', 'no_data'):
            archive.state, archive.retry_at = 'capturing', 0
    session.add(archive)
    return archive
