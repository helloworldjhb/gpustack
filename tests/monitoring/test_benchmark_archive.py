"""Archive replay does not depend on mutable resource inventories or Prometheus."""

from copy import deepcopy
from types import SimpleNamespace
from unittest.mock import AsyncMock
import time

import pytest
from sqlalchemy.ext.asyncio import create_async_engine
from sqlmodel.ext.asyncio.session import AsyncSession

from gpustack.api.exceptions import (
    ConflictException,
    ForbiddenException,
    InvalidException,
    NotFoundException,
    ServiceUnavailableException,
)
from gpustack.api.tenant import TenantContext
from gpustack.artifacts import ArtifactStorageError
from gpustack.schemas.artifact_storage import (
    ArtifactStorageCapacity,
    ArtifactStorageObject,
)
from gpustack.monitoring import archive
from gpustack.schemas.benchmark_archive import (
    BenchmarkMonitoringArchive,
    BenchmarkMonitoringChunk,
    BenchmarkMonitoringInstallation,
    BenchmarkMonitoringRevision,
    BenchmarkMonitoringScope,
)
from gpustack.schemas.benchmark_monitoring import BenchmarkModelMetrics
from gpustack.schemas.principals import OrgRole, PrincipalType
from gpustack.server import benchmark_monitoring_archiver as worker


def context(owner=4, role=OrgRole.OWNER, admin=False):
    return TenantContext(
        user=SimpleNamespace(kind=PrincipalType.USER),
        is_platform_admin=admin,
        current_principal_id=owner,
        org_role=role,
    )


def benchmark():
    return SimpleNamespace(id=7, run_id='run-one', owner_principal_id=4)


def payload():
    return dict(
        model_id=9,
        start=1000.25,
        end=1040.75,
        step=15,
        rate_window_seconds=60,
        last_refreshed_at=1041,
        history_scope='benchmark_snapshot',
        available=True,
        phase_id='128:0',
        source='engine',
        sample_statistics={},
        metrics={
            'ttft': dict(
                kind='histogram',
                unit='seconds',
                measurement_scope='engine',
                status='available',
                supported_statistics=['avg', 'p50', 'p95', 'p99'],
                series=[
                    dict(
                        name='instance',
                        statistic='p99',
                        dimensions={'model_instance_id': 12, 'role': 'decode'},
                        points=[[1000.25, 0], [1015.25, None]],
                        lower_bounds=[[1015.25, 10]],
                    )
                ],
                summaries=[
                    dict(
                        statistic='p99',
                        dimensions={'role': 'decode'},
                        value=None,
                        lower_bound=10,
                    )
                ],
                coverage=dict(
                    expected_instances=1, observed_instances=1, partial=False
                ),
            ),
            'errors': dict(
                kind='counter',
                unit='requests/s',
                measurement_scope='engine',
                status='query_error',
                reason='query_failed',
                series=[],
                summaries=[],
            ),
            'unsupported': dict(
                kind='histogram',
                unit='seconds',
                measurement_scope='service_ingress',
                status='not_applicable',
                reason='gateway_bypassed',
                series=[],
                summaries=[],
            ),
        },
    )


def stored_context():
    return dict(
        benchmark_id=7,
        model_id=9,
        available=True,
        capabilities=dict(model_id=9),
        run_window=dict(start=1000.25, end=1040.75),
        stages=[
            dict(
                id='128:0',
                sequence=0,
                input_tokens=128,
                start=1000.25,
                end=1040.75,
                status='available',
                reason=None,
            )
        ],
        instances=[
            dict(id=12, name='captured', worker_id=3, worker_ids=[3], gpu_indexes=[0])
        ],
        workers=[dict(id=3, name='captured-worker')],
        devices=[],
        resource_status='available',
    )


def row():
    return BenchmarkMonitoringArchive(
        id='archive',
        benchmark_id=7,
        run_id='run-one',
        owner_principal_id=4,
        profile_id='primary',
        profile_fingerprint='f',
        revision=1,
        snapshot={
            'monitoring': {
                'owner_principal_id': 4,
                'workers': [{'id': 3, 'owner_principal_id': 4}],
            }
        },
        plan={'installation_id': 'installation'},
        state='ready',
    )


def publication():
    data = payload()
    metadata = deepcopy(data)
    for metric in metadata['metrics'].values():
        metric['series'] = []
    entry = dict(metadata=metadata, objects=['chunk'], partial=True)
    return BenchmarkMonitoringRevision(
        id='revision',
        archive_id='archive',
        revision=1,
        state='ready',
        created_at=time.time(),
        context=stored_context(),
        scopes={
            'phase:128:0/resources/all/aggregate': dict(
                metadata=dict(
                    model_id=9,
                    source='hardware',
                    start=1000.25,
                    end=1040.75,
                    step=15,
                    last_refreshed_at=1041,
                    history_scope='benchmark_snapshot',
                    metrics={},
                    sample_statistics={},
                ),
                objects=[],
            ),
            **{
                f'{scope}/runtime/all/{view}': deepcopy(entry)
                for scope in ('run', 'phase:128:0')
                for view in ('aggregate', 'instances')
            },
        },
    )


async def database():
    engine = create_async_engine('sqlite+aiosqlite:///:memory:')
    tables = [
        m.__table__
        for m in (
            BenchmarkMonitoringArchive,
            BenchmarkMonitoringChunk,
            BenchmarkMonitoringRevision,
            BenchmarkMonitoringInstallation,
            BenchmarkMonitoringScope,
            ArtifactStorageCapacity,
            ArtifactStorageObject,
        )
    ]
    async with engine.begin() as connection:
        await connection.run_sync(
            lambda sync: BenchmarkMonitoringArchive.metadata.create_all(
                sync, tables=tables
            )
        )
    return engine


async def seed(engine):
    async with AsyncSession(engine, expire_on_commit=False) as session:
        session.add(row())
        session.add(publication())
        session.add(
            BenchmarkMonitoringChunk(
                id='chunk',
                archive_id='archive',
                scope='phase:128:0/runtime/all/aggregate',
                profile_id='primary',
                object_key='key',
                sha256='digest',
                size_bytes=100,
                uncompressed_size_bytes=1000,
                created_at=1,
                published=True,
            )
        )
        await session.commit()


@pytest.mark.asyncio
async def test_exact_replay_preserves_zero_null_bounds_and_errors_without_inventory(
    monkeypatch,
):
    engine = await database()
    await seed(engine)
    archive._CACHE.clear()
    archive._INFLIGHT.clear()
    archive._CACHE_SIZE = 0
    store = SimpleNamespace(
        profile_fingerprint='f', get_json=AsyncMock(return_value=payload())
    )
    monkeypatch.setattr(archive, 'get_artifact_store', lambda **kwargs: store)
    async with AsyncSession(engine) as session:
        replay = await archive.read_archive_metrics(
            session, context(), benchmark(), phase_id='128:0', revision=1
        )
    original = BenchmarkModelMetrics.model_validate(payload())
    assert replay.model_dump(
        exclude={'observation_storage', 'query_capabilities'}
    ) == original.model_dump(exclude={'observation_storage', 'query_capabilities'})
    assert replay.observation_storage.capture_complete is True
    assert replay.observation_storage.data_coverage == 'partial'
    store.get_json.assert_awaited_once_with('key', expected_sha256='digest')
    await engine.dispose()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    'kwargs',
    [{'start': 1001, 'end': 1040.75}, {'instance_ids': '12'}, {'role': 'invalid'}],
)
async def test_unsupported_queries_are_explicit_not_silently_full_run(kwargs):
    engine = await database()
    await seed(engine)
    async with AsyncSession(engine) as session:
        with pytest.raises(InvalidException):
            await archive.read_archive_metrics(
                session, context(), benchmark(), **kwargs
            )
    await engine.dispose()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    'ctx,exception',
    [
        (context(owner=5), NotFoundException),
        (context(role=OrgRole.MEMBER), ForbiddenException),
    ],
)
async def test_authorization_is_checked_before_cached_objects(ctx, exception):
    engine = await database()
    await seed(engine)
    async with AsyncSession(engine) as session:
        with pytest.raises(exception):
            await archive.read_archive_context(session, ctx, benchmark())
    await engine.dispose()


@pytest.mark.asyncio
async def test_cross_owner_hardware_hidden_even_for_platform_admin():
    engine = await database()
    await seed(engine)
    async with AsyncSession(engine) as session:
        record = await session.get(BenchmarkMonitoringArchive, 'archive')
        record.snapshot = {
            'monitoring': {
                'owner_principal_id': 4,
                'workers': [{'owner_principal_id': 5}],
            }
        }
        session.add(record)
        await session.commit()
        replay = await archive.read_archive_context(
            session, context(admin=True), benchmark()
        )
        assert replay.workers == []
        assert replay.instances[0].worker_ids == []
        assert replay.resource_status == 'restricted'
        with pytest.raises(ForbiddenException):
            await archive.read_archive_resources(
                session, context(admin=True), benchmark()
            )
    await engine.dispose()


@pytest.mark.asyncio
async def test_batch_summaries_do_not_read_objects(monkeypatch):
    engine = await database()
    await seed(engine)
    monkeypatch.setattr(
        archive, '_object', AsyncMock(side_effect=AssertionError('object read'))
    )
    async with AsyncSession(engine) as session:
        result = await archive.read_archive_phase_observations(
            session, context(), benchmark(), phase_ids=['128:0']
        )
    assert result['phases']['128:0']['runtime'].metrics['ttft'].series == []
    assert (
        result['phases']['128:0']['runtime'].metrics['ttft'].summaries[0].lower_bound
        == 10
    )
    await engine.dispose()


@pytest.mark.asyncio
async def test_missing_revision_and_wrong_run_never_fall_back():
    engine = await database()
    await seed(engine)
    async with AsyncSession(engine) as session:
        with pytest.raises(ConflictException):
            await archive.read_archive_context(
                session, context(), benchmark(), revision=2
            )
        with pytest.raises(NotFoundException):
            await archive.read_archive_context(
                session, context(), benchmark(), run_id='run-two'
            )
    await engine.dispose()


@pytest.mark.asyncio
async def test_storage_failure_is_unavailable_not_no_samples(monkeypatch):
    engine = await database()
    await seed(engine)
    archive._CACHE.clear()
    archive._CACHE_SIZE = 0
    store = SimpleNamespace(
        profile_fingerprint='f',
        get_json=AsyncMock(side_effect=ArtifactStorageError('object_missing')),
    )
    monkeypatch.setattr(archive, 'get_artifact_store', lambda **kwargs: store)
    async with AsyncSession(engine) as session:
        with pytest.raises(ServiceUnavailableException) as error:
            await archive.read_archive_metrics(session, context(), benchmark())
        assert error.value.details['reason'] == 'object_missing'
    await engine.dispose()


@pytest.mark.asyncio
async def test_lease_takeover_fences_old_publication_and_checkpoint_change(monkeypatch):
    engine = await database()
    await seed(engine)
    monkeypatch.setattr(
        worker, 'async_session', lambda: AsyncSession(engine, expire_on_commit=False)
    )
    first = worker.BenchmarkMonitoringArchiver(SimpleNamespace())
    second = worker.BenchmarkMonitoringArchiver(SimpleNamespace())
    token = await first.claim('archive')
    assert token == 1
    assert await second.claim('archive') is None
    async with AsyncSession(engine, expire_on_commit=False) as session:
        old = await session.get(BenchmarkMonitoringArchive, 'archive')
        old.lease_until = 0
        session.add(old)
        await session.commit()
    new_token = await second.claim('archive')
    assert new_token == 2
    with pytest.raises(worker.ArchiveLeaseLost):
        await first._publish(
            old,
            token,
            SimpleNamespace(model_dump=lambda **kwargs: stored_context()),
            {},
            [],
            1000,
            'ready',
        )
    async with AsyncSession(engine, expire_on_commit=False) as session:
        current = await session.get(BenchmarkMonitoringArchive, 'archive')
        stale = deepcopy(current)
        current.checkpoint += 1
        session.add(current)
        await session.commit()
    with pytest.raises(worker.ArchiveLeaseLost):
        await second._publish(
            stale,
            new_token,
            SimpleNamespace(model_dump=lambda **kwargs: stored_context()),
            {},
            [],
            1000,
            'ready',
        )
    await engine.dispose()


def test_generation_cutoff_preserves_valid_prefix_and_invalidates_native_whole_scope():
    response = BenchmarkModelMetrics.model_validate(payload())
    worker.BenchmarkMonitoringArchiver._isolate_generation(response, 1010)
    assert response.metrics['ttft'].series[0].points == [(1000.25, 0), (1015.25, None)]
    assert response.metrics['ttft'].series[0].lower_bounds == []
    assert response.metrics['ttft'].summaries[0].lower_bound is None
    assert response.metrics['ttft'].reason == 'historical_association_changed'


@pytest.mark.asyncio
async def test_summary_only_never_issues_range_queries(monkeypatch):
    from gpustack.monitoring.models import _Collection

    collection = _Collection(
        SimpleNamespace(),
        [],
        None,
        'http://prometheus',
        1000,
        1040,
        15,
        60,
        'aggregate',
        ('p99',),
        summary_only=True,
    )
    monkeypatch.setattr(
        'gpustack.monitoring.models._query_range',
        AsyncMock(side_effect=AssertionError('range read')),
    )
    assert await collection.read('some_query', ranged=True) == []


@pytest.mark.asyncio
async def test_expiration_success_does_not_recapture_and_keeps_status(monkeypatch):
    engine = await database()
    await seed(engine)
    async with AsyncSession(engine) as session:
        record = await session.get(BenchmarkMonitoringArchive, 'archive')
        record.state = 'expiring'
        session.add(record)
        await session.commit()
    monkeypatch.setattr(
        worker, 'async_session', lambda: AsyncSession(engine, expire_on_commit=False)
    )
    store = SimpleNamespace(
        profile_id='primary', profile_fingerprint='f', delete=AsyncMock()
    )
    monkeypatch.setattr(worker, 'get_artifact_store', lambda *args, **kwargs: store)
    job = worker.BenchmarkMonitoringArchiver(SimpleNamespace())
    token = await job.claim('archive')
    monkeypatch.setattr(
        job, '_collect_scope', AsyncMock(side_effect=AssertionError('recapture'))
    )
    await job._capture('archive', token)
    async with AsyncSession(engine) as session:
        status = await archive.archive_status(session, context(), benchmark())
        assert status['state'] == 'expired'
        with pytest.raises(ConflictException):
            await archive.read_archive_context(session, context(), benchmark())
        assert await session.get(BenchmarkMonitoringChunk, 'chunk') is None
        assert await session.get(BenchmarkMonitoringRevision, 'revision') is not None
    store.delete.assert_awaited_once_with('key')
    await engine.dispose()


def test_hardware_redaction_includes_sample_statistics():
    from gpustack.schemas.benchmark_monitoring import BenchmarkSampleStatistics

    response = BenchmarkModelMetrics.model_validate(payload())
    response.sample_statistics = {
        'queue': [
            BenchmarkSampleStatistics(
                dimensions=dict(
                    worker_id=3, device_id='gpu-secret', source_name='source-secret'
                )
            )
        ]
    }
    archive._filter_metrics(response, 'avg,p50,p95,p99', 'all', hide_hardware=True)
    assert response.sample_statistics['queue'][0].dimensions.model_dump() == {}


def test_sample_statistics_merge_preserves_counts_peak_and_last():
    first = dict(
        dimensions={'role': 'decode'},
        average=10,
        peak=20,
        peak_at=1,
        last=20,
        last_at=1,
        threshold=None,
        threshold_fraction=None,
        observed_samples=2,
        expected_samples=2,
        coverage=1,
    )
    second = {
        **first,
        'average': 30,
        'peak': 30,
        'peak_at': 2,
        'last': 30,
        'last_at': 2,
        'observed_samples': 1,
        'expected_samples': 2,
        'coverage': 0.5,
    }
    result = worker._combine_samples({'queue': [first]}, {'queue': [second]})['queue'][
        0
    ]
    assert result['average'] == pytest.approx(50 / 3)
    assert result['peak'] == 30
    assert result['last_at'] == 2
    assert result['coverage'] == 0.75


@pytest.mark.asyncio
async def test_block_grid_and_source_lookback_are_frozen_across_flushes(monkeypatch):
    fake_model = BenchmarkModelMetrics.model_validate(payload())
    from gpustack.schemas.benchmark_monitoring import BenchmarkResourceMetrics

    fake_resource = BenchmarkResourceMetrics(
        model_id=9,
        start=1000.25,
        end=1040.75,
        step=15,
        last_refreshed_at=1041,
        history_scope='benchmark_snapshot',
    )
    runtime = AsyncMock(return_value=fake_model)
    resources = AsyncMock(return_value=fake_resource)
    monkeypatch.setattr(worker.collectors, 'collect_benchmark_model_metrics', runtime)
    monkeypatch.setattr(
        worker.collectors, 'collect_benchmark_resource_metrics', resources
    )
    history = SimpleNamespace(
        context=SimpleNamespace(
            run_window=SimpleNamespace(start=1000.25, end=1120.5), stages=[], events=[]
        ),
        instances=[],
    )
    job = worker.BenchmarkMonitoringArchiver(SimpleNamespace())
    await job._collect_scope(
        history,
        'run',
        None,
        {'anchor': 1000.25, 'step': 15, 'rate_window': 60},
        start=1062.5,
        end=1120.5,
    )
    options = runtime.await_args.kwargs
    assert options['query_interval'].start == 1075.25
    assert options['source_not_before'] == 1000.25
    assert resources.await_args.kwargs['source_not_before'] == 1000.25
    assert options['rate_window_seconds'] == 60


@pytest.mark.asyncio
async def test_failed_upload_has_durable_cleanup_index_and_no_publication(monkeypatch):
    engine = await database()
    await seed(engine)
    monkeypatch.setattr(
        worker, 'async_session', lambda: AsyncSession(engine, expire_on_commit=False)
    )
    store = SimpleNamespace(
        profile_id='primary',
        put_json=AsyncMock(side_effect=ArtifactStorageError('storage_unavailable')),
    )
    job = worker.BenchmarkMonitoringArchiver(SimpleNamespace())
    with pytest.raises(ArtifactStorageError):
        await job._store_outputs(
            row(),
            store,
            {
                'run/runtime/all/aggregate': BenchmarkModelMetrics.model_validate(
                    payload()
                )
            },
        )
    from sqlmodel import select

    async with AsyncSession(engine) as session:
        chunks = (await session.exec(select(BenchmarkMonitoringChunk))).all()
        assert len(chunks) == 2
        assert sum(chunk.published for chunk in chunks) == 1
        current = await session.get(BenchmarkMonitoringArchive, 'archive')
        assert current.revision == 1
    await engine.dispose()


@pytest.mark.asyncio
async def test_two_minute_run_flushes_partial_block_with_immutable_scope_index(
    monkeypatch,
):
    from gpustack.schemas.benchmark import Benchmark
    from gpustack.schemas.benchmark_monitoring import BenchmarkResourceMetrics
    from gpustack.artifacts import ArtifactMetadata
    from hashlib import sha256
    import json

    engine = await database()
    async with AsyncSession(engine) as session:
        record = row()
        record.revision = 0
        record.state = 'capturing'
        record.snapshot = dict(
            monitoring=dict(
                captured_at=990,
                confirmed_at=1000,
                model_id=9,
                cluster_id=1,
                owner_principal_id=4,
                backend='vllm',
            ),
            load_mode='stages',
            target_mode='model_instance',
            via_gateway=False,
            results=[],
            state='running',
        )
        record.plan.update(step=15, rate_window=60, block_seconds=300)
        session.add(record)
        await session.commit()
    active = SimpleNamespace(id=7, run_id='run-one', state='running', deleted_at=None)

    class CaptureSession(AsyncSession):
        async def get(self, entity, ident, **kwargs):
            if entity is Benchmark:
                return active
            return await super().get(entity, ident, **kwargs)

    monkeypatch.setattr(
        worker, 'async_session', lambda: CaptureSession(engine, expire_on_commit=False)
    )
    monkeypatch.setattr(worker.time, 'time', lambda: 1120)

    async def put(key, value):
        raw = json.dumps(
            value, ensure_ascii=False, sort_keys=True, separators=(',', ':')
        ).encode()
        return ArtifactMetadata('primary', key, sha256(raw).hexdigest(), 100, len(raw))

    store = SimpleNamespace(profile_id='primary', profile_fingerprint='f', put_json=put)
    monkeypatch.setattr(worker, 'get_artifact_store', lambda *args, **kwargs: store)
    job = worker.BenchmarkMonitoringArchiver(SimpleNamespace())

    async def collect(history, scope, phase_id, plan, **kwargs):
        start = kwargs.get('start', 1000)
        end = kwargs.get('end', 1090)
        data = payload()
        data.update(start=start, end=end, phase_id=None)
        responses = {
            f'{scope}/runtime/all/{view}': BenchmarkModelMetrics.model_validate(data)
            for view in ('aggregate', 'instances')
        }
        if not kwargs.get('summary_only'):
            responses[f'{scope}/resources/all/aggregate'] = BenchmarkResourceMetrics(
                model_id=9,
                start=start,
                end=end,
                step=15,
                last_refreshed_at=end,
                history_scope='benchmark_snapshot',
            )
        return responses

    monkeypatch.setattr(job, '_collect_scope', collect)
    token = await job.claim('archive')
    await job._capture('archive', token)
    async with AsyncSession(engine) as session:
        current = await session.get(BenchmarkMonitoringArchive, 'archive')
        assert current.captured_until == 1090
        assert current.revision == 1
        from sqlmodel import select

        revision = (await session.exec(select(BenchmarkMonitoringRevision))).one()
        assert revision.context['run_window'] == {'start': 1000, 'end': 1090}
        assert all(isinstance(value, str) for value in revision.scopes.values())
        assert len(revision.scopes) == 3
        scope = await session.get(
            BenchmarkMonitoringScope, revision.scopes['run/runtime/all/aggregate']
        )
        assert scope.data['metadata']['start'] == 1000
        assert scope.data['metadata']['end'] == 1090
        assert len(scope.data['objects']) == 1
    await engine.dispose()


@pytest.mark.asyncio
@pytest.mark.parametrize('confirmed_at', [None, 901])
async def test_repeated_running_confirmation_keeps_first_execution_anchor(
    monkeypatch, confirmed_at
):
    from gpustack.schemas.benchmark import Benchmark, BenchmarkSnapshot
    from gpustack.schemas.benchmark_monitoring import BenchmarkRunSnapshot
    from gpustack.monitoring import benchmarks

    snapshot = BenchmarkRunSnapshot(
        captured_at=900,
        confirmed_at=confirmed_at,
        model_id=9,
        cluster_id=1,
        backend='vllm',
    )
    record = Benchmark(name='b', snapshot=BenchmarkSnapshot(monitoring=snapshot))
    monkeypatch.setattr(benchmarks.time, 'time', lambda: 1000)
    first = await benchmarks.verify_benchmark_run(record, None, confirm=True)
    record.snapshot = first
    monkeypatch.setattr(benchmarks.time, 'time', lambda: 2000)
    second = await benchmarks.verify_benchmark_run(record, None, confirm=True)
    assert second.monitoring.confirmed_at == (901 if confirmed_at else 1000)


@pytest.mark.asyncio
async def test_setup_failure_does_not_block_result_checkpoint_transaction(monkeypatch):
    from gpustack.schemas.benchmark_monitoring import BenchmarkRunSnapshot

    engine = await database()
    record = benchmark()
    record.snapshot = SimpleNamespace(
        monitoring=BenchmarkRunSnapshot(
            captured_at=900, confirmed_at=901, model_id=9, cluster_id=1, backend='vllm'
        )
    )
    record.target_mode = 'model_instance'
    record.state = 'completed'

    def unavailable(*args, **kwargs):
        raise ArtifactStorageError('builtin_credentials_unavailable')

    monkeypatch.setattr(archive, 'get_artifact_store', unavailable)
    async with AsyncSession(engine) as session:
        assert (
            await archive.ensure_archive_for_benchmark(session, record, results=[])
            is None
        )
        await session.commit()
    await engine.dispose()


@pytest.mark.asyncio
async def test_archive_job_snapshot_excludes_role_secrets_and_raw_scheduler_fields(
    monkeypatch,
):
    from gpustack.schemas.benchmark import Benchmark
    from gpustack.schemas.benchmark_monitoring import BenchmarkRunSnapshot

    engine = await database()
    record = Benchmark(
        id=7, name='b', run_id='run-one', owner_principal_id=4, state='completed'
    )
    monitoring = BenchmarkRunSnapshot(
        captured_at=900,
        confirmed_at=901,
        model_id=9,
        cluster_id=1,
        backend='vllm',
        roles=[
            dict(
                name='decode',
                env={'HF_TOKEN': 'very-secret'},
                backend_parameters=['--api-key', 'very-secret'],
            )
        ],
    )
    record.snapshot = SimpleNamespace(monitoring=monitoring, gateway_url=None)
    monkeypatch.setattr(
        archive,
        'get_artifact_store',
        lambda *args, **kwargs: SimpleNamespace(
            profile_id='primary', profile_fingerprint='f'
        ),
    )
    point = SimpleNamespace(
        input_tokens=128,
        sequence=0,
        rate=1,
        raw_metrics={
            'scheduler_metrics': {
                'measure_start_time': 1000,
                'measure_end_time': 1010,
                'api_key': 'very-secret',
                'request_start_time': 'very-secret',
            }
        },
    )
    async with AsyncSession(engine) as session:
        result = await archive.ensure_archive_for_benchmark(
            session, record, results=[point]
        )
        assert 'very-secret' not in str(result.snapshot)
        assert result.snapshot['monitoring']['roles'] == [{'name': 'decode'}]
        assert result.snapshot['results'][0]['raw_metrics']['scheduler_metrics'] == {
            'measure_start_time': 1000,
            'measure_end_time': 1010,
        }
        await session.commit()
    await engine.dispose()


@pytest.mark.asyncio
async def test_complete_retry_repairs_scope_but_unavailable_source_preserves_old(
    monkeypatch,
):
    from gpustack.schemas.model_metrics import ModelMetric

    engine = await database()
    await seed(engine)
    monkeypatch.setattr(
        worker, 'async_session', lambda: AsyncSession(engine, expire_on_commit=False)
    )
    record = row()
    record.plan.update(repair_scopes=True)
    history = SimpleNamespace(
        context=SimpleNamespace(run_window=SimpleNamespace(start=1000.25, end=1040.75))
    )
    old = {
        'run/runtime/all/aggregate': {
            'partial': True,
            'objects': ['chunk'],
            'metadata': payload(),
        }
    }
    job = worker.BenchmarkMonitoringArchiver(SimpleNamespace())
    response = BenchmarkModelMetrics.model_validate(payload())
    response.metrics = {
        'ttft': ModelMetric(
            kind='histogram',
            unit='seconds',
            measurement_scope='engine',
            status='available',
        )
    }
    monkeypatch.setattr(
        job,
        '_collect_scope',
        AsyncMock(return_value={'run/runtime/all/aggregate': response}),
    )
    replacement = {
        'run/runtime/all/aggregate': {
            'partial': False,
            'objects': ['recovered'],
            'metadata': worker._metadata(response),
        },
        '_chunks': [],
    }
    monkeypatch.setattr(
        job, '_store_outputs', AsyncMock(return_value=(replacement, False))
    )
    await job._repair_run_scopes(record, SimpleNamespace(), history, old, True)
    assert old['run/runtime/all/aggregate']['objects'] == ['recovered']
    assert old['run/runtime/all/aggregate']['metadata_scope'] == 'native_full_scope'
    record.plan.update(repair_scopes=True)
    response.metrics['ttft'].status = 'query_error'
    before = deepcopy(old)
    await job._repair_run_scopes(record, SimpleNamespace(), history, old, True)
    assert old == before
    await engine.dispose()


@pytest.mark.asyncio
async def test_existing_publication_reports_current_job_failure():
    engine = await database()
    await seed(engine)
    async with AsyncSession(engine) as session:
        record = await session.get(BenchmarkMonitoringArchive, 'archive')
        record.state = 'failed'
        record.last_error = 'storage_unavailable'
        session.add(record)
        await session.commit()
        response = await archive.read_archive_context(
            session, context(), benchmark(), revision=1
        )
        assert response.observation_storage.archive_state == 'failed'
        assert response.observation_storage.last_error == 'storage_unavailable'
        assert response.observation_storage.capture_complete is True
    await engine.dispose()
