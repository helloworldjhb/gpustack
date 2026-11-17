"""Capture and replay through SQL indexes, normalized collectors and S3 objects."""

from contextlib import asynccontextmanager
from copy import deepcopy
from hashlib import sha256
import asyncio
import io
import json
import math
import re
from types import SimpleNamespace

from botocore.exceptions import ClientError
import pytest
from sqlalchemy.ext.asyncio import create_async_engine
from sqlmodel import select
from sqlmodel.ext.asyncio.session import AsyncSession

from gpustack.api.exceptions import (
    ConflictException,
    ForbiddenException,
    ServiceUnavailableException,
)
from gpustack.artifacts import ArtifactStorageError
from gpustack.artifacts.capacity import CapacityManager
from gpustack.artifacts.config import ArtifactStorageProfile
from gpustack.artifacts.store import ArtifactStore
from gpustack.config import config as configuration
from gpustack.monitoring import archive, models, prometheus
from gpustack.schemas.benchmark import Benchmark, BenchmarkSnapshot
from gpustack.schemas.artifact_storage import (
    ArtifactStorageCapacity,
    ArtifactStorageObject,
)
from gpustack.schemas.benchmark_archive import (
    BenchmarkMonitoringArchive,
    BenchmarkMonitoringChunk,
    BenchmarkMonitoringInstallation,
    BenchmarkMonitoringRevision,
    BenchmarkMonitoringScope,
)
from gpustack.schemas.benchmark_monitoring import BenchmarkRunSnapshot
from gpustack.server import benchmark_monitoring_archiver as worker
from tests.monitoring.test_benchmark_archive import context, payload


class MemoryS3:
    """The SDK boundary keeps compression, canonical hashes and limits real."""

    def __init__(self):
        self.objects = {}
        self.reads = []
        self.fail_after = None
        self.fail_delete = False

    def put_object(self, **request):
        if self.fail_after is not None and len(self.objects) >= self.fail_after:
            raise ClientError({'Error': {'Code': '503'}}, 'PutObject')
        key = request['Key']
        if key in self.objects:
            raise ClientError({'Error': {'Code': 'PreconditionFailed'}}, 'PutObject')
        self.objects[key] = deepcopy(request)
        return {'ETag': 'opaque'}

    def get_object(self, **request):
        key = request['Key']
        self.reads.append(key)
        if key not in self.objects:
            raise ClientError({'Error': {'Code': 'NoSuchKey'}}, 'GetObject')
        item = self.objects[key]
        return {
            'Body': io.BytesIO(item['Body']),
            'Metadata': item['Metadata'],
            'ContentLength': len(item['Body']),
        }

    def head_object(self, **request):
        key = request['Key']
        if key not in self.objects:
            raise ClientError({'Error': {'Code': 'NoSuchKey'}}, 'HeadObject')
        item = self.objects[key]
        return {'Metadata': item['Metadata'], 'ContentLength': len(item['Body'])}

    def delete_object(self, **request):
        if self.fail_delete:
            raise ClientError({'Error': {'Code': 'AccessDenied'}}, 'DeleteObject')
        self.objects.pop(request['Key'], None)


class PrometheusFixture:
    """A deterministic query boundary; collector and publication code stay real."""

    def __init__(self, monitoring):
        self.monitoring = monitoring
        self.calls = []
        self.offline = False
        self.fail_waiting = False
        self.fail_full_window = None
        self.failed_calls = 0

    def engine_labels(self, expression):
        roles = re.search(r'role=~"([^"]*)"', expression)
        selected = set(roles.group(1).split('|')) if roles else None
        return [
            {
                'cluster_id': '1',
                'model_id': '9',
                'role': member.role or '',
                **source,
            }
            for member in self.monitoring.instances
            if selected is None or (member.role or '') in selected
            for source in member.source_identities
        ]

    def labels(self, expression):
        if 'gpustack:' in expression and 'gpustack:worker_' not in expression:
            rows = self.engine_labels(expression)
            if 'worker_health' in expression:
                rows = [
                    {**row, 'worker': 'http://engine-12.internal:8000'} for row in rows
                ]
            return rows
        labels = {
            key: json.loads(value)
            for key, value in re.findall(r'(\w+)=("(?:[^"\\]|\\.)*")', expression)
        }
        if 'worker_gpu_identity_info' in expression:
            labels.update(gpu_uuid='gpu-private', gpu_chip_index='0', gpu_index='0')
        return [labels]

    def check(self, expression):
        if self.offline:
            pytest.fail('Archived reads must not query Prometheus')
        if self.fail_waiting and 'gpustack:num_requests_waiting{' in expression:
            raise OSError('Prometheus transport unavailable')
        if (
            self.fail_full_window is not None
            and f'[{self.fail_full_window}]' in expression
            and 'gpustack:time_to_first_token_seconds' in expression
        ):
            self.failed_calls += 1
            raise OSError('Full interval summary unavailable')

    async def instant(self, client, url, expression, at):
        self.check(expression)
        self.calls.append(('instant', expression, at))
        labels = self.labels(expression)
        if re.search(r'gpustack:[a-z_]+_bucket\{', expression):
            return [
                {'metric': {**label, 'le': bound}, 'value': [at, str(value)]}
                for label in labels
                for bound, value in (('0.1', 5), ('0.5', 10), ('+Inf', 10))
            ]
        value = (
            10
            if re.search(r'gpustack:[a-z_]+_count\{', expression)
            else 2 if re.search(r'gpustack:[a-z_]+_sum\{', expression) else 1
        )
        return [{'metric': label, 'value': [at, str(value)]} for label in labels]

    async def ranged(self, client, url, expression, start, end, step):
        self.check(expression)
        self.calls.append(('range', expression, start, end, step))
        labels = self.labels(expression)
        bound = re.search(r'"histogram_upper_bound", "([^"]+)"', expression)
        if bound:
            labels = [
                {**label, 'histogram_upper_bound': bound.group(1)} for label in labels
            ]
        return [
            {
                'metric': label,
                'values': [
                    [
                        start + index * step,
                        (
                            '1'
                            if bound or 'worker_health' in expression
                            else str(index % 3)
                        ),
                    ]
                    for index in range(math.floor((end - start) / step) + 1)
                ],
            }
            for label in labels
        ]


def recorded(backend='vllm', pd=False):
    roles = ('prefill', 'decode', 'router') if pd else (None,)
    return BenchmarkRunSnapshot(
        captured_at=990,
        confirmed_at=1000.25,
        model_id=9,
        cluster_id=1,
        owner_principal_id=4,
        backend=backend,
        roles=[{'name': role} for role in roles if role],
        disaggregation=(
            {'mode': 'sglang-mooncake' if backend == 'sglang' else 'vllm-nixl'}
            if pd
            else None
        ),
        instances=[
            dict(
                id=12 + index,
                name=f'captured-{role or "engine"}',
                role=role,
                worker_id=3,
                worker_ids=[3],
                gpu_indexes=[0],
                allocations={3: [0]},
                source_addresses=[(f'engine-{12 + index}.internal', 8000 + index)],
                source_identities=[
                    dict(
                        model_instance_id=str(12 + index),
                        worker_id='3',
                        restart_count='0',
                        source_kind='legacy',
                        source_name='private-source',
                        endpoint_dp_rank='',
                        component_attempt='',
                    )
                ],
            )
            for index, role in enumerate(roles)
        ],
        workers=[
            dict(
                id=3,
                name='captured-worker',
                worker_uuid='worker-private',
                owner_principal_id=4,
            )
        ],
        devices=[
            dict(
                id='device-one',
                worker_id=3,
                name='GPU',
                index=0,
                uuid='gpu-private',
                model_instance_ids=[12],
            )
        ],
    )


def point(end=1180.75):
    return SimpleNamespace(
        input_tokens=128,
        sequence=0,
        rate=4,
        raw_metrics={
            'prompt': 'must-not-be-archived',
            'scheduler_metrics': {
                'request_start_time': 1000.25,
                'measure_start_time': 1000.5,
                'measure_end_time': end - 0.25,
                'request_end_time': end,
            },
        },
    )


@asynccontextmanager
async def harness(
    monkeypatch,
    *,
    backend='vllm',
    pd=False,
    state='completed',
    results=True,
    run_end=1180.75,
    worker_owner=4,
    initial_now=None,
    confirmed_at=1000.25,
):
    engine = create_async_engine('sqlite+aiosqlite:///:memory:')
    tables = [
        model.__table__
        for model in (
            Benchmark,
            BenchmarkMonitoringArchive,
            BenchmarkMonitoringChunk,
            BenchmarkMonitoringInstallation,
            BenchmarkMonitoringRevision,
            BenchmarkMonitoringScope,
            ArtifactStorageCapacity,
            ArtifactStorageObject,
        )
    ]
    async with engine.begin() as connection:
        await connection.run_sync(
            lambda sync: Benchmark.metadata.create_all(sync, tables=tables)
        )
    clock = SimpleNamespace(now=run_end + 120 if initial_now is None else initial_now)
    settings = SimpleNamespace(
        benchmark_archive_settle_seconds=0,
        benchmark_archive_flush_interval_seconds=60,
        benchmark_archive_sample_step_seconds=15,
    )
    sdk = MemoryS3()
    store = ArtifactStore(
        'primary', ArtifactStorageProfile(bucket='test', prefix='archive')
    )
    store._client = sdk
    monitoring = recorded(backend, pd)
    monitoring.confirmed_at = confirmed_at
    monitoring.captured_at = min(monitoring.captured_at, confirmed_at - 200)
    monitoring.workers[0].owner_principal_id = worker_owner
    prom = PrometheusFixture(monitoring)
    monkeypatch.setattr(configuration, 'get_global_config', lambda: settings)
    monkeypatch.setattr(
        worker, 'async_session', lambda: AsyncSession(engine, expire_on_commit=False)
    )
    monkeypatch.setattr(worker, 'get_artifact_store', lambda *args, **kwargs: store)
    monkeypatch.setattr(archive, 'get_artifact_store', lambda *args, **kwargs: store)
    monkeypatch.setattr(worker.time, 'time', lambda: clock.now)
    monkeypatch.setattr(models, '_prometheus_url', lambda: 'http://prometheus.invalid')
    monkeypatch.setattr(
        prometheus, 'prometheus_url', lambda: 'http://prometheus.invalid'
    )
    monkeypatch.setattr(models, '_query_instant', prom.instant)
    monkeypatch.setattr(models, '_query_range', prom.ranged)
    monkeypatch.setattr(prometheus, 'query_instant', prom.instant)
    monkeypatch.setattr(prometheus, 'query_range', prom.ranged)
    archive._CACHE.clear()
    archive._INFLIGHT.clear()
    archive._CACHE_SIZE = 0
    benchmark = Benchmark(
        id=7,
        name='integration',
        cluster_id=1,
        model_id=9,
        model_instance_name='captured-engine',
        owner_principal_id=4,
        run_id='run-one',
        state=state,
        snapshot=BenchmarkSnapshot(monitoring=monitoring),
        time_to_first_token_p99=777,
    )
    async with AsyncSession(engine, expire_on_commit=False) as session:
        session.add(benchmark)
        capture = await archive.ensure_archive_for_benchmark(
            session, benchmark, results=[point(run_end)] if results else []
        )
        await session.commit()
        archive_id = capture.id
    value = SimpleNamespace(
        engine=engine,
        sdk=sdk,
        store=store,
        prom=prom,
        clock=clock,
        settings=settings,
        archive_id=archive_id,
        benchmark=benchmark,
        job=worker.BenchmarkMonitoringArchiver(settings),
    )
    try:
        yield value
    finally:
        archive._CACHE.clear()
        archive._INFLIGHT.clear()
        archive._CACHE_SIZE = 0
        await engine.dispose()


async def capture(value):
    token = await value.job.claim(value.archive_id)
    assert token is not None
    await value.job._capture(value.archive_id, token)
    return token


@pytest.mark.asyncio
@pytest.mark.parametrize(
    'backend,pd', [('vllm', False), ('sglang', False), ('vllm', True), ('sglang', True)]
)
async def test_capture_publish_replays_without_prometheus_or_current_inventory(
    monkeypatch, backend, pd
):
    async with harness(monkeypatch, backend=backend, pd=pd) as value:
        await capture(value)
        assert value.prom.calls
        value.prom.offline = True
        async with AsyncSession(value.engine) as session:
            current = await session.get(Benchmark, 7)
            publication = (
                await session.exec(select(BenchmarkMonitoringRevision))
            ).one()
            chunks = (await session.exec(select(BenchmarkMonitoringChunk))).all()
            assert publication.state == 'ready'
            assert len(publication.scopes) == 18
            assert chunks and all(chunk.published for chunk in chunks)
            filenames = {
                chunk.scope: chunk.object_key.rsplit('/', 1)[-1] for chunk in chunks
            }
            assert filenames['run/resources/all/aggregate'].startswith('resources-')
            assert filenames['run/runtime/decode/instances'].startswith(
                'runtime-decode-instances-'
            )
            for chunk in chunks:
                assert chunk.object_key.endswith(f'-{chunk.id}.json.gz')
                stored = value.sdk.objects['archive/' + chunk.object_key]
                assert chunk.sha256 == stored['Metadata']['sha256']
                assert chunk.uncompressed_size_bytes == int(
                    stored['Metadata']['uncompressed-size']
                )
                assert chunk.size_bytes == len(stored['Body'])
                assert 'private-source' not in chunk.object_key
            history = await archive.read_archive_context(
                session, context(), current, revision=1
            )
            metrics = await archive.read_archive_metrics(
                session, context(), current, revision=1
            )
            resources = await archive.read_archive_resources(
                session, context(), current, revision=1
            )
            reads = len(value.sdk.reads)
            phases = await archive.read_archive_phase_observations(
                session, context(), current, revision=1
            )
            assert len(value.sdk.reads) == reads
            assert history.observation_storage.capture_complete
            assert history.run_window.start == 1000.25
            assert history.stages[0].start == 1000.5
            assert {line.statistic for line in metrics.metrics['ttft'].series} == {
                'avg',
                'p50',
                'p95',
                'p99',
            }
            assert any(
                v == 0
                for line in metrics.metrics['ttft'].series
                for _, v in line.points
            )
            ingress = [
                metric
                for metric in metrics.metrics.values()
                if metric.measurement_scope == 'service_ingress'
            ]
            assert ingress and all(m.reason == 'gateway_bypassed' for m in ingress)
            assert resources.allocated_gpu_count == 1
            assert (
                resources.metrics['gpu_utilization'].series[0].dimensions.device_id
                == 'device-one'
            )
            phase = phases['phases']['128:0']['runtime']
            assert phase.metrics['ttft'].series == []
            assert {
                summary.statistic for summary in phase.metrics['ttft'].summaries
            } == {'avg', 'p50', 'p95', 'p99'}
            assert current.time_to_first_token_p99 == 777
            assert 'must-not-be-archived' not in json.dumps(publication.context)
            if pd:
                assert {
                    summary.dimensions.role
                    for summary in metrics.metrics['input_tokens'].summaries
                } == {'prefill'}
                assert {
                    summary.dimensions.role
                    for summary in metrics.metrics['output_tokens'].summaries
                } == {'decode'}
                expected = 'available' if backend == 'sglang' else 'unsupported'
                assert metrics.metrics['pd_prefill_bootstrap_queue'].status == expected


@pytest.mark.asyncio
async def test_running_flush_then_stop_on_another_server_preserves_grid_and_pinned_revision(
    monkeypatch,
):
    async with harness(monkeypatch, state='running', results=False) as value:
        value.clock.now = 1120.75
        await capture(value)
        async with AsyncSession(value.engine, expire_on_commit=False) as session:
            current = await session.get(Benchmark, 7)
            first = await archive.read_archive_metrics(
                session, context(), current, revision=1
            )
            assert first.end == 1120.75
            current.state = 'stopped'
            value.clock.now = 1180.75
            await archive.ensure_archive_for_benchmark(session, current, results=[])
            await session.commit()
        value.job = worker.BenchmarkMonitoringArchiver(value.settings)
        await capture(value)
        value.prom.offline = True
        async with AsyncSession(value.engine) as session:
            current = await session.get(Benchmark, 7)
            final = await archive.read_archive_metrics(
                session, context(), current, revision=2
            )
            pinned = await archive.read_archive_metrics(
                session, context(), current, revision=1
            )
            assert pinned.model_dump(
                exclude={'observation_storage'}
            ) == first.model_dump(exclude={'observation_storage'})
            assert pinned.observation_storage.archive_revision == 1
            assert not pinned.observation_storage.capture_complete
            assert final.observation_storage.capture_complete
            assert final.end == 1180.75
            expected = [1000.25 + 15 * index for index in range(13)]
            assert [at for at, _ in final.metrics['ttft'].series[0].points] == expected
            assert final.sample_statistics['running_requests'][0].expected_samples == 13
            assert final.sample_statistics['running_requests'][0].observed_samples == 13


@pytest.mark.asyncio
async def test_running_capture_starts_before_first_result_with_confirmation_checkpoint(
    monkeypatch,
):
    async with harness(
        monkeypatch,
        state='running',
        results=False,
        initial_now=1000,
        confirmed_at=1000,
    ) as value:
        value.clock.now = 1020
        await capture(value)
        async with AsyncSession(value.engine) as session:
            current = await session.get(Benchmark, 7)
            history = await archive.read_archive_context(session, context(), current)
            record = await session.get(BenchmarkMonitoringArchive, value.archive_id)
            assert record.snapshot['observed_at'] == 1000
            assert history.run_window is not None
            assert (history.run_window.start, history.run_window.end) == (1000, 1020)
            runtime = await archive.read_archive_metrics(session, context(), current)
            assert [
                at for at, _ in runtime.metrics['running_requests'].series[0].points
            ] == [1000, 1015]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    'traffic_start,traffic_end', [(1100, 1200), (900, 1200), (1000, 1010)]
)
async def test_first_measured_window_replaces_provisional_capture_without_changing_grid(
    monkeypatch, traffic_start, traffic_end
):
    async with harness(
        monkeypatch,
        state='running',
        results=False,
        initial_now=1000,
        confirmed_at=1000,
    ) as value:
        value.clock.now = 1020
        await capture(value)
        async with AsyncSession(value.engine, expire_on_commit=False) as session:
            current = await session.get(Benchmark, 7)
            provisional = await archive.read_archive_metrics(
                session, context(), current, revision=1
            )
            assert provisional is not None
            assert (provisional.start, provisional.end) == (1000, 1020)
            measured = point(end=traffic_end)
            measured.raw_metrics['scheduler_metrics'].update(
                request_start_time=traffic_start,
                measure_start_time=traffic_start + 1,
            )
            current.state = 'completed'
            value.clock.now = 1220
            await archive.ensure_archive_for_benchmark(
                session, current, results=[measured]
            )
            await session.commit()
        value.prom.calls.clear()
        await capture(value)
        value.prom.offline = True
        archive._CACHE.clear()
        archive._CACHE_SIZE = 0
        async with AsyncSession(value.engine) as session:
            current = await session.get(Benchmark, 7)
            final = await archive.read_archive_metrics(
                session, context(), current, revision=2
            )
            hardware = await archive.read_archive_resources(
                session, context(), current, revision=2
            )
            pinned = await archive.read_archive_metrics(
                session, context(), current, revision=1
            )
            record = await session.get(BenchmarkMonitoringArchive, value.archive_id)
            assert record.plan['anchor'] == 1000
            assert final.start == traffic_start
            assert final.end == traffic_end
            first_tick = 1000 + math.ceil((traffic_start - 1000) / 15) * 15
            expected = list(range(first_tick, traffic_end, 15))
            gauge = final.metrics['running_requests'].series[0]
            assert [at for at, _ in gauge.points] == expected
            samples = final.sample_statistics['running_requests'][0]
            assert samples.expected_samples == samples.observed_samples == len(expected)
            assert samples.average == pytest.approx(
                sum(v for _, v in gauge.points) / len(expected)
            )
            gpu = hardware.metrics['gpu_utilization'].series[0]
            assert [at for at, _ in gpu.points] == expected
            gpu_samples = hardware.sample_statistics['gpu_utilization'][0]
            assert (
                gpu_samples.expected_samples
                == gpu_samples.observed_samples
                == len(expected)
            )
            assert pinned.model_dump(
                exclude={'observation_storage'}
            ) == provisional.model_dump(exclude={'observation_storage'})


@pytest.mark.asyncio
async def test_failed_upload_retries_without_exposing_unpublished_objects(monkeypatch):
    async with harness(monkeypatch) as value:
        value.sdk.fail_after = 2
        token = await value.job.claim(value.archive_id)
        with pytest.raises(ArtifactStorageError):
            await value.job._capture(value.archive_id, token)
        await value.job._fail(value.archive_id, token, 'storage_unavailable')
        async with AsyncSession(value.engine, expire_on_commit=False) as session:
            current = await session.get(Benchmark, 7)
            record = await session.get(BenchmarkMonitoringArchive, value.archive_id)
            chunks = (await session.exec(select(BenchmarkMonitoringChunk))).all()
            assert record.revision == 0
            assert record.state == 'failed'
            assert current.state == 'completed'
            assert chunks and not any(chunk.published for chunk in chunks)
            assert (
                await archive.read_archive_context(session, context(), current) is None
            )
            await archive.retry_archive(session, context(), current)
        value.sdk.fail_after = None
        value.job = worker.BenchmarkMonitoringArchiver(value.settings)
        await capture(value)
        async with AsyncSession(value.engine) as session:
            current = await session.get(Benchmark, 7)
            replay = await archive.read_archive_metrics(session, context(), current)
            assert replay.observation_storage.archive_revision == 1
            assert replay.available
            assert replay.metrics['ttft'].summaries


@pytest.mark.asyncio
async def test_capacity_rejection_keeps_benchmark_and_run_budget_recoverable(
    monkeypatch,
):
    async with harness(monkeypatch) as value:

        def sessions():
            return AsyncSession(value.engine, expire_on_commit=False)

        capacity = CapacityManager(value.settings, sessions)
        await capacity.set_limit(1)
        for _ in range(5):
            token = await value.job.claim(value.archive_id)
            with pytest.raises(ArtifactStorageError, match='storage_capacity_exceeded'):
                await value.job._capture(value.archive_id, token)
            await value.job._fail(value.archive_id, token, 'storage_capacity_exceeded')
        async with sessions() as session:
            record = await session.get(BenchmarkMonitoringArchive, value.archive_id)
            benchmark = await session.get(Benchmark, 7)
            assert record.last_error == 'storage_capacity_exceeded'
            assert record.state == 'failed'
            assert record.revision == 0
            assert benchmark.state == 'completed'
            assert benchmark.time_to_first_token_p99 == 777
            assert not (await session.exec(select(BenchmarkMonitoringChunk))).all()
        assert not value.sdk.objects
        assert (await capacity.usage())['total_bytes'] == 0
        await capacity.set_limit(0)
        async with sessions() as session:
            record = await session.get(BenchmarkMonitoringArchive, value.archive_id)
            assert record.retry_at == 0
        await capture(value)
        value.prom.offline = True
        async with sessions() as session:
            benchmark = await session.get(Benchmark, 7)
            replay = await archive.read_archive_metrics(session, context(), benchmark)
            assert replay.available
            assert replay.observation_storage.archive_state == 'ready'
            assert replay.metrics['ttft'].summaries
        assert (await capacity.usage())['reserved_bytes'] == 0
        assert (await capacity.usage())['used_bytes'] == sum(
            len(item['Body']) for item in value.sdk.objects.values()
        )


@pytest.mark.asyncio
async def test_mid_batch_capacity_rejection_frees_only_confirmed_unpublished_objects(
    monkeypatch,
):
    from gpustack.schemas.benchmark_monitoring import BenchmarkModelMetrics

    async with harness(monkeypatch) as value:

        def sessions():
            return AsyncSession(value.engine, expire_on_commit=False)

        capacity = CapacityManager(value.settings, sessions)
        response = BenchmarkModelMetrics.model_validate(payload())
        prepared = await value.store.prepare_json(response.model_dump(mode='json'))
        size = len(prepared.body)
        await capacity.set_limit(size + 1)
        await value.job.claim(value.archive_id)
        async with sessions() as session:
            record = await session.get(BenchmarkMonitoringArchive, value.archive_id)
        outputs = {'first': response, 'second': response}
        with pytest.raises(ArtifactStorageError, match='storage_capacity_exceeded'):
            await value.job._store_outputs(record, value.store, outputs)
        assert not value.sdk.objects
        assert (await capacity.usage())['total_bytes'] == 0
        async with sessions() as session:
            assert not (await session.exec(select(BenchmarkMonitoringChunk))).all()
        await capacity.set_limit(size * 2)
        entries, _ = await value.job._store_outputs(record, value.store, outputs)
        assert len(entries['_chunks']) == 2
        assert len(value.sdk.objects) == 2
        assert (await capacity.usage())['used_bytes'] == size * 2


@pytest.mark.asyncio
async def test_query_error_preserves_other_metrics_and_completed_benchmark(monkeypatch):
    async with harness(monkeypatch) as value:
        value.prom.fail_waiting = True
        await capture(value)
        value.prom.offline = True
        async with AsyncSession(value.engine) as session:
            current = await session.get(Benchmark, 7)
            replay = await archive.read_archive_metrics(session, context(), current)
            assert current.state == 'completed'
            assert replay.observation_storage.archive_state == 'partial'
            assert not replay.observation_storage.capture_complete
            assert replay.metrics['waiting_requests'].status == 'query_error'
            assert replay.metrics['waiting_requests'].reason == 'query_failed'
            assert replay.metrics['ttft'].status == 'available'
            assert replay.metrics['ttft'].summaries


@pytest.mark.asyncio
async def test_failed_native_full_run_summary_cannot_be_replaced_with_last_block(
    monkeypatch,
):
    async with harness(monkeypatch, run_end=1700.75) as value:
        value.prom.fail_full_window = '700500ms'
        await capture(value)
        assert value.prom.failed_calls > 0
        value.prom.offline = True
        async with AsyncSession(value.engine, expire_on_commit=False) as session:
            current = await session.get(Benchmark, 7)
            replay = await archive.read_archive_metrics(session, context(), current)
            ttft = replay.metrics['ttft']
            assert ttft.status == 'query_error'
            assert ttft.reason == 'query_failed'
            assert all(
                s.value is None and s.lower_bound is None for s in ttft.summaries
            )
            assert ttft.series and any(
                v is not None for line in ttft.series for _, v in line.points
            )
            assert replay.observation_storage.archive_state == 'partial'
            assert replay.observation_storage.data_coverage == 'partial'
            value.prom.offline = False
            value.prom.fail_full_window = None
            await archive.retry_archive(session, context(), current)
        await capture(value)
        value.prom.offline = True
        async with AsyncSession(value.engine) as session:
            current = await session.get(Benchmark, 7)
            repaired = await archive.read_archive_metrics(session, context(), current)
            assert repaired.metrics['ttft'].status == 'available'
            assert repaired.observation_storage.archive_state == 'ready'
            assert repaired.observation_storage.data_coverage == 'complete'
            assert repaired.observation_storage.archive_revision == 2
            pinned = await archive.read_archive_metrics(
                session, context(), current, revision=1
            )
            assert pinned.metrics['ttft'].status == 'query_error'
            assert pinned.observation_storage.data_coverage == 'partial'


@pytest.mark.asyncio
async def test_superseded_execution_finishes_only_from_its_durable_checkpoint(
    monkeypatch,
):
    async with harness(monkeypatch, state='running', results=False) as value:
        value.clock.now = 1120.75
        await capture(value)
        async with AsyncSession(value.engine, expire_on_commit=False) as session:
            current = await session.get(Benchmark, 7)
            current.state = 'stopped'
            value.clock.now = 1150.75
            await archive.ensure_archive_for_benchmark(session, current, results=[])
            await session.commit()
            current.run_id = 'replacement-run'
            current.state = 'running'
            snapshot = current.snapshot.model_copy(deep=True)
            snapshot.monitoring.captured_at = 1990
            snapshot.monitoring.confirmed_at = 2000.25
            current.snapshot = snapshot
            value.clock.now = 2100.75
            newer = await archive.ensure_archive_for_benchmark(
                session, current, results=[]
            )
            await session.commit()
            new_id = newer.id
        value.prom.calls.clear()
        await capture(value)
        assert all(
            call[2] < 2000 if call[0] == 'instant' else call[3] < 2000
            for call in value.prom.calls
        )
        value.prom.offline = True
        async with AsyncSession(value.engine) as session:
            current = await session.get(Benchmark, 7)
            replay = await archive.read_archive_metrics(
                session, context(), current, run_id='run-one', revision=2
            )
            assert replay.start == 1000.25
            assert replay.end == 1150.75
            assert replay.observation_storage.capture_complete
            new_record = await session.get(BenchmarkMonitoringArchive, new_id)
            assert new_record.revision == 0
            assert new_record.captured_until is None


@pytest.mark.asyncio
@pytest.mark.parametrize(
    'damage,reason', [('missing', 'object_missing'), ('checksum', 'checksum_mismatch')]
)
async def test_object_loss_and_corruption_are_unavailable_without_live_fallback(
    monkeypatch, damage, reason
):
    async with harness(monkeypatch) as value:
        await capture(value)
        value.prom.offline = True
        async with AsyncSession(value.engine) as session:
            current = await session.get(Benchmark, 7)
            chunk = (
                await session.exec(
                    select(BenchmarkMonitoringChunk).where(
                        BenchmarkMonitoringChunk.scope == 'run/runtime/all/aggregate'
                    )
                )
            ).one()
            key = 'archive/' + chunk.object_key
            if damage == 'missing':
                del value.sdk.objects[key]
            else:
                value.sdk.objects[key]['Metadata']['sha256'] = sha256(
                    b'other-content'
                ).hexdigest()
            with pytest.raises(ServiceUnavailableException) as error:
                await archive.read_archive_metrics(
                    session, context(), current, revision=1
                )
            assert error.value.details['reason'] == reason
            summaries = await archive.read_archive_phase_observations(
                session, context(), current, revision=1
            )
            assert summaries['phases']['128:0']['runtime'].available


@pytest.mark.asyncio
async def test_benchmark_deletion_retains_retryable_cleanup_after_s3_failure(
    monkeypatch,
):
    async with harness(monkeypatch) as value:
        await capture(value)
        value.prom.offline = True
        async with AsyncSession(value.engine, expire_on_commit=False) as session:
            current = await session.get(Benchmark, 7)
            await archive.enqueue_archive_deletion(session, current)
            await session.delete(current)
            await session.commit()
            with pytest.raises(ConflictException):
                await archive.read_archive_context(session, context(), current)
        value.sdk.fail_delete = True
        token = await value.job.claim(value.archive_id)
        with pytest.raises(ArtifactStorageError):
            await value.job._capture(value.archive_id, token)
        await value.job._fail(value.archive_id, token, 'access_denied')
        async with AsyncSession(value.engine) as session:
            record = await session.get(BenchmarkMonitoringArchive, value.archive_id)
            assert record.state == 'deleting'
            assert record.delete_requested
            assert record.last_error == 'access_denied'
            assert (await session.exec(select(BenchmarkMonitoringChunk))).all()
            assert value.sdk.objects
        value.sdk.fail_delete = False
        value.job = worker.BenchmarkMonitoringArchiver(value.settings)
        await capture(value)
        async with AsyncSession(value.engine) as session:
            assert (
                await session.get(BenchmarkMonitoringArchive, value.archive_id) is None
            )
            for model in (
                BenchmarkMonitoringChunk,
                BenchmarkMonitoringScope,
                BenchmarkMonitoringRevision,
            ):
                assert (await session.exec(select(model))).all() == []
        assert not value.sdk.objects


@pytest.mark.asyncio
async def test_hidden_hardware_source_is_absent_from_public_series_names_and_statistics(
    monkeypatch,
):
    async with harness(monkeypatch, backend='sglang', pd=True) as value:
        await capture(value)
        async with AsyncSession(value.engine, expire_on_commit=False) as session:
            current = await session.get(Benchmark, 7)
            record = await session.get(BenchmarkMonitoringArchive, value.archive_id)
            snapshot = deepcopy(record.snapshot)
            snapshot['monitoring']['workers'][0]['owner_principal_id'] = 99
            record.snapshot = snapshot
            await session.commit()
            result = await archive.read_archive_metrics(
                session, context(), current, revision=1
            )
            assert result.metrics['pd_prefill_bootstrap_queue'].series
            assert 'private-source' not in result.model_dump_json()
            assert all(
                sample.dimensions.source_name is None
                and sample.dimensions.worker_id is None
                for samples in result.sample_statistics.values()
                for sample in samples
            )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    'principal', ['owner', 'cluster_grantee', 'admin_act_as', 'admin_all']
)
async def test_shared_hardware_authorization_matches_current_worker_visibility(
    monkeypatch, principal
):
    async with harness(monkeypatch, worker_owner=99) as value:
        await capture(value)
        value.prom.offline = True
        admin = principal.startswith('admin_')
        ctx = context(owner=None if principal == 'admin_all' else 4, admin=admin)
        if principal == 'cluster_grantee':
            ctx.accessible_cluster_ids = {1}
            ctx.accessible_cluster_owner_ids = {99}
        async with AsyncSession(value.engine) as session:
            current = await session.get(Benchmark, 7)
            history = await archive.read_archive_context(session, ctx, current)
            if principal == 'admin_all':
                assert history.resource_status == 'available'
                resources = await archive.read_archive_resources(session, ctx, current)
                assert resources.allocated_gpu_count == 1
            else:
                assert history.resource_status == 'restricted'
                assert history.workers == []
                with pytest.raises(ForbiddenException):
                    await archive.read_archive_resources(session, ctx, current)


@pytest.mark.asyncio
@pytest.mark.parametrize('fetch_fails', [False, True])
async def test_cancelled_reader_releases_completed_background_fetch_and_can_retry(
    monkeypatch, fetch_fails
):
    async with harness(monkeypatch) as value:
        await capture(value)
        started, release, drained = asyncio.Event(), asyncio.Event(), asyncio.Event()
        original = value.store.get_json
        attempts = 0

        async def fetch(*args, **kwargs):
            nonlocal attempts
            attempts += 1
            if attempts == 1:
                started.set()
                await release.wait()
                if fetch_fails:
                    raise ArtifactStorageError('storage_unavailable')
            return await original(*args, **kwargs)

        monkeypatch.setattr(value.store, 'get_json', fetch)

        async def read():
            async with AsyncSession(value.engine) as session:
                current = await session.get(Benchmark, 7)
                return await archive.read_archive_metrics(
                    session, context(), current, revision=1
                )

        request = asyncio.create_task(read())
        await asyncio.wait_for(started.wait(), timeout=5)
        pending = list(archive._INFLIGHT.values())
        assert len(pending) == 1
        pending[0].add_done_callback(lambda task: drained.set())
        request.cancel()
        with pytest.raises(asyncio.CancelledError):
            await request
        release.set()
        await asyncio.wait_for(drained.wait(), timeout=5)
        await asyncio.gather(*pending, return_exceptions=True)
        assert not archive._INFLIGHT
        replay = await read()
        assert replay.available
        assert attempts == 2
