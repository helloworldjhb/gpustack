"""Saving a source gap completes capture without inventing missing observations."""

import pytest
from sqlmodel import select
from sqlmodel.ext.asyncio.session import AsyncSession

from gpustack.monitoring import archive
from gpustack.server import benchmark_monitoring_archiver as worker
from gpustack.schemas.benchmark import Benchmark
from gpustack.schemas.benchmark_archive import BenchmarkMonitoringRevision
from tests.monitoring.test_archive_integration_review import capture, harness
from tests.monitoring.test_benchmark_archive import context


def first_grid_tick_precedes_scrape(monkeypatch, job):
    collect = job._collect_scope

    async def sparse(*args, **kwargs):
        outputs = await collect(*args, **kwargs)
        if not kwargs.get('summary_only'):
            for response in outputs.values():
                for metric in response.metrics.values():
                    if metric.kind == 'gauge':
                        for line in metric.series:
                            line.points = [
                                (at, None if index == 0 else value)
                                for index, (at, value) in enumerate(line.points)
                            ]
                worker.collectors._statistics(response)
        return outputs

    monkeypatch.setattr(job, '_collect_scope', sparse)


@pytest.mark.asyncio
async def test_saved_short_phase_gaps_are_ready_with_partial_data(monkeypatch):
    async with harness(monkeypatch, run_end=1020.75) as value:
        first_grid_tick_precedes_scrape(monkeypatch, value.job)
        await capture(value)
        value.prom.offline = True
        async with AsyncSession(value.engine) as session:
            current = await session.get(Benchmark, 7)
            response = await archive.read_archive_metrics(session, context(), current)
            resources = await archive.read_archive_resources(
                session, context(), current
            )
            phase = await archive.read_archive_metrics(
                session, context(), current, phase_id='128:0'
            )
            publication = (
                await session.exec(select(BenchmarkMonitoringRevision))
            ).one()
            assert response.sample_statistics['running_requests'][0].coverage == 0.5
            assert resources.sample_statistics['gpu_utilization'][0].coverage == 0.5
            assert phase.metrics['ttft'].reason == 'insufficient_phase_rate_window'
            assert publication.state == 'ready'
            assert response.observation_storage.archive_state == 'ready'
            assert response.observation_storage.capture_complete is True
            assert response.observation_storage.data_coverage == 'partial'


@pytest.mark.asyncio
async def test_short_phase_query_failure_is_incomplete_and_retryable(monkeypatch):
    async with harness(monkeypatch, run_end=1020.75) as value:
        first_grid_tick_precedes_scrape(monkeypatch, value.job)
        value.prom.fail_waiting = True
        await capture(value)
        async with AsyncSession(value.engine, expire_on_commit=False) as session:
            current = await session.get(Benchmark, 7)
            before = await archive.read_archive_metrics(session, context(), current)
            assert before.metrics['waiting_requests'].status == 'query_error'
            assert before.observation_storage.archive_state == 'partial'
            assert before.observation_storage.capture_complete is False
            assert before.observation_storage.data_coverage == 'partial'
            value.prom.fail_waiting = False
            await archive.retry_archive(session, context(), current)
        await capture(value)
        value.prom.offline = True
        async with AsyncSession(value.engine) as session:
            current = await session.get(Benchmark, 7)
            after = await archive.read_archive_metrics(session, context(), current)
            assert after.metrics['waiting_requests'].status == 'available'
            assert after.observation_storage.archive_state == 'ready'
            assert after.observation_storage.capture_complete is True
            assert after.observation_storage.data_coverage == 'partial'
            pinned = await archive.read_archive_metrics(
                session, context(), current, revision=1
            )
            assert pinned.observation_storage.capture_complete is False
            assert pinned.metrics['waiting_requests'].status == 'query_error'


@pytest.mark.asyncio
async def test_legacy_sample_gap_flags_are_reclassified_only_in_new_revision(
    monkeypatch,
):
    from gpustack.schemas.benchmark_archive import (
        BenchmarkMonitoringArchive,
        BenchmarkMonitoringScope,
    )

    async with harness(monkeypatch, run_end=1020.75) as value:
        first_grid_tick_precedes_scrape(monkeypatch, value.job)
        await capture(value)
        async with AsyncSession(value.engine, expire_on_commit=False) as session:
            current = await session.get(Benchmark, 7)
            record = await session.get(BenchmarkMonitoringArchive, value.archive_id)
            record.state = 'partial'
            session.add(record)
            publication = (
                await session.exec(select(BenchmarkMonitoringRevision))
            ).one()
            publication.state = 'partial'
            session.add(publication)
            scopes = (await session.exec(select(BenchmarkMonitoringScope))).all()
            for scope in scopes:
                entry = dict(scope.data)
                entry['partial'] = entry.pop('data_partial')
                entry['block_partial'] = entry.pop('block_data_partial')
                entry['native_partial'] = entry.pop('native_data_partial', False)
                scope.data = entry
                session.add(scope)
            await session.commit()
            await archive.retry_archive(session, context(), current)
        await capture(value)
        value.prom.offline = True
        async with AsyncSession(value.engine) as session:
            current = await session.get(Benchmark, 7)
            latest = await archive.read_archive_metrics(session, context(), current)
            pinned = await archive.read_archive_metrics(
                session, context(), current, revision=1
            )
            assert latest.observation_storage.archive_revision == 2
            assert latest.observation_storage.archive_state == 'ready'
            assert latest.observation_storage.capture_complete is True
            assert latest.observation_storage.data_coverage == 'partial'
            assert pinned.observation_storage.capture_complete is False
            assert pinned.observation_storage.data_coverage == 'partial'
            assert latest.sample_statistics == pinned.sample_statistics
            assert latest.metrics == pinned.metrics
