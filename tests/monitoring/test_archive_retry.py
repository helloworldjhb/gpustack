"""Retry mutations remain durable independently of the status read."""

from unittest.mock import AsyncMock

import pytest
from sqlalchemy import update
from sqlmodel.ext.asyncio.session import AsyncSession

from gpustack.artifacts import manifest
from gpustack.monitoring import archive
from gpustack.schemas.benchmark_archive import BenchmarkMonitoringArchive
from tests.monitoring.test_benchmark_archive import (
    benchmark,
    context,
    database,
    seed,
)


@pytest.mark.asyncio
@pytest.mark.parametrize('expire_on_commit', [False, True])
async def test_retry_is_committed_when_manifest_status_fails(
    monkeypatch, expire_on_commit
):
    engine = await database()
    try:
        await seed(engine)
        monkeypatch.setattr(
            manifest, 'manifest_status', AsyncMock(side_effect=RuntimeError('offline'))
        )
        async with AsyncSession(engine, expire_on_commit=expire_on_commit) as session:
            with pytest.raises(RuntimeError, match='offline'):
                await archive.retry_archive(session, context(), benchmark())
        async with AsyncSession(engine) as session:
            saved = await session.get(BenchmarkMonitoringArchive, 'archive')
            assert saved.state == 'capturing'
            assert saved.retry_at == 0
            assert saved.last_error is None
            assert saved.checkpoint == 1
            assert saved.plan['repair_scopes'] is True
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_retry_commit_failure_does_not_read_manifest_status(monkeypatch):
    engine = await database()
    try:
        await seed(engine)
        status = AsyncMock(return_value={'state': 'pending'})
        monkeypatch.setattr(manifest, 'manifest_status', status)
        async with AsyncSession(engine, expire_on_commit=False) as session:
            monkeypatch.setattr(
                session, 'commit', AsyncMock(side_effect=RuntimeError('commit failed'))
            )
            with pytest.raises(RuntimeError, match='commit failed'):
                await archive.retry_archive(session, context(), benchmark())
        status.assert_not_awaited()
        async with AsyncSession(engine) as session:
            saved = await session.get(BenchmarkMonitoringArchive, 'archive')
            assert saved.state == 'ready'
            assert saved.checkpoint == 0
            assert 'repair_scopes' not in saved.plan
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_retry_status_refreshes_state_committed_by_another_session(monkeypatch):
    engine = await database()
    try:
        await seed(engine)
        async with AsyncSession(engine, expire_on_commit=False) as session:
            commit = session.commit

            async def commit_and_finish_capture():
                await commit()
                async with AsyncSession(engine) as worker_session:
                    await worker_session.exec(
                        update(BenchmarkMonitoringArchive)
                        .where(BenchmarkMonitoringArchive.id == 'archive')
                        .values(state='ready', attempts=2)
                    )
                    await worker_session.commit()

            monkeypatch.setattr(session, 'commit', commit_and_finish_capture)
            status = await archive.retry_archive(session, context(), benchmark())
        assert status['state'] == 'ready'
        assert status['attempts'] == 2
    finally:
        await engine.dispose()
