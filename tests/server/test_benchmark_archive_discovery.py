"""A competing archive insert cannot discard unrelated discovery work."""

from contextlib import asynccontextmanager
from types import SimpleNamespace

import pytest
from sqlalchemy.exc import IntegrityError

from gpustack.monitoring import archive
from gpustack.schemas.benchmark import Benchmark, BenchmarkResult, BenchmarkSnapshot
from gpustack.schemas.benchmark_archive import (
    BenchmarkMonitoringArchive,
    BenchmarkMonitoringInstallation,
)
from gpustack.schemas.benchmark_monitoring import BenchmarkRunSnapshot
from gpustack.server import benchmark_monitoring_archiver as worker


class DiscoverySession:
    """Model autoflush and savepoints at the database boundary without a database."""

    def __init__(self, benchmarks, *, conflicting_id=None, failure=None):
        self.benchmarks = benchmarks
        self.conflicting_id = conflicting_id
        self.failure = failure
        self.pending = []
        self.flushed = {}
        self.committed = {}
        self.commits = 0

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_args):
        await self.rollback()

    async def exec(self, statement):
        await self.flush()
        columns = statement.column_descriptions
        entity = columns[0]['entity']
        if entity is Benchmark:
            rows = self.benchmarks
        elif entity is BenchmarkResult:
            rows = []
        elif entity is BenchmarkMonitoringArchive:
            stored = {**self.committed, **self.flushed}
            if len(columns) == 2:
                rows = list(stored)
            else:
                values = statement.compile().params
                key = (values['benchmark_id_1'], values['run_id_1'])
                rows = [stored[key]] if key in stored else []
        else:
            raise AssertionError(f'Unexpected query entity: {entity}')
        return SimpleNamespace(
            all=lambda: rows, first=lambda: rows[0] if rows else None
        )

    async def get(self, entity, identifier):
        assert entity is BenchmarkMonitoringInstallation and identifier == 1
        return BenchmarkMonitoringInstallation(id=1, namespace_id='installation')

    def add(self, row):
        assert isinstance(row, BenchmarkMonitoringArchive)
        key = (row.benchmark_id, row.run_id)
        if row.benchmark_id == self.conflicting_id and self.failure is None:
            # Another server commits after the lookup and before this insert.
            self.committed[key] = row.model_copy(update={'id': 'concurrent-winner'})
        self.pending.append(row)

    async def flush(self):
        for row in self.pending:
            if row.benchmark_id == self.conflicting_id and self.failure is not None:
                raise self.failure
            key = (row.benchmark_id, row.run_id)
            if key in self.committed or key in self.flushed:
                raise IntegrityError('INSERT archive', {}, Exception('duplicate run'))
            self.flushed[key] = row
        self.pending.clear()

    @asynccontextmanager
    async def begin_nested(self):
        # SQLAlchemy flushes pending work before opening and releasing a savepoint.
        await self.flush()
        before = dict(self.flushed), list(self.pending)
        try:
            yield
            await self.flush()
        except BaseException:
            self.flushed, self.pending = before
            raise

    async def commit(self):
        await self.flush()
        self.committed.update(self.flushed)
        self.flushed.clear()
        self.commits += 1

    async def rollback(self):
        self.pending.clear()
        self.flushed.clear()


@pytest.fixture
def discovery(monkeypatch, config):
    store = SimpleNamespace(profile_id='primary', profile_fingerprint='f')
    monkeypatch.setattr(worker, 'get_artifact_store', lambda *_args, **_kwargs: store)
    monkeypatch.setattr(archive, 'get_artifact_store', lambda *_args, **_kwargs: store)

    def create(count, **kwargs):
        benchmarks = [
            Benchmark(
                id=identifier,
                name=f'benchmark-{identifier}',
                run_id=f'run-{identifier}',
                owner_principal_id=4,
                state='completed',
                snapshot=BenchmarkSnapshot(
                    monitoring=BenchmarkRunSnapshot(
                        captured_at=900,
                        confirmed_at=901,
                        model_id=9,
                        cluster_id=1,
                        backend='vllm',
                    )
                ),
            )
            for identifier in range(1, count + 1)
        ]
        session = DiscoverySession(benchmarks, **kwargs)
        monkeypatch.setattr(worker, 'async_session', lambda: session)
        return worker.BenchmarkMonitoringArchiver(config), session

    return create


@pytest.mark.asyncio
@pytest.mark.parametrize(
    'candidate_count', [2, 3], ids=['last-candidate', 'middle-candidate']
)
async def test_duplicate_discovery_preserves_other_archives(discovery, candidate_count):
    job, session = discovery(candidate_count, conflicting_id=2)

    await job._discover()

    assert session.committed[(2, 'run-2')].id == 'concurrent-winner'
    assert {
        key for key, row in session.committed.items() if row.id != 'concurrent-winner'
    } == {
        (identifier, f'run-{identifier}')
        for identifier in range(1, candidate_count + 1)
        if identifier != 2
    }
    assert session.commits == 1


@pytest.mark.asyncio
async def test_non_integrity_discovery_failure_reaches_scheduler(discovery):
    failure = RuntimeError('archive persistence failed')
    job, session = discovery(3, conflicting_id=2, failure=failure)

    with pytest.raises(RuntimeError, match='archive persistence failed') as raised:
        await job._discover()

    assert raised.value is failure
    assert session.committed == {}
    assert session.commits == 0
