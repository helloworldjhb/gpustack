"""The installation limit serializes writers and never evicts saved history."""

import asyncio
from types import SimpleNamespace

import pytest
import pytest_asyncio
from pydantic import ValidationError
from sqlalchemy.ext.asyncio import create_async_engine
from sqlmodel.ext.asyncio.session import AsyncSession

from gpustack.artifacts.capacity import CapacityManager
from gpustack.artifacts.store import ArtifactStorageError
from gpustack.schemas.artifact_storage import (
    ArtifactCapacityUpdate,
    ArtifactStorageCapacity as Capacity,
    ArtifactStorageObject as Object,
)
from gpustack.schemas.benchmark_archive import (
    BenchmarkMonitoringArchive as Archive,
    BenchmarkMonitoringChunk as Chunk,
)


@pytest_asyncio.fixture
async def database(tmp_path):
    engine = create_async_engine(f'sqlite+aiosqlite:///{tmp_path / "capacity.db"}')
    async with engine.begin() as connection:
        await connection.run_sync(
            lambda sync: Capacity.metadata.create_all(
                sync,
                tables=[
                    Capacity.__table__,
                    Object.__table__,
                    Archive.__table__,
                    Chunk.__table__,
                ],
            )
        )
    yield engine
    await engine.dispose()


def manager(engine, maximum=0):
    return CapacityManager(
        SimpleNamespace(artifact_storage_max_total_bytes=maximum),
        session_factory=lambda: AsyncSession(engine, expire_on_commit=False),
    )


@pytest.mark.asyncio
async def test_unlimited_default_and_shared_policy_survive_new_servers(database):
    first = manager(database)
    assert (await first.usage())['unlimited'] is True
    await first.reserve('profile', 'one', 2**34, archive_id='archive')
    await first.commit('profile', 'one', 2**34)
    assert (await first.usage())['used_bytes'] == 2**34
    await first.set_limit(2**35)
    second = manager(database, maximum=2)
    usage = await second.usage()
    assert usage['max_bytes'] == 2**35
    assert usage['remaining_bytes'] == 2**34
    assert usage['state'] == 'normal'
    assert usage['usage_percent'] == 50


@pytest.mark.asyncio
async def test_limit_seed_is_applied_once_and_zero_removes_limit(database):
    job = manager(database, maximum=100)
    assert (await job.usage())['max_bytes'] == 100
    await job.reserve('profile', 'one', 80)
    assert (await job.usage())['state'] == 'warning'
    await job.commit('profile', 'one', 80)
    usage = await job.set_limit(0)
    assert usage['state'] == 'unlimited'
    assert usage['remaining_bytes'] is None
    assert usage['usage_percent'] is None
    assert usage['total_bytes'] == 80
    assert (await manager(database, maximum=1).usage())['max_bytes'] == 0


@pytest.mark.asyncio
async def test_rejected_first_allocation_does_not_reset_seeded_policy(database):
    with pytest.raises(ArtifactStorageError, match='storage_capacity_exceeded'):
        await manager(database, maximum=1).reserve('profile', 'one', 2)
    assert (await manager(database).usage())['max_bytes'] == 1


@pytest.mark.asyncio
async def test_two_servers_cannot_reserve_the_same_remaining_capacity(database):
    first, second = manager(database, maximum=100), manager(database)
    await first.usage()
    results = await asyncio.gather(
        first.reserve('profile', 'one', 60),
        second.reserve('profile', 'two', 60),
        return_exceptions=True,
    )
    assert sum(result is None for result in results) == 1
    error = next(result for result in results if result is not None)
    assert isinstance(error, ArtifactStorageError)
    assert error.reason == 'storage_capacity_exceeded'
    assert (await first.usage())['reserved_bytes'] == 60


@pytest.mark.asyncio
async def test_duplicate_reservations_commit_and_deletion_are_idempotent(database):
    job = manager(database, maximum=100)
    await job.usage()
    await asyncio.gather(
        job.reserve('profile', 'one', 100),
        job.reserve('profile', 'one', 100),
    )
    assert (await job.usage())['total_bytes'] == 100
    await job.commit('profile', 'one', 75)
    await job.commit('profile', 'one', 75)
    await job.reserve('profile', 'one', 75)
    usage = await job.usage()
    assert (usage['used_bytes'], usage['reserved_bytes']) == (75, 0)
    await job.release('profile', 'one')
    await job.release('profile', 'one')
    assert (await job.usage())['total_bytes'] == 0


@pytest.mark.asyncio
async def test_uncertain_upload_reservation_survives_restart_until_deleted(database):
    job = manager(database, maximum=100)
    await job.reserve('profile', 'uncertain', 100)
    restarted = manager(database)
    assert (await restarted.usage())['state'] == 'full'
    with pytest.raises(ArtifactStorageError, match='storage_capacity_exceeded'):
        await restarted.reserve('profile', 'another', 1)
    await restarted.release('profile', 'uncertain')
    await restarted.reserve('profile', 'another', 1)
    assert (await restarted.usage())['reserved_bytes'] == 1


@pytest.mark.asyncio
async def test_lower_limit_preserves_saved_objects_and_wakes_blocked_archives(database):
    job = manager(database)
    await job.reserve('profile', 'one', 100)
    await job.commit('profile', 'one', 100)
    async with AsyncSession(database, expire_on_commit=False) as session:
        for identity, state, deleting in (
            ('blocked', 'failed', False),
            ('expired', 'expired', False),
            ('deleting', 'deleting', True),
        ):
            session.add(
                Archive(
                    id=identity,
                    benchmark_id=len(identity),
                    run_id=identity,
                    profile_id='profile',
                    profile_fingerprint='fingerprint',
                    state=state,
                    delete_requested=deleting,
                    last_error='storage_capacity_exceeded',
                    retry_at=500,
                )
            )
        await session.commit()
    usage = await job.set_limit(50)
    assert usage['state'] == 'full'
    assert usage['used_bytes'] == 100
    assert usage['usage_percent'] == 200
    assert usage['remaining_bytes'] == 0
    async with AsyncSession(database, expire_on_commit=False) as session:
        assert (await session.get(Archive, 'blocked')).retry_at == 0
        assert (await session.get(Archive, 'expired')).retry_at == 500
        assert (await session.get(Archive, 'deleting')).retry_at == 500
    with pytest.raises(ArtifactStorageError, match='storage_capacity_exceeded'):
        await job.reserve('profile', 'two', 1)
    await job.set_limit(101)
    await job.reserve('profile', 'two', 1)


def chunk(identity, profile='source', size=20, raw=50):
    return Chunk(
        id=identity,
        archive_id='archive',
        scope='run/runtime/all/aggregate',
        profile_id=profile,
        object_key=f'objects/{identity}.json.gz',
        sha256='a' * 64,
        size_bytes=size,
        uncompressed_size_bytes=raw,
        created_at=1,
        published=False,
    )


@pytest.mark.asyncio
async def test_bootstrap_counts_unpublished_chunks_and_migration_copies_once(database):
    async with AsyncSession(database, expire_on_commit=False) as session:
        session.add(chunk('confirmed', profile='target'))
        session.add(chunk('uncertain', size=0))
        session.add(
            Archive(
                id='archive',
                benchmark_id=7,
                run_id='run',
                profile_id='target',
                profile_fingerprint='fingerprint',
                plan={
                    'storage_migrations': [
                        {
                            'source_profile_id': 'source',
                            'target_profile_id': 'target',
                            'objects': [
                                {
                                    'chunk_id': 'confirmed',
                                    'object_key': 'objects/confirmed.json.gz',
                                    'size_bytes': 20,
                                    'verified': True,
                                },
                                {
                                    'chunk_id': 'uncertain',
                                    'object_key': 'objects/uncertain.json.gz',
                                    'verified': False,
                                },
                            ],
                        }
                    ]
                },
            )
        )
        await session.commit()
    job = manager(database)
    usage = await job.usage()
    assert usage['used_bytes'] == 20
    assert usage['reserved_bytes'] == (50 + 65536) * 3
    assert (await job.usage()) == usage


@pytest.mark.asyncio
async def test_two_servers_initialize_legacy_objects_only_once(database):
    async with AsyncSession(database, expire_on_commit=False) as session:
        session.add(chunk('legacy', size=30))
        await session.commit()
    first, second = manager(database, maximum=100), manager(database, maximum=100)
    results = await asyncio.gather(first.usage(), second.usage())
    assert [result['used_bytes'] for result in results] == [30, 30]
    assert (await first.usage())['total_bytes'] == 30


@pytest.mark.asyncio
async def test_commit_replaces_conservative_reservation_with_actual_compressed_size(
    database,
):
    job = manager(database, maximum=100)
    await job.reserve('profile', 'one', 100)
    await job.commit('profile', 'one', 10)
    assert (await job.usage())['total_bytes'] == 10
    await job.reserve('profile', 'two', 90)
    assert (await job.usage())['state'] == 'full'


@pytest.mark.parametrize('value', [-1, 1.5, True, '100', 9007199254740992])
def test_capacity_api_rejects_invalid_or_inexact_byte_limits(value):
    with pytest.raises(ValidationError):
        ArtifactCapacityUpdate(max_bytes=value)


@pytest.mark.asyncio
@pytest.mark.parametrize('value', [-1, True, 1.5, 9007199254740992])
async def test_manager_rejects_invalid_limits_without_changing_policy(database, value):
    job = manager(database, maximum=100)
    with pytest.raises(ArtifactStorageError, match='invalid_capacity_bytes'):
        await job.set_limit(value)
    assert (await job.usage())['max_bytes'] == 100
