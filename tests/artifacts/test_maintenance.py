import json
import hashlib
import gzip
from copy import deepcopy
from types import SimpleNamespace
from unittest.mock import patch

import pytest
from sqlalchemy.ext.asyncio import create_async_engine
from sqlmodel.ext.asyncio.session import AsyncSession

from gpustack.artifacts import ArtifactMetadata, ArtifactStorageError
from gpustack.artifacts.capacity import CapacityManager
from gpustack.artifacts.maintenance import ArchiveMaintenance, cleanup_archive_copies
from gpustack.schemas.benchmark_archive import (
    BenchmarkMonitoringArchive as Archive,
    BenchmarkMonitoringChunk as Chunk,
)
from gpustack.schemas.artifact_storage import (
    ArtifactStorageCapacity,
    ArtifactStorageObject,
)


class Store:
    def __init__(self, name):
        self.profile_id = name
        self.profile_fingerprint = name + '-fingerprint'
        self.objects = {}
        self.puts = 0
        self.fail_after = None
        self.hook = None

    async def get_json(self, key, expected_sha256=None):
        if key not in self.objects:
            raise ArtifactStorageError('object_missing')
        value = self.objects[key]
        digest = hashlib.sha256(
            json.dumps(
                value,
                ensure_ascii=False,
                sort_keys=True,
                separators=(',', ':'),
                allow_nan=False,
            ).encode()
        ).hexdigest()
        if expected_sha256 and digest != expected_sha256:
            raise ArtifactStorageError('checksum_mismatch')
        return deepcopy(value)

    async def put_json(self, key, value):
        self.puts += 1
        if self.fail_after is not None and self.puts > self.fail_after:
            raise ArtifactStorageError('storage_unavailable')
        self.objects[key] = deepcopy(value)
        raw = json.dumps(
            value,
            ensure_ascii=False,
            sort_keys=True,
            separators=(',', ':'),
            allow_nan=False,
        ).encode()
        if self.hook:
            await self.hook()
        return ArtifactMetadata(
            self.profile_id,
            key,
            hashlib.sha256(raw).hexdigest(),
            len(gzip.compress(raw, mtime=0)),
            len(raw),
        )

    async def delete(self, key):
        self.objects.pop(key, None)


async def database():
    engine = create_async_engine('sqlite+aiosqlite:///:memory:')
    async with engine.begin() as connection:
        await connection.run_sync(
            lambda sync: Archive.metadata.create_all(
                sync,
                tables=[
                    Archive.__table__,
                    Chunk.__table__,
                    ArtifactStorageCapacity.__table__,
                    ArtifactStorageObject.__table__,
                ],
            )
        )
    return engine


async def seed(engine, source, state='ready', count=2):
    async with AsyncSession(engine, expire_on_commit=False) as session:
        session.add(
            Archive(
                id='archive',
                benchmark_id=7,
                run_id='run-one',
                profile_id=source.profile_id,
                profile_fingerprint=source.profile_fingerprint,
                state=state,
                revision=3,
                plan={'step': 15},
            )
        )
        for i in range(count):
            key = f'installations/one/objects/{i}.json.gz'
            meta = await source.put_json(key, {'series': [0, None, i], 'name': '归档'})
            session.add(
                Chunk(
                    id=f'chunk-{i}',
                    archive_id='archive',
                    scope='run/runtime/all/aggregate',
                    profile_id=source.profile_id,
                    object_key=key,
                    sha256=meta.sha256,
                    size_bytes=meta.size_bytes,
                    uncompressed_size_bytes=meta.uncompressed_size_bytes,
                    created_at=1,
                    published=True,
                )
            )
        await session.commit()


def manager(engine, stores):
    return ArchiveMaintenance(
        SimpleNamespace(),
        session_factory=lambda: AsyncSession(engine, expire_on_commit=False),
        store_factory=lambda config=None, profile_id=None: stores[profile_id],
    )


@pytest.mark.asyncio
async def test_capacity_counts_source_and_interrupted_target_copies_then_releases():
    engine = await database()

    def sessions():
        return AsyncSession(engine, expire_on_commit=False)

    source, target = Store('source'), Store('target')
    stores = {'source': source, 'target': target}
    capacity = CapacityManager(SimpleNamespace(), sessions)
    try:
        await seed(engine, source)
        initial = (await capacity.usage())['used_bytes']
        await capacity.set_limit(initial + initial // 2)
        job = manager(engine, stores)
        with pytest.raises(ArtifactStorageError, match='storage_capacity_exceeded'):
            await job.migrate_profile('source', 'target', dry_run=False)
        assert len(source.objects) == 2
        assert len(target.objects) == 1
        async with sessions() as session:
            archive = await session.get(Archive, 'archive')
            assert archive.profile_id == 'source'
            assert archive.revision == 3
        partial = await capacity.usage()
        assert partial['used_bytes'] == initial + initial // 2
        assert partial['reserved_bytes'] == 0
        await capacity.set_limit(0)
        await job.migrate_profile('source', 'target', dry_run=False)
        assert (await capacity.usage())['used_bytes'] == initial * 2
        async with sessions() as session:
            archive = await session.get(Archive, 'archive')
        with patch(
            'gpustack.artifacts.maintenance.get_artifact_store',
            side_effect=lambda config=None, profile_id=None: stores[profile_id],
        ):
            await cleanup_archive_copies(archive, session_factory=sessions)
        assert (await capacity.usage())['total_bytes'] == 0
        assert not source.objects
        assert not target.objects
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_migration_rejects_two_profile_aliases_for_same_physical_target():
    engine = await database()
    source, target = Store('source'), Store('target')
    target.profile_fingerprint = source.profile_fingerprint
    try:
        await seed(engine, source)
        with pytest.raises(ArtifactStorageError, match='migration_profiles_identical'):
            await manager(engine, {'source': source, 'target': target}).migrate_profile(
                'source', 'target', dry_run=False
            )
        assert target.puts == 0
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_verified_profile_migration_is_atomic_and_retains_old_cleanup_refs():
    engine = await database()
    source, target = Store('source'), Store('target')
    stores = {'source': source, 'target': target}
    try:
        await seed(engine, source)
        job = manager(engine, stores)
        preview = await job.migrate_profile('source', 'target', dry_run=True)
        assert preview['archives'][0]['objects'] == 2
        assert target.puts == 0
        result = await job.migrate_profile('source', 'target', dry_run=False)
        assert result['archives'][0]['state'] == 'complete'
        async with AsyncSession(engine, expire_on_commit=False) as session:
            archive = await session.get(Archive, 'archive')
            assert archive.profile_id == 'target'
            assert archive.profile_fingerprint == target.profile_fingerprint
            assert archive.revision == 3
            assert archive.plan['step'] == 15
            for i in range(2):
                assert (await session.get(Chunk, f'chunk-{i}')).profile_id == 'target'
            assert len(source.objects) == 2
            assert archive.plan['storage_migrations'][0]['state'] == 'complete'
            with patch(
                'gpustack.artifacts.maintenance.get_artifact_store',
                side_effect=lambda config=None, profile_id=None: stores[profile_id],
            ):
                await cleanup_archive_copies(
                    archive,
                    session_factory=lambda: AsyncSession(
                        engine, expire_on_commit=False
                    ),
                )
            assert not source.objects
            assert not target.objects
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_interrupted_copy_resumes_without_switching_partial_index():
    engine = await database()
    source, target = Store('source'), Store('target')
    try:
        await seed(engine, source)
        job = manager(engine, {'source': source, 'target': target})
        target.fail_after = 1
        with pytest.raises(ArtifactStorageError, match='storage_unavailable'):
            await job.migrate_profile('source', 'target', dry_run=False)
        async with AsyncSession(engine, expire_on_commit=False) as session:
            archive = await session.get(Archive, 'archive')
            assert archive.profile_id == 'source'
            assert not archive.lease_owner
            assert (
                archive.plan['storage_migrations'][0]['objects'][0]['verified'] is True
            )
            assert (await session.get(Chunk, 'chunk-0')).profile_id == 'source'
        target.fail_after = None
        await job.migrate_profile('source', 'target', dry_run=False)
        async with AsyncSession(engine, expire_on_commit=False) as session:
            archive = await session.get(Archive, 'archive')
            assert archive.profile_id == 'target'
            assert len(archive.plan['storage_migrations']) == 1
    finally:
        await engine.dispose()


@pytest.mark.asyncio
@pytest.mark.parametrize('state', ['capturing', 'failed', 'deleting', 'expired'])
async def test_migration_never_claims_active_or_nonterminal_archives(state):
    engine = await database()
    source, target = Store('source'), Store('target')
    try:
        await seed(engine, source, state=state)
        result = await manager(
            engine, {'source': source, 'target': target}
        ).migrate_profile('source', 'target', dry_run=False)
        assert result['archives'] == []
        assert target.puts == 0
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_audit_reports_missing_object_without_modifying_objects_or_database():
    engine = await database()
    source = Store('source')
    try:
        await seed(engine, source)
        source.objects.pop(next(iter(source.objects)))
        result = await manager(engine, {'source': source}).audit(profile_id='source')
        assert result['checked'] == 2
        assert result['issues'][0]['reason'] == 'object_missing'
        async with AsyncSession(engine) as session:
            assert (await session.get(Archive, 'archive')).lease_owner is None
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_lost_fence_prevents_profile_publication():
    engine = await database()
    source, target = Store('source'), Store('target')
    try:
        await seed(engine, source)

        async def steal():
            async with AsyncSession(engine, expire_on_commit=False) as session:
                archive = await session.get(Archive, 'archive')
                archive.fencing_token += 1
                archive.lease_owner = 'new-owner'
                session.add(archive)
                await session.commit()

        target.hook = steal
        with pytest.raises(ArtifactStorageError, match='lease_lost'):
            await manager(engine, {'source': source, 'target': target}).migrate_profile(
                'source', 'target', dry_run=False
            )
        async with AsyncSession(engine) as session:
            assert (await session.get(Archive, 'archive')).profile_id == 'source'
            assert (await session.get(Chunk, 'chunk-0')).profile_id == 'source'
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_resuming_recreates_a_missing_previously_verified_target():
    engine = await database()
    source, target = Store('source'), Store('target')
    try:
        await seed(engine, source)
        job = manager(engine, {'source': source, 'target': target})
        target.fail_after = 1
        with pytest.raises(ArtifactStorageError):
            await job.migrate_profile('source', 'target', dry_run=False)
        target.objects.clear()
        target.fail_after = None
        await job.migrate_profile('source', 'target', dry_run=False)
        assert len(target.objects) == 2
    finally:
        await engine.dispose()
