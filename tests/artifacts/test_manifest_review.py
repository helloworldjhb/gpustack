"""Independent checks of readable-manifest encoding and archive lifecycle."""

import asyncio
import json
import time
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import patch

import pytest
from sqlmodel import select
from sqlmodel.ext.asyncio.session import AsyncSession

from gpustack.artifacts.capacity import CapacityManager
from gpustack.artifacts.config import ArtifactStorageProfile
from gpustack.artifacts.maintenance import ArchiveMaintenance, cleanup_archive_copies
from gpustack.artifacts.managed import put_managed_json
from gpustack.artifacts.manifest import ArchiveManifestWriter, manifest_status
from gpustack.artifacts.store import ArtifactStorageError, ArtifactStore
from gpustack.schemas.benchmark_archive import (
    BenchmarkMonitoringArchive as Archive,
    BenchmarkMonitoringChunk as Chunk,
    BenchmarkMonitoringRevision as Revision,
)
from gpustack.server import benchmark_monitoring_archiver as archiver
from tests.artifacts.test_managed_capacity import database
from tests.artifacts.test_store import MemoryS3
from tests.monitoring.test_benchmark_archive import (
    database as archive_database,
    payload,
    publication,
    row,
)


@asynccontextmanager
async def environment(monkeypatch):
    engine = await archive_database()

    def sessions():
        return AsyncSession(engine, expire_on_commit=False)

    client = MemoryS3()
    store = ArtifactStore(
        'primary', ArtifactStorageProfile(bucket='artifacts', prefix='private')
    )
    store._client = client
    config = SimpleNamespace()
    writer = ArchiveManifestWriter(
        config,
        session_factory=sessions,
        store_factory=lambda *args, **kwargs: store,
    )
    metadata = await store.put_json('old-uuid-object.json.gz', payload())
    archive, revision = row(), publication()
    archive.profile_fingerprint = store.profile_fingerprint
    async with sessions() as session:
        session.add(archive)
        session.add(revision)
        session.add(
            Chunk(
                id='chunk',
                archive_id=archive.id,
                scope='phase:128:0/runtime/all/aggregate',
                profile_id='primary',
                object_key=metadata.key,
                sha256=metadata.sha256,
                size_bytes=metadata.size_bytes,
                uncompressed_size_bytes=metadata.uncompressed_size_bytes,
                created_at=1,
                published=True,
            )
        )
        await session.commit()
    monkeypatch.setattr(archiver, 'async_session', sessions)
    monkeypatch.setattr(archiver, 'get_artifact_store', lambda *args, **kwargs: store)
    try:
        yield SimpleNamespace(
            sessions=sessions,
            store=store,
            client=client,
            writer=writer,
            config=config,
        )
    finally:
        await engine.dispose()


async def leave_uploaded_manifest_unpublished(value, monkeypatch):
    original = value.writer._save_plan

    async def fail_commit(session, archive, plan):
        if plan['manifest_jobs']['1']['state'] == 'ready':
            raise ArtifactStorageError('storage_unavailable')
        return await original(session, archive, plan)

    monkeypatch.setattr(value.writer, '_save_plan', fail_commit)
    assert await value.writer.ensure('revision')
    monkeypatch.setattr(value.writer, '_save_plan', original)
    async with value.sessions() as session:
        chunks = (
            await session.exec(
                select(Chunk).where(
                    Chunk.archive_id == 'archive', Chunk.scope == 'manifest:1'
                )
            )
        ).all()
        assert len(chunks) == 1
        assert not chunks[0].published
        return chunks[0]


@pytest.mark.asyncio
@pytest.mark.parametrize('lease_takeover', [False, True])
async def test_orphan_gc_cannot_delete_a_retried_published_manifest(
    monkeypatch, lease_takeover
):
    async with environment(monkeypatch) as value:
        intent = await leave_uploaded_manifest_unpublished(value, monkeypatch)
        async with value.sessions() as session:
            stored = await session.get(Chunk, intent.id)
            stored.created_at = time.time() - 7200
            session.add(stored)
            await session.commit()
        orphan = await put_managed_json(
            value.store,
            'orphan-curve.json.gz',
            payload(),
            config=value.config,
            archive_id='archive',
            session_factory=value.sessions,
        )
        async with value.sessions() as session:
            session.add(
                Chunk(
                    id='orphan-curve',
                    archive_id='archive',
                    scope='run/runtime/all/aggregate',
                    profile_id='primary',
                    object_key=orphan.key,
                    sha256=orphan.sha256,
                    size_bytes=orphan.size_bytes,
                    uncompressed_size_bytes=orphan.uncompressed_size_bytes,
                    created_at=time.time() - 7200,
                    published=False,
                )
            )
            await session.commit()
        entered, resume = asyncio.Event(), asyncio.Event()
        original = value.store.delete
        deleted = []

        async def paused_delete(key):
            deleted.append(key)
            entered.set()
            await resume.wait()
            await original(key)

        monkeypatch.setattr(value.store, 'delete', paused_delete)
        gc = archiver.BenchmarkMonitoringArchiver(value.config)
        cleanup = asyncio.create_task(gc._clean_orphans())
        try:
            await asyncio.wait_for(entered.wait(), timeout=2)
            if lease_takeover:
                async with value.sessions() as session:
                    archive = await session.get(Archive, 'archive')
                    archive.lease_until = 0
                    archive.fencing_token += 1
                    session.add(archive)
                    await session.commit()
            await value.writer.ensure('revision', force=True)
        finally:
            resume.set()
            await cleanup
        await value.writer.ensure('revision', force=True)
        async with value.sessions() as session:
            archive = await session.get(Archive, 'archive')
            status = await manifest_status(session, archive)
        assert status['state'] == 'ready'
        assert 'private/' + intent.object_key in value.client.objects
        assert deleted == ['orphan-curve.json.gz']


@pytest.mark.asyncio
async def test_retry_preserves_frozen_sizes_and_excludes_unlisted_source_fields(
    monkeypatch,
):
    async with environment(monkeypatch) as value:
        secret = 'SECRET-MARKER-do-not-export'
        async with value.sessions() as session:
            revision = await session.get(Revision, 'revision')
            revision.context = {
                **revision.context,
                'raw_env': {'API_KEY': secret},
                'worker_ip': '192.0.2.15',
            }
            for entry in revision.scopes.values():
                entry['metadata']['raw_metrics'] = {'credential': secret}
                for metric in entry['metadata']['metrics'].values():
                    metric['series'] = [
                        {'name': secret, 'dimensions': {'source_name': secret}}
                    ]
            session.add(revision)
            await session.commit()
        intent = await leave_uploaded_manifest_unpublished(value, monkeypatch)
        before = value.client.objects['private/' + intent.object_key]['Body']
        assert secret.encode() not in before
        assert b'192.0.2.15' not in before
        async with value.sessions() as session:
            chunk = await session.get(Chunk, 'chunk')
            chunk.size_bytes += 1234
            session.add(chunk)
            await session.commit()
        assert await value.writer.ensure('revision', force=True)
        after = value.client.objects['private/' + intent.object_key]['Body']
        assert before == after
        manifest = json.loads(after)
        assert manifest['object_count'] == 1
        assert all(
            item['checksum_scope'] == 'uncompressed_json_bytes'
            for scope in manifest['scopes']
            for item in scope['objects']
        )


@pytest.mark.asyncio
async def test_safe_status_requires_a_published_revision(monkeypatch):
    async with environment(monkeypatch) as value:
        async with value.sessions() as session:
            archive = await session.get(Archive, 'archive')
            missing = await manifest_status(session, archive, revision=999)
            pending = await manifest_status(session, archive, revision=1)
        assert missing['state'] == 'unavailable'
        assert pending['state'] == 'pending'
        assert await value.writer.ensure('revision')
        async with value.sessions() as session:
            archive = await session.get(Archive, 'archive')
            ready = await manifest_status(session, archive, revision=1)
        assert ready['state'] == 'ready'
        assert ready['filename'] == 'revision-000001.json'
        assert ready['checksum_scope'] == 'utf8_json_bytes'
        assert set(ready).isdisjoint(
            {'object_key', 'profile_id', 'bucket', 'endpoint', 'object_sizes'}
        )


@pytest.mark.asyncio
@pytest.mark.parametrize('expired', [False, True])
async def test_archive_cleanup_releases_unknown_manifest_upload_reservations(
    monkeypatch, expired
):
    async with environment(monkeypatch) as value:
        original = value.client.put_object

        def unknown_upload(**kwargs):
            original(**kwargs)
            raise TimeoutError('Upload acknowledgement unavailable')

        monkeypatch.setattr(value.client, 'put_object', unknown_upload)
        assert await value.writer.ensure('revision')
        capacity = CapacityManager(value.config, session_factory=value.sessions)
        assert (await capacity.usage())['reserved_bytes'] > 0
        async with value.sessions() as session:
            archive = await session.get(Archive, 'archive')
            assert archive.state == 'ready'
            assert (await manifest_status(session, archive))['state'] == 'failed'
        gc = archiver.BenchmarkMonitoringArchiver(value.config)
        token = await gc.claim('archive')
        async with value.sessions() as session:
            archive = await session.get(Archive, 'archive')
        await gc._delete(archive, token, expired=expired)
        assert (await capacity.usage())['total_bytes'] == 0
        assert not value.client.objects
        async with value.sessions() as session:
            archive = await session.get(Archive, 'archive')
            if expired:
                assert archive.state == 'expired'
            else:
                assert archive is None


@pytest.mark.asyncio
async def test_unpublished_alias_cannot_gc_a_published_manifest_key(monkeypatch):
    async with environment(monkeypatch) as value:
        assert await value.writer.ensure('revision')
        async with value.sessions() as session:
            manifest = (
                await session.exec(select(Chunk).where(Chunk.scope == 'manifest:1'))
            ).one()
            session.add(
                Chunk(
                    id='old-manifest-intent',
                    archive_id='archive',
                    scope='manifest:1',
                    profile_id='primary',
                    object_key=manifest.object_key,
                    sha256=manifest.sha256,
                    size_bytes=manifest.size_bytes,
                    uncompressed_size_bytes=manifest.uncompressed_size_bytes,
                    created_at=time.time() - 7200,
                    published=False,
                )
            )
            await session.commit()
        gc = archiver.BenchmarkMonitoringArchiver(value.config)
        await gc._clean_orphans()
        token = await gc.claim('archive')
        async with value.sessions() as session:
            archive = await session.get(Archive, 'archive')
        await gc._clean_capacity_attempt(archive)
        assert 'private/' + manifest.object_key in value.client.objects
        await gc._delete(archive, token)
        assert not value.client.objects
        capacity = CapacityManager(value.config, session_factory=value.sessions)
        assert (await capacity.usage())['total_bytes'] == 0


@pytest.mark.asyncio
async def test_profile_migration_preserves_plain_manifest_bytes_and_gzip_objects():
    engine = await database()

    def sessions():
        return AsyncSession(engine, expire_on_commit=False)

    client = MemoryS3()
    stores = {}
    for name in ('source', 'target'):
        store = ArtifactStore(
            name,
            ArtifactStorageProfile(bucket='private-artifacts', prefix=name),
        )
        store._client = client
        stores[name] = store
    payloads = [
        (
            'manifest',
            'manifest:1',
            'installations/test/manifests/revision-000001.json',
            {'schema': 'benchmark-monitoring-manifest/v1', 'label': '归档目录'},
            'json',
        ),
        (
            'curve',
            'run/runtime/all/aggregate',
            'installations/test/objects/runtime-aggregate-id.json.gz',
            {'metrics': {'ttft': {'series': [[1, 0], [2, None]]}}},
            'json.gz',
        ),
    ]
    try:
        async with sessions() as session:
            session.add(
                Archive(
                    id='archive',
                    benchmark_id=19,
                    run_id='run-one',
                    state='ready',
                    revision=1,
                    profile_id='source',
                    profile_fingerprint=stores['source'].profile_fingerprint,
                )
            )
            for identity, scope, key, value, format in payloads:
                meta = await stores['source'].put_json(key, value, format=format)
                session.add(
                    Chunk(
                        id=identity,
                        archive_id='archive',
                        scope=scope,
                        profile_id='source',
                        object_key=key,
                        sha256=meta.sha256,
                        size_bytes=meta.size_bytes,
                        uncompressed_size_bytes=meta.uncompressed_size_bytes,
                        created_at=1,
                        published=True,
                    )
                )
            await session.commit()
        capacity = CapacityManager(SimpleNamespace(), session_factory=sessions)
        before = (await capacity.usage())['used_bytes']
        maintenance = ArchiveMaintenance(
            SimpleNamespace(),
            session_factory=sessions,
            store_factory=lambda config=None, profile_id=None: stores[profile_id],
        )
        result = await maintenance.migrate_profile('source', 'target', dry_run=False)
        assert result['archives'][0]['state'] == 'complete'
        for _, _, key, value, format in payloads:
            original = client.objects['source/' + key]
            copied = client.objects['target/' + key]
            assert copied['Body'] == original['Body']
            assert copied['Metadata']['sha256'] == original['Metadata']['sha256']
            assert copied['Metadata']['format'] == format
            assert await stores['target'].get_json(key) == value
            if format == 'json':
                assert 'ContentEncoding' not in copied
                assert copied['ContentType'] == 'application/json; charset=utf-8'
                assert b'\n' in copied['Body']
                assert '归档目录' in copied['Body'].decode()
            else:
                assert copied['ContentEncoding'] == 'gzip'
        assert (await capacity.usage())['used_bytes'] == before * 2
        async with sessions() as session:
            archive = await session.get(Archive, 'archive')
        with patch(
            'gpustack.artifacts.maintenance.get_artifact_store',
            side_effect=lambda config=None, profile_id=None: stores[profile_id],
        ):
            await cleanup_archive_copies(archive, session_factory=sessions)
        assert not client.objects
        assert (await capacity.usage())['total_bytes'] == 0
    finally:
        await engine.dispose()
