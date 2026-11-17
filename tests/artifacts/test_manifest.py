"""Readable manifests are optional, immutable and accounted archive artifacts."""

import hashlib
import asyncio
import json

import pytest
from sqlalchemy import update
from sqlmodel import select
from sqlmodel.ext.asyncio.session import AsyncSession

from gpustack.artifacts.capacity import CapacityManager
from gpustack.artifacts.manifest import ArchiveManifestWriter, manifest_status
from gpustack.artifacts.store import ArtifactStorageError
from gpustack.schemas.benchmark_archive import (
    BenchmarkMonitoringArchive as Archive,
    BenchmarkMonitoringChunk as Chunk,
    BenchmarkMonitoringRevision as Revision,
)
from gpustack.schemas.benchmark_monitoring import BenchmarkModelMetrics
from tests.monitoring.test_archive_integration_review import capture, harness
from tests.monitoring.test_benchmark_archive import payload


def writer_for(value):
    return ArchiveManifestWriter(
        value.settings,
        session_factory=lambda: AsyncSession(value.engine, expire_on_commit=False),
        store_factory=lambda *args, **kwargs: value.store,
    )


async def revision_id(value):
    async with AsyncSession(value.engine) as session:
        return (await session.exec(select(Revision))).one().id


@pytest.mark.asyncio
async def test_completed_archive_backfills_a_plain_readable_directory_without_prometheus(
    monkeypatch,
):
    async with harness(monkeypatch, pd=True) as value:
        await capture(value)
        value.prom.offline = True
        before = len(value.sdk.objects)
        writer = writer_for(value)
        identity = await revision_id(value)
        assert await writer.ensure(identity)
        assert not await writer.ensure(identity)
        async with writer.sessions() as session:
            archive = await session.get(Archive, value.archive_id)
            status = await manifest_status(session, archive)
            manifest = (
                await session.exec(select(Chunk).where(Chunk.scope == 'manifest:1'))
            ).one()
            publication = await session.get(Revision, identity)
        assert archive.revision == publication.revision == 1
        assert archive.state == publication.state == 'ready'
        assert status['state'] == 'ready'
        assert status['filename'] == 'revision-000001.json'
        assert 'object_key' not in status and 'profile_id' not in status
        assert len(value.sdk.objects) == before + 1
        stored = value.sdk.objects['archive/' + manifest.object_key]
        raw = stored['Body']
        assert 'ContentEncoding' not in stored
        assert stored['ContentType'] == 'application/json; charset=utf-8'
        assert b'\n' in raw
        assert hashlib.sha256(raw).hexdigest() == manifest.sha256
        document = json.loads(raw)
        assert document['object_count'] == before
        assert document['revision_created_at'] == publication.created_at
        assert len(document['scopes']) == 18
        assert any(scope['kind'] == 'resources' for scope in document['scopes'])
        assert any(scope['role'] == 'prefill' for scope in document['scopes'])
        assert all(
            manifest.id not in scope['objects']
            for scope in publication.scopes.values()
            if isinstance(scope, dict)
        )
        assert await value.store.get_json(manifest.object_key) == document


@pytest.mark.asyncio
async def test_manifest_capacity_failure_does_not_break_capture_and_recovers(
    monkeypatch,
):
    async with harness(monkeypatch) as value:
        await capture(value)
        writer = writer_for(value)
        capacity = CapacityManager(value.settings, writer.sessions)
        used = (await capacity.usage())['used_bytes']
        await capacity.set_limit(used)
        identity = await revision_id(value)
        assert await writer.ensure(identity)
        async with writer.sessions() as session:
            archive = await session.get(Archive, value.archive_id)
            status = await manifest_status(session, archive)
        assert archive.state == 'ready' and archive.revision == 1
        assert archive.last_error is None
        assert status['state'] == 'failed'
        assert status['last_error'] == 'storage_capacity_exceeded'
        assert (await capacity.usage())['used_bytes'] == used
        assert not await writer.ensure(identity)
        await capacity.set_limit(0)
        assert await writer.ensure(identity, force=True)
        async with writer.sessions() as session:
            archive = await session.get(Archive, value.archive_id)
            assert (await manifest_status(session, archive))['state'] == 'ready'
        assert (await capacity.usage())['reserved_bytes'] == 0
        assert (await capacity.usage())['used_bytes'] > used


@pytest.mark.asyncio
async def test_stale_manifest_writer_cannot_commit_its_object(monkeypatch):
    async with harness(monkeypatch) as value:
        await capture(value)
        writer = writer_for(value)
        identity = await revision_id(value)
        original = writer._save_plan

        # Lose the lease after S3 upload while a fresh transaction fences the
        # old writer. No uncommitted upload may become a visible manifest.
        async def lose_commit(session, archive, plan):
            if plan['manifest_jobs']['1']['state'] == 'ready':
                async with writer.sessions() as another:
                    await another.exec(
                        update(Archive)
                        .where(Archive.id == archive.id)
                        .values(
                            fencing_token=Archive.fencing_token + 1,
                            lease_owner='replacement',
                        )
                    )
                    await another.commit()
            await original(session, archive, plan)

        monkeypatch.setattr(writer, '_save_plan', lose_commit)
        with pytest.raises(ArtifactStorageError, match='manifest_lease_lost'):
            await writer.ensure(identity)
        async with writer.sessions() as session:
            archive = await session.get(Archive, value.archive_id)
            chunks = (
                await session.exec(select(Chunk).where(Chunk.scope == 'manifest:1'))
            ).all()
        assert archive.state == 'ready' and archive.revision == 1
        assert archive.lease_owner == 'replacement'
        assert len(chunks) == 1 and not chunks[0].published


@pytest.mark.asyncio
async def test_revision_directories_are_separate_and_batches_are_bounded(monkeypatch):
    async with harness(monkeypatch) as value:
        await capture(value)
        writer = writer_for(value)
        async with writer.sessions() as session:
            original = (await session.exec(select(Revision))).one()
            session.add(
                Revision(
                    id='revision-two',
                    archive_id=original.archive_id,
                    revision=2,
                    state='partial',
                    context=original.context,
                    scopes=original.scopes,
                    created_at=original.created_at + 1,
                )
            )
            archive = await session.get(Archive, value.archive_id)
            archive.revision = 2
            session.add(archive)
            await session.commit()
        await writer.tick(limit=1)
        async with writer.sessions() as session:
            manifests = (
                await session.exec(select(Chunk).where(Chunk.scope.like('manifest:%')))
            ).all()
        assert len(manifests) == 1 and manifests[0].scope == 'manifest:2'
        before = await value.store.get_json(manifests[0].object_key)
        await writer.tick(limit=1)
        async with writer.sessions() as session:
            manifests = (
                await session.exec(select(Chunk).where(Chunk.scope.like('manifest:%')))
            ).all()
        assert {chunk.scope for chunk in manifests} == {'manifest:1', 'manifest:2'}
        assert before['archive_revision'] == 2 and before['revision_state'] == 'partial'


@pytest.mark.asyncio
async def test_manifests_do_not_consume_monitoring_run_budget_and_are_deleted(
    monkeypatch,
):
    async with harness(monkeypatch) as value:
        await capture(value)
        writer = writer_for(value)
        assert await writer.ensure(await revision_id(value))
        async with writer.sessions() as session:
            archive = await session.get(Archive, value.archive_id)
            chunks = (await session.exec(select(Chunk))).all()
        data_bytes = sum(
            chunk.uncompressed_size_bytes
            for chunk in chunks
            if not chunk.scope.startswith('manifest:')
        )
        response = BenchmarkModelMetrics.model_validate(payload())
        extra_bytes = len(response.model_dump_json())
        value.job.max_bytes = data_bytes + extra_bytes
        entries, _ = await value.job._store_outputs(
            archive, value.store, {'run/runtime/all/aggregate': response}
        )
        assert len(entries['_chunks']) == 1
        token = await value.job.claim(archive.id)
        await value.job._delete(archive, token)
        assert not value.sdk.objects
        capacity = CapacityManager(value.settings, writer.sessions)
        assert (await capacity.usage())['total_bytes'] == 0


@pytest.mark.asyncio
async def test_failed_upload_stays_reserved_and_plain_retry_is_idempotent(
    monkeypatch,
):
    async with harness(monkeypatch) as value:
        await capture(value)
        writer = writer_for(value)
        identity = await revision_id(value)
        original = value.sdk.put_object

        def unknown_ack(**parameters):
            original(**parameters)
            raise OSError('acknowledgement unavailable')

        monkeypatch.setattr(value.sdk, 'put_object', unknown_ack)
        assert await writer.ensure(identity)
        capacity = CapacityManager(value.settings, writer.sessions)
        assert (await capacity.usage())['reserved_bytes'] > 0
        monkeypatch.setattr(value.sdk, 'put_object', original)
        assert await writer.ensure(identity, force=True)
        assert (await capacity.usage())['reserved_bytes'] == 0


@pytest.mark.asyncio
async def test_running_capture_defers_optional_manifests_until_sealed(monkeypatch):
    async with harness(monkeypatch) as value:
        await capture(value)
        writer = writer_for(value)
        identity = await revision_id(value)
        async with writer.sessions() as session:
            archive = await session.get(Archive, value.archive_id)
            archive.state = 'capturing'
            session.add(archive)
            await session.commit()
        before = len(value.sdk.objects)
        assert not await writer.ensure(identity, force=True)
        assert len(value.sdk.objects) == before
        async with writer.sessions() as session:
            archive = await session.get(Archive, value.archive_id)
            archive.state = 'ready'
            session.add(archive)
            await session.commit()
        assert await writer.ensure(identity)


@pytest.mark.asyncio
async def test_slow_manifest_loop_never_blocks_capture_and_stops_with_parent(
    monkeypatch,
):
    async with harness(monkeypatch) as value:
        started, stopped = asyncio.Event(), asyncio.Event()
        calls = 0

        async def slow_manifests():
            started.set()
            try:
                await asyncio.Event().wait()
            finally:
                stopped.set()

        async def capture_tick():
            nonlocal calls
            calls += 1
            if calls == 2:
                raise asyncio.CancelledError()

        value.job.flush_interval = 0
        monkeypatch.setattr(value.job.manifests, 'run', slow_manifests)
        monkeypatch.setattr(value.job, 'tick', capture_tick)
        with pytest.raises(asyncio.CancelledError):
            await value.job.run()
        assert calls == 2 and started.is_set() and stopped.is_set()
