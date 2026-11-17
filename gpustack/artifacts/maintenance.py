"""Administrator-only verified profile migration and archive integrity audit."""

import asyncio
from copy import deepcopy
import time
from typing import Optional
from uuid import uuid4

from sqlalchemy import update
from sqlmodel import select

from gpustack.artifacts import ArtifactStorageError, get_artifact_store
from gpustack.artifacts.capacity import CapacityManager
from gpustack.artifacts.managed import delete_managed_object, put_managed_json
from gpustack.schemas.benchmark_archive import (
    BenchmarkMonitoringArchive as Archive,
    BenchmarkMonitoringChunk as Chunk,
)
from gpustack.server.db import async_session

_TERMINAL = ('ready', 'partial', 'no_data')
_LEASE_SECONDS = 180


def _store(factory, config, profile_id, fingerprint=None):
    store = factory(config, profile_id=profile_id)
    if store is None:
        raise ArtifactStorageError('profile_unavailable')
    if fingerprint and store.profile_fingerprint != fingerprint:
        raise ArtifactStorageError('profile_binding_changed')
    return store


def cleanup_references(archive):
    """Yield indexed old and intended target copies, including interrupted work."""
    seen = set()
    for migration in archive.plan.get('storage_migrations', []):
        for item in migration.get('objects', []):
            for side in ('source', 'target'):
                profile_id = migration.get(side + '_profile_id')
                fingerprint = migration.get(side + '_profile_fingerprint')
                key = item.get('object_key')
                if not profile_id or not fingerprint or not key:
                    raise ArtifactStorageError('invalid_migration_history')
                identity = (profile_id, key)
                if identity not in seen:
                    seen.add(identity)
                    yield {
                        'profile_id': profile_id,
                        'object_key': key,
                        'fingerprint': fingerprint,
                    }


async def cleanup_archive_copies(archive, config=None, *, session_factory=None) -> None:
    """Delete migration copies during the archive's existing fenced GC job.

    References remain in the archive plan until its GC transaction succeeds, so
    failed cleanup can retry without losing track of either storage destination.
    """
    for item in cleanup_references(archive):
        store = _store(
            get_artifact_store, config, item['profile_id'], item['fingerprint']
        )
        await delete_managed_object(
            store, item['object_key'], config=config, session_factory=session_factory
        )


class ArchiveMaintenance:
    """Lease completed archives, verify all copies, then publish one DB switch."""

    def __init__(self, config, session_factory=None, store_factory=None):
        self.config = config
        self.sessions = session_factory or async_session
        self.stores = store_factory or get_artifact_store
        self.worker_id = str(uuid4())

    def _query(self, profile_id=None, archive_id=None, run_id=None, benchmark_id=None):
        query = select(Archive)
        for field, value in (
            ('profile_id', profile_id),
            ('id', archive_id),
            ('run_id', run_id),
            ('benchmark_id', benchmark_id),
        ):
            if value is not None:
                query = query.where(getattr(Archive, field) == value)
        return query

    async def _chunks(self, archive_id):
        async with self.sessions() as session:
            return list(
                (
                    await session.exec(
                        select(Chunk)
                        .where(
                            Chunk.archive_id == archive_id, Chunk.published.is_(True)
                        )
                        .order_by(Chunk.id)
                    )
                ).all()
            )

    async def migrate_profile(
        self,
        source_id: str,
        target_id: str,
        *,
        archive_id: Optional[str] = None,
        run_id: Optional[str] = None,
        benchmark_id: Optional[int] = None,
        dry_run: bool = True,
        limit: int = 100,
    ):
        """Copy terminal archives and preserve source cleanup references.

        Args:
            source_id: Existing profile bound to the selected archives.
            target_id: Configured destination profile; a retained profile is valid.
            dry_run: Report selection without leases, uploads or DB writes.
        """
        if source_id == target_id:
            raise ArtifactStorageError('migration_profiles_identical')
        source = _store(self.stores, self.config, source_id)
        target = _store(self.stores, self.config, target_id)
        if source.profile_fingerprint == target.profile_fingerprint:
            raise ArtifactStorageError('migration_profiles_identical')
        query = (
            self._query(source_id, archive_id, run_id, benchmark_id)
            .where(Archive.state.in_(_TERMINAL), Archive.delete_requested.is_(False))
            .order_by(Archive.id)
            .limit(max(1, min(limit, 1000)))
        )
        async with self.sessions() as session:
            archives = list((await session.exec(query)).all())
        report = {
            'dry_run': dry_run,
            'source_profile_id': source_id,
            'target_profile_id': target_id,
            'archives': [],
        }
        for archive in archives:
            if source.profile_fingerprint != archive.profile_fingerprint:
                raise ArtifactStorageError('profile_binding_changed')
            if dry_run:
                chunks = await self._chunks(archive.id)
                report['archives'].append(
                    {
                        'archive_id': archive.id,
                        'run_id': archive.run_id,
                        'objects': len(chunks),
                        'state': 'planned',
                    }
                )
            else:
                report['archives'].append(
                    await self._migrate_one(archive.id, source, target)
                )
        return report

    def _fence(self, archive_id, token):
        return (
            Archive.id == archive_id,
            Archive.lease_owner == self.worker_id,
            Archive.fencing_token == token,
            Archive.lease_until > time.time(),
            Archive.delete_requested.is_(False),
            Archive.state.in_(_TERMINAL),
        )

    async def _claim(self, archive_id, source):
        async with self.sessions() as session:
            result = await session.exec(
                update(Archive)
                .where(
                    Archive.id == archive_id,
                    Archive.profile_id == source.profile_id,
                    Archive.profile_fingerprint == source.profile_fingerprint,
                    Archive.state.in_(_TERMINAL),
                    Archive.delete_requested.is_(False),
                    Archive.lease_until <= time.time(),
                )
                .values(
                    lease_owner=self.worker_id,
                    lease_until=time.time() + _LEASE_SECONDS,
                    fencing_token=Archive.fencing_token + 1,
                )
            )
            await session.commit()
            if result.rowcount != 1:
                raise ArtifactStorageError('archive_busy')
            return await session.get(Archive, archive_id)

    async def _renew(self, archive_id, token):
        while True:
            await asyncio.sleep(30)
            async with self.sessions() as session:
                result = await session.exec(
                    update(Archive)
                    .where(*self._fence(archive_id, token))
                    .values(lease_until=time.time() + _LEASE_SECONDS)
                )
                await session.commit()
                if result.rowcount != 1:
                    raise ArtifactStorageError('lease_lost')

    async def _save(self, archive, plan):
        async with self.sessions() as session:
            result = await session.exec(
                update(Archive)
                .where(*self._fence(archive.id, archive.fencing_token))
                .values(plan=deepcopy(plan), lease_until=time.time() + _LEASE_SECONDS)
            )
            if result.rowcount != 1:
                await session.rollback()
                raise ArtifactStorageError('lease_lost')
            await session.commit()

    async def _release(self, archive):
        async with self.sessions() as session:
            await session.exec(
                update(Archive)
                .where(
                    Archive.id == archive.id,
                    Archive.lease_owner == self.worker_id,
                    Archive.fencing_token == archive.fencing_token,
                )
                .values(lease_owner=None, lease_until=0)
            )
            await session.commit()

    def _migration(self, plan, chunks, source, target):
        history = plan.setdefault('storage_migrations', [])
        for migration in history:
            if (
                migration['source_profile_id'] == source.profile_id
                and migration['target_profile_id'] == target.profile_id
                and migration['state'] == 'copying'
            ):
                if (
                    migration['source_profile_fingerprint']
                    != source.profile_fingerprint
                    or migration['target_profile_fingerprint']
                    != target.profile_fingerprint
                ):
                    raise ArtifactStorageError('profile_binding_changed')
                expected = {(c.id, c.object_key, c.sha256) for c in chunks}
                recorded = {
                    (o['chunk_id'], o['object_key'], o['sha256'])
                    for o in migration['objects']
                }
                if expected != recorded:
                    raise ArtifactStorageError('archive_index_changed')
                return migration
        migration = {
            'id': str(uuid4()),
            'source_profile_id': source.profile_id,
            'source_profile_fingerprint': source.profile_fingerprint,
            'target_profile_id': target.profile_id,
            'target_profile_fingerprint': target.profile_fingerprint,
            'state': 'copying',
            'started_at': time.time(),
            'objects': [
                {
                    'chunk_id': c.id,
                    'object_key': c.object_key,
                    'sha256': c.sha256,
                    'verified': False,
                }
                for c in chunks
            ],
        }
        history.append(migration)
        return migration

    async def _migrate_one(self, archive_id, source, target):
        archive = await self._claim(archive_id, source)
        heartbeat = asyncio.create_task(self._renew(archive.id, archive.fencing_token))
        try:
            chunks = await self._chunks(archive.id)
            if any(c.profile_id != source.profile_id for c in chunks):
                raise ArtifactStorageError('archive_mixed_profiles')
            plan = deepcopy(archive.plan)
            await CapacityManager(self.config, session_factory=self.sessions).usage()
            migration = self._migration(plan, chunks, source, target)
            # Record every intended copy before any upload, including targets
            # whose upload may succeed immediately before this process dies.
            await self._save(archive, plan)
            for item in migration['objects']:
                await self._copy_object(source, target, item, archive.id)
                await self._save(archive, plan)
            await self._publish(archive, source, target, plan, migration)
            return {
                'archive_id': archive.id,
                'run_id': archive.run_id,
                'objects': len(chunks),
                'state': 'complete',
                'migration_id': migration['id'],
            }
        finally:
            heartbeat.cancel()
            await asyncio.gather(heartbeat, return_exceptions=True)
            await self._release(archive)

    async def _copy_object(self, source, target, item, archive_id):
        payload = await source.get_json(
            item['object_key'], expected_sha256=item['sha256']
        )
        if item['verified']:
            try:
                await target.get_json(
                    item['object_key'], expected_sha256=item['sha256']
                )
                return
            except ArtifactStorageError as error:
                if error.reason != 'object_missing':
                    raise
        metadata = await put_managed_json(
            target,
            item['object_key'],
            payload,
            config=self.config,
            archive_id=archive_id,
            session_factory=self.sessions,
            format='json' if item['object_key'].endswith('.json') else 'json.gz',
        )
        if metadata.sha256 != item['sha256']:
            raise ArtifactStorageError('checksum_mismatch')
        await target.get_json(item['object_key'], expected_sha256=item['sha256'])
        item.update(
            size_bytes=metadata.size_bytes,
            uncompressed_size_bytes=metadata.uncompressed_size_bytes,
            verified=True,
        )

    async def _publish(self, archive, source, target, plan, migration):
        migration.update(state='complete', completed_at=time.time())
        async with self.sessions() as session:
            result = await session.exec(
                update(Archive)
                .where(
                    *self._fence(archive.id, archive.fencing_token),
                    Archive.profile_id == source.profile_id,
                    Archive.profile_fingerprint == source.profile_fingerprint,
                    Archive.revision == archive.revision,
                    Archive.checkpoint == archive.checkpoint,
                )
                .values(
                    profile_id=target.profile_id,
                    profile_fingerprint=target.profile_fingerprint,
                    plan=deepcopy(plan),
                    lease_owner=None,
                    lease_until=0,
                )
            )
            if result.rowcount != 1:
                await session.rollback()
                raise ArtifactStorageError('lease_lost')
            for item in migration['objects']:
                result = await session.exec(
                    update(Chunk)
                    .where(
                        Chunk.id == item['chunk_id'],
                        Chunk.archive_id == archive.id,
                        Chunk.sha256 == item['sha256'],
                        Chunk.profile_id == source.profile_id,
                        Chunk.published.is_(True),
                    )
                    .values(
                        profile_id=target.profile_id,
                        size_bytes=item['size_bytes'],
                        uncompressed_size_bytes=item['uncompressed_size_bytes'],
                    )
                )
                if result.rowcount != 1:
                    await session.rollback()
                    raise ArtifactStorageError('archive_index_changed')
            await session.commit()

    async def audit(
        self,
        *,
        profile_id=None,
        archive_id=None,
        run_id=None,
        benchmark_id=None,
        limit=100,
    ):
        """Read and checksum indexed objects; never mutate objects or DB rows."""
        query = (
            self._query(profile_id, archive_id, run_id, benchmark_id)
            .order_by(Archive.id)
            .limit(max(1, min(limit, 1000)))
        )
        async with self.sessions() as session:
            archives = list((await session.exec(query)).all())
        report = {'checked': 0, 'issues': []}
        for archive in archives:
            for chunk in await self._chunks(archive.id):
                report['checked'] += 1
                try:
                    store = _store(
                        self.stores,
                        self.config,
                        chunk.profile_id,
                        (
                            archive.profile_fingerprint
                            if chunk.profile_id == archive.profile_id
                            else None
                        ),
                    )
                    await store.get_json(chunk.object_key, expected_sha256=chunk.sha256)
                except ArtifactStorageError as error:
                    report['issues'].append(
                        {
                            'archive_id': archive.id,
                            'chunk_id': chunk.id,
                            'profile_id': chunk.profile_id,
                            'object_key': chunk.object_key,
                            'reason': error.reason,
                        }
                    )
        return report
