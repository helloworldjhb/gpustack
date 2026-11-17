"""Optional human-readable directories for immutable monitoring revisions."""

import asyncio
from copy import deepcopy
from datetime import datetime, timezone
import hashlib
import json
import logging
import math
import re
import time
from uuid import NAMESPACE_URL, uuid4, uuid5

from sqlalchemy import delete, or_, update
from sqlmodel import select

from gpustack.artifacts import ArtifactStorageError, get_artifact_store
from gpustack.artifacts.capacity import CapacityManager
from gpustack.artifacts.managed import put_managed_json
from gpustack.artifacts.store import _relative_key
from gpustack.schemas.benchmark_archive import (
    BenchmarkMonitoringArchive as Archive,
    BenchmarkMonitoringChunk as Chunk,
    BenchmarkMonitoringRevision as Revision,
    BenchmarkMonitoringScope as Scope,
)
from gpustack.server.db import async_session

logger = logging.getLogger(__name__)
_LEASE_SECONDS = 180
_UNAVAILABLE = ('deleting', 'expiring', 'expired')
_TERMINAL = ('ready', 'partial', 'no_data')
_CODE = re.compile(r'^[a-z][a-z0-9_]{0,127}$')


def _code(value):
    return value if isinstance(value, str) and _CODE.fullmatch(value) else None


def _window(value):
    if not isinstance(value, dict):
        return None
    result = {key: value.get(key) for key in ('start', 'end')}
    return {
        **result,
        'start_utc': _utc(result['start']),
        'end_utc': _utc(result['end']),
    }


def _utc(value):
    if (
        isinstance(value, bool)
        or not isinstance(value, (int, float))
        or not math.isfinite(value)
    ):
        return None
    try:
        return (
            datetime.fromtimestamp(value, tz=timezone.utc)
            .isoformat()
            .replace('+00:00', 'Z')
        )
    except (ValueError, OverflowError, OSError):
        return None


def _metric_catalog(metadata):
    metrics = []
    for key, metric in sorted(metadata.get('metrics', {}).items()):
        if _code(key) is None or not isinstance(metric, dict):
            continue
        if metric.get('status') in ('unsupported', 'not_applicable'):
            continue
        unit = metric.get('unit')
        if not isinstance(unit, str) or not re.fullmatch(
            r'[A-Za-z0-9_/% .()-]{1,64}', unit
        ):
            unit = None
        coverage = metric.get('coverage')
        detail = {
            'key': key,
            'kind': _code(metric.get('kind')),
            'unit': unit,
            'measurement_scope': _code(metric.get('measurement_scope')),
            'status': _code(metric.get('status')),
            'reason': _code(metric.get('reason')),
            'supported_statistics': [
                statistic
                for statistic in metric.get('supported_statistics', [])
                if statistic in ('avg', 'p50', 'p95', 'p99', 'current', 'rate')
            ],
            'coverage': (
                {
                    name: coverage.get(name)
                    for name in (
                        'expected_instances',
                        'observed_instances',
                        'partial',
                    )
                }
                if isinstance(coverage, dict)
                else None
            ),
        }
        metrics.append(
            {key: value for key, value in detail.items() if value is not None}
        )
    return metrics


def _unavailable_metrics(metadata):
    groups = {'unsupported': [], 'not_applicable': []}
    for key, metric in sorted(metadata.get('metrics', {}).items()):
        if _code(key) is None or not isinstance(metric, dict):
            continue
        status = metric.get('status')
        if status in groups:
            entry = {'key': key}
            reason = _code(metric.get('reason'))
            if reason is not None:
                entry['reason'] = reason
            groups[status].append(entry)
    return {key: value for key, value in groups.items() if value}


async def manifest_status(session, archive, revision=None):
    """Return safe publication metadata without exposing private object paths."""
    number = archive.revision if revision is None else revision
    if number < 1 or archive.state in _UNAVAILABLE or archive.delete_requested:
        return {'state': 'unavailable', 'archive_revision': number or None}
    publication = await session.exec(
        select(Revision.id).where(
            Revision.archive_id == archive.id, Revision.revision == number
        )
    )
    if publication.first() is None:
        return {'state': 'unavailable', 'archive_revision': number}
    result = await session.exec(
        select(Chunk).where(
            Chunk.archive_id == archive.id,
            Chunk.scope == f'manifest:{number}',
            Chunk.profile_id == archive.profile_id,
            Chunk.published.is_(True),
        )
    )
    chunk = result.first()
    if chunk is not None:
        return {
            'state': 'ready',
            'archive_revision': number,
            'filename': chunk.object_key.rsplit('/', 1)[-1],
            'size_bytes': chunk.size_bytes,
            'sha256': chunk.sha256,
            'checksum_scope': 'utf8_json_bytes',
        }
    job = archive.plan.get('manifest_jobs', {}).get(str(number), {})
    return {
        'state': job.get('state', 'pending'),
        'archive_revision': number,
        'last_error': _code(job.get('last_error')),
    }


class ArchiveManifestWriter:
    """Backfill bounded manifest batches without changing capture publications."""

    def __init__(self, config, session_factory=None, store_factory=None):
        self.config = config
        self.sessions = session_factory or async_session
        self.stores = store_factory or get_artifact_store
        self.worker_id = str(uuid4())
        self._cursor = None

    async def run(self):
        """Keep optional sidecar writes out of the ongoing capture loop."""
        while True:
            try:
                await self.tick()
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.warning('Benchmark archive manifest scheduler is pending')
            await asyncio.sleep(60)

    async def tick(self, limit=10):
        latest = (
            select(Revision)
            .join(Archive, Archive.id == Revision.archive_id)
            .where(
                Archive.delete_requested.is_(False),
                Archive.state.in_(_TERMINAL),
                Revision.revision == Archive.revision,
            )
            .order_by(Revision.created_at.desc(), Revision.id.desc())
            .limit(20)
        )
        async with self.sessions() as session:
            latest_revisions = (await session.exec(latest)).all()
        attempts = 0
        for revision in latest_revisions:
            if await self.ensure(revision.id):
                attempts += 1
            if attempts >= min(2, limit):
                break
        if attempts >= limit:
            return
        query = (
            select(Revision)
            .join(Archive, Archive.id == Revision.archive_id)
            .where(Archive.delete_requested.is_(False), Archive.state.in_(_TERMINAL))
            .order_by(Revision.created_at.desc(), Revision.id.desc())
        )
        if self._cursor is not None:
            created, identity = self._cursor
            query = query.where(
                or_(
                    Revision.created_at < created,
                    (Revision.created_at == created) & (Revision.id < identity),
                )
            )
        async with self.sessions() as session:
            revisions = (await session.exec(query.limit(100))).all()
        if not revisions:
            self._cursor = None
            return
        for revision in revisions:
            self._cursor = (revision.created_at, revision.id)
            if await self.ensure(revision.id):
                attempts += 1
            if attempts >= limit:
                break

    def _fence(self, archive, token):
        return (
            Archive.id == archive.id,
            Archive.lease_owner == self.worker_id,
            Archive.fencing_token == token,
            Archive.lease_until > time.time(),
            Archive.delete_requested.is_(False),
            Archive.state.in_(_TERMINAL),
        )

    async def _claim(self, archive_id):
        async with self.sessions() as session:
            result = await session.exec(
                update(Archive)
                .where(
                    Archive.id == archive_id,
                    Archive.lease_until <= time.time(),
                    Archive.delete_requested.is_(False),
                    Archive.state.in_(_TERMINAL),
                )
                .values(
                    lease_owner=self.worker_id,
                    lease_until=time.time() + _LEASE_SECONDS,
                    fencing_token=Archive.fencing_token + 1,
                )
            )
            await session.commit()
            return (
                await session.get(Archive, archive_id) if result.rowcount == 1 else None
            )

    async def _renew(self, archive):
        while True:
            await asyncio.sleep(30)
            async with self.sessions() as session:
                result = await session.exec(
                    update(Archive)
                    .where(*self._fence(archive, archive.fencing_token))
                    .values(lease_until=time.time() + _LEASE_SECONDS)
                )
                await session.commit()
                if result.rowcount != 1:
                    return

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

    async def ensure(self, revision_id, force=False):
        """Create one optional directory; capture failures remain independent."""
        async with self.sessions() as session:
            revision = await session.get(Revision, revision_id)
            archive = (
                await session.get(Archive, revision.archive_id) if revision else None
            )
            if (
                archive is None
                or archive.state not in _TERMINAL
                or archive.delete_requested
            ):
                return False
            status = await manifest_status(session, archive, revision.revision)
            if status['state'] == 'ready':
                return False
            job = archive.plan.get('manifest_jobs', {}).get(str(revision.revision), {})
            if not force and job.get('retry_at', 0) > time.time():
                return False
        archive = await self._claim(archive.id)
        if archive is None:
            return False
        heartbeat = asyncio.create_task(self._renew(archive))
        try:
            await self._generate(archive, revision)
        except asyncio.CancelledError:
            raise
        except Exception as error:
            reason = (
                error.reason
                if isinstance(error, ArtifactStorageError)
                else 'manifest_unavailable'
            )
            await self._failed(archive, revision.revision, reason)
        finally:
            heartbeat.cancel()
            await asyncio.gather(heartbeat, return_exceptions=True)
            await self._release(archive)
        return True

    async def _inputs(self, archive, revision):
        scopes = {}
        async with self.sessions() as session:
            for key, identity in sorted(revision.scopes.items()):
                if not re.fullmatch(
                    r'(run|phase:[A-Za-z0-9:_-]{1,100})/(runtime|resources)/(all|prefill|decode|router)/(aggregate|instances)',
                    key,
                ):
                    raise ArtifactStorageError('manifest_scope_invalid')
                if isinstance(identity, dict):
                    scopes[key] = identity
                else:
                    scope = await session.get(Scope, identity)
                    if scope is None or scope.archive_id != archive.id:
                        raise ArtifactStorageError('manifest_index_incomplete')
                    scopes[key] = scope.data
            ids = {
                identity
                for entry in scopes.values()
                for identity in entry.get('objects', [])
            }
            chunks = (
                (
                    await session.exec(
                        select(Chunk).where(
                            Chunk.archive_id == archive.id,
                            Chunk.id.in_(ids),
                            Chunk.published.is_(True),
                        )
                    )
                ).all()
                if ids
                else []
            )
        if len(chunks) != len(ids):
            raise ArtifactStorageError('manifest_index_incomplete')
        return scopes, {chunk.id: chunk for chunk in chunks}

    async def _save_plan(self, session, archive, plan):
        result = await session.exec(
            update(Archive)
            .where(*self._fence(archive, archive.fencing_token))
            .values(plan=deepcopy(plan))
        )
        if result.rowcount != 1:
            raise ArtifactStorageError('manifest_lease_lost')

    async def _freeze_sizes(self, archive, revision, chunks):
        plan = deepcopy(archive.plan)
        job = plan.setdefault('manifest_jobs', {}).setdefault(
            str(revision.revision), {}
        )
        sizes = job.setdefault(
            'object_sizes',
            {identity: chunk.size_bytes for identity, chunk in chunks.items()},
        )
        if set(sizes) != set(chunks):
            raise ArtifactStorageError('manifest_index_changed')
        job.update(schema_version=1, state='pending', last_error=None)
        async with self.sessions() as session:
            await self._save_plan(session, archive, plan)
            await session.commit()
        archive.plan = plan
        return sizes

    @staticmethod
    def _document(archive, revision, scopes, chunks, sizes):
        items = []
        for key, entry in sorted(scopes.items()):
            interval, kind, role, view = key.split('/')
            metadata = entry.get('metadata', {})
            files = []
            for identity in entry.get('objects', []):
                chunk = chunks[identity]
                files.append(
                    {
                        'object_id': chunk.id,
                        'filename': chunk.object_key.rsplit('/', 1)[-1],
                        'object_key': _relative_key(chunk.object_key),
                        'format': 'json.gz',
                        'start': chunk.start,
                        'end': chunk.end,
                        'start_utc': _utc(chunk.start),
                        'end_utc': _utc(chunk.end),
                        'stored_bytes': sizes[identity],
                        'uncompressed_bytes': chunk.uncompressed_size_bytes,
                        'sha256': chunk.sha256,
                        'checksum_scope': 'uncompressed_json_bytes',
                    }
                )
            items.append(
                {
                    'scope': key,
                    'phase_id': interval[6:] if interval.startswith('phase:') else None,
                    'kind': kind,
                    'role': role,
                    'view': view,
                    'window': _window(metadata),
                    'sample_step_seconds': metadata.get('step'),
                    'capture_incomplete': bool(entry.get('partial')),
                    'sampling_incomplete': bool(
                        entry.get('data_partial', entry.get('partial'))
                    ),
                    'metrics': _metric_catalog(metadata),
                    'unavailable_metrics': _unavailable_metrics(metadata),
                    'objects': files,
                }
            )
        return {
            'format': 'gpustack-benchmark-monitoring-manifest',
            'schema_version': 1,
            'benchmark_id': archive.benchmark_id,
            'run_id': archive.run_id,
            'archive_revision': revision.revision,
            'revision_id': revision.id,
            'revision_created_at': revision.created_at,
            'revision_created_at_utc': _utc(revision.created_at),
            'time_unit': 'unix_seconds',
            'revision_state': revision.state,
            'capture_complete': revision.state in ('ready', 'no_data'),
            'data_coverage': revision.data_coverage,
            'captured_until': revision.captured_until,
            'captured_until_utc': _utc(revision.captured_until),
            'run_window': _window(revision.context.get('run_window')),
            'publication_note': (
                'This directory describes an already published immutable revision. '
                'The shared database determines the active revision and whether '
                'this manifest object is committed. Upload intents are not active '
                'publications.'
            ),
            'size_note': (
                'Stored byte sizes are frozen when this manifest is first '
                'prepared; a later profile migration can use another compression '
                'encoding.'
            ),
            'object_count': len(chunks),
            'total_stored_bytes': sum(sizes.values()),
            'total_uncompressed_bytes': sum(
                chunk.uncompressed_size_bytes for chunk in chunks.values()
            ),
            'scopes': items,
        }

    async def _generate(self, archive, revision):
        scopes, chunks = await self._inputs(archive, revision)
        sizes = await self._freeze_sizes(archive, revision, chunks)
        document = self._document(archive, revision, scopes, chunks, sizes)
        raw = await asyncio.to_thread(
            lambda: json.dumps(
                document, ensure_ascii=False, sort_keys=True, indent=2, allow_nan=False
            ).encode()
        )
        store = self.stores(self.config, profile_id=archive.profile_id)
        if store is None or store.profile_fingerprint != archive.profile_fingerprint:
            raise ArtifactStorageError('profile_binding_changed')
        identity = str(
            uuid5(
                NAMESPACE_URL,
                f'gpustack:manifest:{revision.id}:{archive.profile_fingerprint}',
            )
        )
        key = (
            f'installations/{archive.plan["installation_id"]}/'
            f'owners/{archive.owner_principal_id or "global"}/'
            f'benchmarks/{archive.benchmark_id}/runs/{archive.run_id}/'
            f'monitoring/v1/manifests/revision-{revision.revision:06d}.json'
        )
        await CapacityManager(self.config, session_factory=self.sessions).usage()
        async with self.sessions() as session:
            existing = await session.get(Chunk, identity)
            created = existing is None
            if existing is None:
                session.add(
                    Chunk(
                        id=identity,
                        archive_id=archive.id,
                        scope=f'manifest:{revision.revision}',
                        profile_id=archive.profile_id,
                        object_key=key,
                        sha256=hashlib.sha256(raw).hexdigest(),
                        size_bytes=0,
                        uncompressed_size_bytes=len(raw),
                        created_at=time.time(),
                    )
                )
                await session.commit()
            elif existing.sha256 != hashlib.sha256(raw).hexdigest():
                raise ArtifactStorageError('manifest_input_changed')
        try:
            metadata = await put_managed_json(
                store,
                key,
                document,
                config=self.config,
                archive_id=archive.id,
                session_factory=self.sessions,
                format='json',
            )
        except ArtifactStorageError as error:
            if created and error.reason == 'storage_capacity_exceeded':
                # Rejection precedes S3 I/O, so this brand-new intent cannot
                # describe an object. Unknown write outcomes retain their row.
                async with self.sessions() as session:
                    await session.exec(
                        delete(Chunk).where(
                            Chunk.id == identity,
                            Chunk.published.is_(False),
                            Chunk.size_bytes == 0,
                        )
                    )
                    await session.commit()
            raise
        plan = deepcopy(archive.plan)
        job = plan['manifest_jobs'][str(revision.revision)]
        job.update(state='ready', last_error=None, attempts=0, retry_at=0)
        async with self.sessions() as session:
            await self._save_plan(session, archive, plan)
            await session.exec(
                update(Chunk)
                .where(Chunk.id == identity)
                .values(published=True, size_bytes=metadata.size_bytes)
            )
            await session.commit()
        archive.plan = plan

    async def _failed(self, archive, number, reason):
        plan = deepcopy(archive.plan)
        job = plan.setdefault('manifest_jobs', {}).setdefault(str(number), {})
        job.update(
            state='failed',
            last_error=_code(reason) or 'manifest_unavailable',
            attempts=job.get('attempts', 0) + 1,
            retry_at=time.time() + 60,
        )
        async with self.sessions() as session:
            await self._save_plan(session, archive, plan)
            await session.commit()
        archive.plan = plan
        logger.warning('Benchmark archive manifest generation is pending')
