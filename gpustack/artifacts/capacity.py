"""Installation-wide object accounting with transactional write reservations.

The shared database serializes reservations across Server processes. A reservation
is retained until an upload is confirmed or deletion is acknowledged, including
when the result of an interrupted S3 request is unknown.
"""

import hashlib
from typing import Optional

from sqlalchemy import update
from sqlalchemy.exc import IntegrityError
from sqlmodel import select

from gpustack.artifacts.store import ArtifactStorageError
from gpustack.config.config import get_global_config
from gpustack.schemas.artifact_storage import (
    MAX_CAPACITY_BYTES,
    ArtifactStorageCapacity as Capacity,
    ArtifactStorageObject as Object,
)
from gpustack.schemas.benchmark_archive import (
    BenchmarkMonitoringArchive as Archive,
    BenchmarkMonitoringChunk as Chunk,
)
from gpustack.server.db import async_session


def _object_id(profile_id, object_key):
    return hashlib.sha256((profile_id + '\x00' + object_key).encode()).hexdigest()


def _size(value):
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ArtifactStorageError('invalid_capacity_bytes')
    return value


def _upper_bound(uncompressed):
    size = max(0, int(uncompressed or 0))
    return size + size // 1000 + 65536


def _usage(capacity):
    total = capacity.used_bytes + capacity.reserved_bytes
    maximum = capacity.max_bytes
    percent = total * 100 / maximum if maximum else None
    return {
        'max_bytes': maximum,
        'unlimited': maximum == 0,
        'used_bytes': capacity.used_bytes,
        'reserved_bytes': capacity.reserved_bytes,
        'total_bytes': total,
        'remaining_bytes': max(0, maximum - total) if maximum else None,
        'usage_percent': round(percent, 2) if percent is not None else None,
        'state': (
            'unlimited'
            if not maximum
            else (
                'full' if total >= maximum else 'warning' if percent >= 80 else 'normal'
            )
        ),
        'warning_threshold_percent': 80,
    }


class CapacityManager:
    """Reserve compressed object bytes against one shared installation policy."""

    def __init__(self, config=None, session_factory=None):
        self.config = config if config is not None else get_global_config()
        self.sessions = session_factory or async_session

    async def _ensure_row(self):
        async with self.sessions() as session:
            capacity = await session.get(Capacity, 1)
            if capacity is not None and capacity.initialized:
                return
            if capacity is None:
                session.add(Capacity(id=1))
                try:
                    await session.commit()
                except IntegrityError:
                    await session.rollback()
        # Initialization is durable even when the first requested allocation is
        # too large. Failed writes cannot reset the installation's seeded policy.
        async with self.sessions() as session:
            await self._lock(session)
            await session.commit()

    async def _lock(self, session):
        # Updating one existing row obtains a write lock on every supported
        # dialect, including SQLite tests, without an unsupported FOR UPDATE.
        await session.exec(
            update(Capacity)
            .where(Capacity.id == 1)
            .values(used_bytes=Capacity.used_bytes)
        )
        capacity = await session.get(Capacity, 1)
        if not capacity.initialized:
            await self._bootstrap(session, capacity)
        return capacity

    async def _bootstrap(self, session, capacity):
        capacity.max_bytes = getattr(self.config, 'artifact_storage_max_total_bytes', 0)
        objects = {}
        chunks = list((await session.exec(select(Chunk))).all())
        by_id = {chunk.id: chunk for chunk in chunks}
        for chunk in chunks:
            self._remember(
                objects,
                chunk.profile_id,
                chunk.object_key,
                chunk.archive_id,
                chunk.size_bytes or _upper_bound(chunk.uncompressed_size_bytes),
                'stored' if chunk.size_bytes > 0 else 'reserved',
            )
        archives = (await session.exec(select(Archive))).all()
        for archive in archives:
            if archive.state != 'expired':
                self._remember_migrations(objects, archive, by_id)
        for item in objects.values():
            session.add(item)
            if item.state == 'stored':
                capacity.used_bytes += item.size_bytes
            else:
                capacity.reserved_bytes += item.size_bytes
        capacity.initialized = True
        session.add(capacity)
        await session.flush()

    @staticmethod
    def _remember(objects, profile_id, key, archive_id, size, state):
        identity = _object_id(profile_id, key)
        previous = objects.get(identity)
        if previous is not None:
            if previous.state == 'reserved' and state == 'stored':
                previous.size_bytes, previous.state = size, state
            return
        objects[identity] = Object(
            id=identity,
            archive_id=archive_id,
            profile_id=profile_id,
            object_key=key,
            size_bytes=size,
            state=state,
        )

    def _remember_migrations(self, objects, archive, chunks):
        maximum = getattr(self.config, 'artifact_storage_max_object_bytes', 16777216)
        for migration in archive.plan.get('storage_migrations', []):
            for item in migration.get('objects', []):
                chunk = chunks.get(item.get('chunk_id'))
                for side in ('source', 'target'):
                    profile = migration.get(side + '_profile_id')
                    key = item.get('object_key')
                    if not profile or not key:
                        raise ArtifactStorageError('invalid_migration_history')
                    size = item.get(side + '_size_bytes') or 0
                    if not size and chunk and chunk.profile_id == profile:
                        size = chunk.size_bytes
                    if not size and side == 'target' and item.get('verified'):
                        size = item.get('size_bytes') or 0
                    confirmed = bool(size) and (
                        side == 'source' or bool(item.get('verified'))
                    )
                    if not confirmed:
                        size = _upper_bound(
                            chunk.uncompressed_size_bytes
                            if chunk
                            else item.get('uncompressed_size_bytes') or maximum
                        )
                    self._remember(
                        objects,
                        profile,
                        key,
                        archive.id,
                        size,
                        'stored' if confirmed else 'reserved',
                    )

    async def reserve(
        self,
        profile_id: str,
        object_key: str,
        size_bytes: int,
        archive_id: Optional[str] = None,
    ) -> None:
        """Reserve space before an S3 write; repeat calls for one key are idempotent."""
        size = _size(size_bytes)
        await self._ensure_row()
        async with self.sessions() as session:
            capacity = await self._lock(session)
            identity = _object_id(profile_id, object_key)
            item = await session.get(Object, identity)
            delta = max(0, size - item.size_bytes) if item else size
            if (
                delta
                and capacity.max_bytes
                and capacity.used_bytes + capacity.reserved_bytes + delta
                > capacity.max_bytes
            ):
                raise ArtifactStorageError('storage_capacity_exceeded')
            if item is None:
                session.add(
                    Object(
                        id=identity,
                        archive_id=archive_id,
                        profile_id=profile_id,
                        object_key=object_key,
                        size_bytes=size,
                        state='reserved',
                    )
                )
                capacity.reserved_bytes += size
            elif delta:
                if item.state == 'stored':
                    raise ArtifactStorageError('capacity_object_size_changed')
                item.size_bytes += delta
                capacity.reserved_bytes += delta
                session.add(item)
            session.add(capacity)
            await session.commit()

    async def commit(
        self, profile_id: str, object_key: str, actual_size_bytes: int
    ) -> None:
        """Confirm an uploaded object, releasing any conservative excess reserve."""
        size = _size(actual_size_bytes)
        await self._ensure_row()
        async with self.sessions() as session:
            capacity = await self._lock(session)
            item = await session.get(Object, _object_id(profile_id, object_key))
            if item is None:
                raise ArtifactStorageError('capacity_reservation_missing')
            if item.state == 'stored':
                if item.size_bytes != size:
                    raise ArtifactStorageError('capacity_object_size_changed')
            else:
                # Account every confirmed byte even if a caller underestimated
                # its reservation; never under-report an already stored object.
                capacity.reserved_bytes -= item.size_bytes
                capacity.used_bytes += size
                item.size_bytes, item.state = size, 'stored'
                session.add(item)
            session.add(capacity)
            await session.commit()

    async def release(self, profile_id: str, object_key: str) -> None:
        """Release space only after S3 has acknowledged deletion of this key."""
        await self._ensure_row()
        async with self.sessions() as session:
            capacity = await self._lock(session)
            item = await session.get(Object, _object_id(profile_id, object_key))
            if item is not None:
                if item.state == 'stored':
                    capacity.used_bytes -= item.size_bytes
                else:
                    capacity.reserved_bytes -= item.size_bytes
                await session.delete(item)
                session.add(capacity)
            await session.commit()

    async def usage(self) -> dict:
        """Read shared capacity without connecting to or scanning an S3 bucket."""
        await self._ensure_row()
        async with self.sessions() as session:
            capacity = await self._lock(session)
            result = _usage(capacity)
            await session.commit()
            return result

    async def set_limit(self, max_bytes: int) -> dict:
        """Persist the installation policy; a smaller cap never deletes history."""
        maximum = _size(max_bytes)
        if maximum > MAX_CAPACITY_BYTES:
            raise ArtifactStorageError('invalid_capacity_bytes')
        await self._ensure_row()
        async with self.sessions() as session:
            capacity = await self._lock(session)
            capacity.max_bytes = maximum
            session.add(capacity)
            await session.exec(
                update(Archive)
                .where(
                    Archive.last_error == 'storage_capacity_exceeded',
                    Archive.delete_requested.is_(False),
                    Archive.state.notin_(['expired', 'expiring', 'deleting']),
                )
                .values(retry_at=0)
            )
            result = _usage(capacity)
            await session.commit()
            return result
