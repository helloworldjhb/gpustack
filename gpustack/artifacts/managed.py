"""Database-accounted object writes and confirmed object deletion."""

import asyncio
import gzip
import json
from typing import Any, Dict, Optional

from gpustack.artifacts.capacity import CapacityManager
from gpustack.artifacts.store import ArtifactMetadata, ArtifactStore


def _encoded_size(value, format='json.gz'):
    raw = json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(',', ':') if format == 'json.gz' else None,
        indent=2 if format == 'json' else None,
        allow_nan=False,
    ).encode()
    return len(gzip.compress(raw, mtime=0)) if format == 'json.gz' else len(raw)


async def put_managed_json(
    store: ArtifactStore,
    key: str,
    value: Dict[str, Any],
    *,
    config=None,
    archive_id: Optional[str] = None,
    session_factory=None,
    format: str = 'json.gz',
) -> ArtifactMetadata:
    """Reserve before I/O, retaining the charge when upload outcome is unknown.

    The domain records its cleanup intent before calling this method. Even a
    cancellation or timeout can leave an S3 object, so only a confirmed delete
    may release its durable reservation.
    """
    capacity = CapacityManager(config, session_factory=session_factory)
    prepared = None
    if isinstance(store, ArtifactStore):
        prepared = await store.prepare_json(value, format=format)
        size = len(prepared.body)
    else:
        size = await asyncio.to_thread(_encoded_size, value, format)
    await capacity.reserve(store.profile_id, key, size, archive_id=archive_id)
    metadata = (
        await store.put_prepared(key, prepared)
        if prepared is not None
        else (
            await store.put_json(key, value)
            if format == 'json.gz'
            else await store.put_json(key, value, format=format)
        )
    )
    await capacity.commit(store.profile_id, key, metadata.size_bytes)
    return metadata


async def delete_managed_object(
    store: ArtifactStore, key: str, *, config=None, session_factory=None
) -> None:
    """Release capacity only after the storage service confirms deletion."""
    await store.delete(key)
    await CapacityManager(config, session_factory=session_factory).release(
        store.profile_id, key
    )
