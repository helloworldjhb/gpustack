"""S3 I/O never bypasses capacity, and uncertain outcomes retain their charge."""

from types import SimpleNamespace
from unittest.mock import patch

import pytest
from sqlalchemy.ext.asyncio import create_async_engine
from sqlmodel.ext.asyncio.session import AsyncSession

from gpustack.artifacts import ArtifactStorageError, get_artifact_store
from gpustack.artifacts.capacity import CapacityManager
from gpustack.artifacts.managed import delete_managed_object, put_managed_json
from gpustack.schemas.artifact_storage import (
    ArtifactStorageCapacity,
    ArtifactStorageObject,
)
from gpustack.schemas.benchmark_archive import (
    BenchmarkMonitoringArchive,
    BenchmarkMonitoringChunk,
)
from tests.artifacts.test_store import MemoryS3, config


async def database():
    engine = create_async_engine('sqlite+aiosqlite:///:memory:')
    async with engine.begin() as connection:
        await connection.run_sync(
            lambda sync: ArtifactStorageCapacity.metadata.create_all(
                sync,
                tables=[
                    model.__table__
                    for model in (
                        ArtifactStorageCapacity,
                        ArtifactStorageObject,
                        BenchmarkMonitoringArchive,
                        BenchmarkMonitoringChunk,
                    )
                ],
            )
        )
    return engine


@pytest.mark.asyncio
async def test_exact_compressed_cap_blocks_before_upload_and_unlimited_resumes(
    tmp_path,
):
    engine = await database()

    def sessions():
        return AsyncSession(engine, expire_on_commit=False)

    sdk = MemoryS3()
    settings = config(tmp_path)
    try:
        with patch('gpustack.artifacts.store.boto3.client', return_value=sdk):
            store = get_artifact_store(settings)
            capacity = CapacityManager(settings, sessions)
            value = {'series': [0, None, 1], 'name': '历史'}
            prepared = await store.prepare_json(value)
            size = len(prepared.body)
            await capacity.set_limit(size)
            meta = await put_managed_json(
                store, 'first', value, config=settings, session_factory=sessions
            )
            assert meta.size_bytes == size
            assert (await capacity.usage())['used_bytes'] == size
            with pytest.raises(ArtifactStorageError, match='storage_capacity_exceeded'):
                await put_managed_json(
                    store, 'second', value, config=settings, session_factory=sessions
                )
            assert 'private/second' not in sdk.objects
            assert (await capacity.usage())['reserved_bytes'] == 0
            assert await store.get_json('first') == value
            await capacity.set_limit(1)
            assert (await capacity.usage())['state'] == 'full'
            assert await store.get_json('first') == value
            await capacity.set_limit(0)
            await put_managed_json(
                store, 'second', value, config=settings, session_factory=sessions
            )
            assert (await capacity.usage())['used_bytes'] == size * 2
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_unknown_upload_and_failed_delete_remain_charged_until_confirmed(
    tmp_path,
):
    engine = await database()

    def sessions():
        return AsyncSession(engine, expire_on_commit=False)

    settings = config(tmp_path)
    sdk = MemoryS3()
    try:
        with patch('gpustack.artifacts.store.boto3.client', return_value=sdk):
            store = get_artifact_store(settings)
            capacity = CapacityManager(settings, sessions)
            original = sdk.put_object

            def uncertain(**kwargs):
                original(**kwargs)
                raise TimeoutError('lost acknowledgement')

            sdk.put_object = uncertain
            with pytest.raises(ArtifactStorageError):
                await put_managed_json(
                    store, 'uncertain', {'value': 0}, session_factory=sessions
                )
            assert 'private/uncertain' in sdk.objects
            before = await capacity.usage()
            assert before['reserved_bytes'] > 0
            assert before['used_bytes'] == 0

            def failed_delete(**kwargs):
                raise TimeoutError('delete acknowledgement lost')

            sdk.delete_object = failed_delete
            with pytest.raises(ArtifactStorageError):
                await delete_managed_object(
                    store, 'uncertain', session_factory=sessions
                )
            assert await capacity.usage() == before
            sdk.delete_object = MemoryS3.delete_object.__get__(sdk)
            await delete_managed_object(store, 'uncertain', session_factory=sessions)
            await delete_managed_object(store, 'uncertain', session_factory=sessions)
            assert (await capacity.usage())['total_bytes'] == 0
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_retry_of_uncertain_object_does_not_double_charge(tmp_path):
    engine = await database()

    def sessions():
        return AsyncSession(engine, expire_on_commit=False)

    settings = config(tmp_path)
    sdk = MemoryS3()
    try:
        with patch('gpustack.artifacts.store.boto3.client', return_value=sdk):
            store = get_artifact_store(settings)
            capacity = CapacityManager(settings, sessions)
            value = {'series': [0, None]}
            prepared = await store.prepare_json(value)
            await capacity.reserve('test', 'uncertain', len(prepared.body))
            await store.put_prepared('uncertain', prepared)
            await capacity.set_limit(len(prepared.body))
            await put_managed_json(store, 'uncertain', value, session_factory=sessions)
            usage = await capacity.usage()
            assert usage['used_bytes'] == len(prepared.body)
            assert usage['reserved_bytes'] == 0
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_storage_configuration_does_not_override_shared_policy():
    engine = await database()

    def sessions():
        return AsyncSession(engine, expire_on_commit=False)

    try:
        first = CapacityManager(
            SimpleNamespace(artifact_storage_max_total_bytes=123), sessions
        )
        second = CapacityManager(
            SimpleNamespace(artifact_storage_max_total_bytes=999), sessions
        )
        assert (await first.usage())['max_bytes'] == 123
        assert (await second.usage())['max_bytes'] == 123
        await second.set_limit(0)
        assert (await first.usage())['unlimited'] is True
    finally:
        await engine.dispose()
