"""Independent regressions for the capacity accounting S3 boundary."""

import gzip
from unittest.mock import patch

import pytest

from gpustack.artifacts import get_artifact_store
from gpustack.artifacts.capacity import CapacityManager
from gpustack.artifacts.store import ArtifactStorageError
from gpustack.schemas.benchmark_monitoring import BenchmarkModelMetrics
from gpustack.server import benchmark_monitoring_archiver as archiver
from sqlmodel.ext.asyncio.session import AsyncSession

from tests.artifacts.test_managed_capacity import database
from tests.artifacts.test_store import MemoryS3, config
from tests.monitoring.test_benchmark_archive import payload, row


@pytest.mark.asyncio
async def test_immutable_retry_accounts_existing_encoding_size(tmp_path):
    client = MemoryS3()
    with patch('gpustack.artifacts.store.boto3.client', return_value=client):
        store = get_artifact_store(config(tmp_path))
        payload = {'series': [0, None, 10] * 100, 'label': '历史'}
        original = await store.put_json('existing', payload)
        stored = client.objects['private/existing']
        raw = gzip.decompress(stored['Body'])
        stored['Body'] = gzip.compress(raw, compresslevel=0, mtime=0)
        expected = len(stored['Body'])
        assert expected != original.size_bytes

        prepared = await store.prepare_json(payload)
        metadata = await store.put_prepared('existing', prepared)

        assert metadata.size_bytes == expected
        assert metadata.sha256 == original.sha256
        assert await store.get_json('existing') == payload


@pytest.mark.asyncio
async def test_mid_batch_capacity_rejection_resumes_with_one_batch_of_space(
    tmp_path, monkeypatch
):
    engine = await database()

    def sessions():
        return AsyncSession(engine, expire_on_commit=False)

    monkeypatch.setattr(archiver, 'async_session', sessions)
    settings = config(tmp_path)
    record = row()
    async with sessions() as session:
        session.add(record)
        await session.commit()
    sdk = MemoryS3()
    try:
        with patch('gpustack.artifacts.store.boto3.client', return_value=sdk):
            store = get_artifact_store(settings)
            response = BenchmarkModelMetrics.model_validate(payload())
            outputs = {
                'run/runtime/all/aggregate': response,
                'run/runtime/all/instances': response,
            }
            size = len(
                (await store.prepare_json(response.model_dump(mode='json'))).body
            )
            capacity = CapacityManager(settings, sessions)
            await capacity.set_limit(size * 2 - 1)
            capture = archiver.BenchmarkMonitoringArchiver(settings)
            record.fencing_token = await capture.claim(record.id)
            with pytest.raises(ArtifactStorageError, match='storage_capacity_exceeded'):
                await capture._store_outputs(record, store, outputs)

            # The new allowance can store both scopes. A failed prefix must
            # not force the administrator to fund a duplicate third object.
            await capacity.set_limit(size * 2)
            result, _ = await capture._store_outputs(record, store, outputs)
            assert len(result['_chunks']) == 2
            assert (await capacity.usage())['used_bytes'] == size * 2
    finally:
        await engine.dispose()
