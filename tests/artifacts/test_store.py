import asyncio
import gzip
import hashlib
import io
import json
import threading
from types import SimpleNamespace
from unittest.mock import patch

import pytest
from botocore.exceptions import ClientError

from gpustack.artifacts import ArtifactStorageError, get_artifact_store


class MemoryS3:
    def __init__(self):
        self.objects = {}
        self.calls = []
        self.thread_ids = []

    def head_object(self, **kwargs):
        self.thread_ids.append(threading.get_ident())
        key = kwargs['Key']
        if key not in self.objects:
            raise ClientError({'Error': {'Code': '404'}}, 'HeadObject')
        item = self.objects[key]
        return {'Metadata': item['Metadata'], 'ContentLength': len(item['Body'])}

    def put_object(self, **kwargs):
        self.calls.append(('put', kwargs))
        self.thread_ids.append(threading.get_ident())
        if kwargs['Key'] in self.objects:
            raise ClientError({'Error': {'Code': 'PreconditionFailed'}}, 'PutObject')
        self.objects[kwargs['Key']] = kwargs
        return {'ETag': 'opaque-etag'}

    def get_object(self, **kwargs):
        self.thread_ids.append(threading.get_ident())
        item = self.objects[kwargs['Key']]
        return {
            'Body': io.BytesIO(item['Body']),
            'ContentLength': len(item['Body']),
            'Metadata': item['Metadata'],
        }

    def delete_object(self, **kwargs):
        self.thread_ids.append(threading.get_ident())
        self.calls.append(('delete', kwargs))
        self.objects.pop(kwargs['Key'], None)

    def head_bucket(self, **kwargs):
        self.calls.append(('head_bucket', kwargs))


def config(tmp_path, **kwargs):
    return SimpleNamespace(
        data_dir=str(tmp_path),
        artifact_storage_mode='external',
        artifact_storage_profile_id='test',
        artifact_storage_bucket='archives',
        artifact_storage_prefix='private',
        **kwargs,
    )


@pytest.fixture
def client():
    return MemoryS3()


@pytest.mark.asyncio
async def test_round_trip_metadata_checksum_compression_and_offloading(
    tmp_path, client
):
    with patch('gpustack.artifacts.store.boto3.client', return_value=client):
        store = get_artifact_store(config(tmp_path))
        payload = {'series': [0, None, 2.5], 'name': '测量'}
        metadata = await store.put_json('runs/one.json.gz', payload)
        raw = json.dumps(
            payload,
            ensure_ascii=False,
            sort_keys=True,
            separators=(',', ':'),
            allow_nan=False,
        ).encode()
        assert metadata.sha256 == hashlib.sha256(raw).hexdigest()
        assert metadata.uncompressed_size_bytes == len(raw)
        assert metadata.size_bytes == len(
            client.objects['private/runs/one.json.gz']['Body']
        )
        assert (
            gzip.decompress(client.objects['private/runs/one.json.gz']['Body']) == raw
        )
        assert metadata.key == 'runs/one.json.gz'
        assert metadata.profile_id == 'test'
        assert (
            await store.get_json(metadata.key, expected_sha256=metadata.sha256)
            == payload
        )
        assert all(tid != threading.get_ident() for tid in client.thread_ids)
        await store.delete(metadata.key)
        assert not client.objects


@pytest.mark.asyncio
async def test_immutable_retry_and_conflicting_write(tmp_path, client):
    with patch('gpustack.artifacts.store.boto3.client', return_value=client):
        store = get_artifact_store(config(tmp_path))
        first = await store.put_json('run/data', {'value': 1})
        second = await store.put_json('run/data', {'value': 1})
        assert second.sha256 == first.sha256
        with pytest.raises(ArtifactStorageError, match='object_conflict'):
            await store.put_json('run/data', {'value': 2})
        assert all(call[1]['IfNoneMatch'] == '*' for call in client.calls)
        assert await store.get_json('run/data', expected_sha256=first.sha256) == {
            'value': 1
        }


@pytest.mark.asyncio
async def test_checksum_and_decoded_size_are_enforced(tmp_path, client):
    with patch('gpustack.artifacts.store.boto3.client', return_value=client):
        store = get_artifact_store(
            config(tmp_path, artifact_storage_max_object_bytes=80)
        )
        metadata = await store.put_json('good', {'x': 'short'})
        with pytest.raises(ArtifactStorageError, match='checksum_mismatch'):
            await store.get_json('good', expected_sha256='0' * 64)
        client.objects['private/good']['Body'] = gzip.compress(
            json.dumps({'x': 'a' * 1000}).encode()
        )
        with pytest.raises(ArtifactStorageError, match='object_too_large'):
            await store.get_json('good')
        with pytest.raises(ArtifactStorageError, match='object_too_large'):
            await store.put_json('large', {'x': 'a' * 1000})
        assert metadata.sha256


@pytest.mark.asyncio
@pytest.mark.parametrize(
    'key', ['', '/other', '../other', 'x/../other', 'x//other', 'x\\other']
)
async def test_keys_cannot_escape_prefix(tmp_path, key):
    store = get_artifact_store(config(tmp_path))
    with pytest.raises(ArtifactStorageError, match='invalid_key'):
        await store.get_json(key)


def test_disabled_and_historical_profiles(tmp_path):
    assert get_artifact_store(SimpleNamespace(artifact_storage_mode='disabled')) is None
    cfg = config(
        tmp_path,
        artifact_storage_read_profiles={
            'old': {'bucket': 'old-bucket', 'prefix': 'old'}
        },
    )
    old = get_artifact_store(cfg, profile_id='old')
    assert old.profile_id == 'old'
    assert old.profile_fingerprint != get_artifact_store(cfg).profile_fingerprint
    with pytest.raises(ArtifactStorageError, match='profile_unavailable'):
        get_artifact_store(cfg, profile_id='missing')
    cfg.artifact_storage_bucket = 'different'
    with pytest.raises(ArtifactStorageError, match='profile_binding_changed'):
        get_artifact_store(cfg)


@pytest.mark.asyncio
async def test_safe_error_never_exposes_sdk_message(tmp_path, client):
    with patch('gpustack.artifacts.store.boto3.client', return_value=client):
        store = get_artifact_store(config(tmp_path))
        client.head_bucket = lambda **kwargs: (_ for _ in ()).throw(
            ClientError(
                {
                    'Error': {
                        'Code': 'AccessDenied',
                        'Message': 'secret-token https://user:password@host',
                    }
                },
                'HeadBucket',
            )
        )
        with pytest.raises(ArtifactStorageError) as caught:
            await store.check()
        assert caught.value.reason == 'access_denied'
        assert 'secret-token' not in str(caught.value)
        assert 'password' not in str(caught.value)


@pytest.mark.asyncio
async def test_health_is_read_only_by_default(tmp_path, client):
    with patch('gpustack.artifacts.store.boto3.client', return_value=client):
        store = get_artifact_store(config(tmp_path))
        assert (await store.health())['available'] is True
        assert client.calls == [('head_bucket', {'Bucket': 'archives'})]


@pytest.mark.asyncio
async def test_sdk_configuration_and_default_credential_chain(tmp_path, client):
    with patch('gpustack.artifacts.store.boto3.client', return_value=client) as sdk:
        store = get_artifact_store(
            config(
                tmp_path,
                artifact_storage_endpoint='https://s3.example.test',
                artifact_storage_ca_file='/trusted/ca.pem',
                artifact_storage_addressing_style='path',
            )
        )
        await store.check()
        kwargs = sdk.call_args.kwargs
        assert 'aws_access_key_id' not in kwargs
        assert 'aws_secret_access_key' not in kwargs
        assert kwargs['verify'] == '/trusted/ca.pem'
        assert kwargs['endpoint_url'] == 'https://s3.example.test'
        assert kwargs['config'].signature_version == 's3v4'
        assert kwargs['config'].retries['total_max_attempts'] == 3
        assert kwargs['config'].s3['addressing_style'] == 'path'


@pytest.mark.asyncio
async def test_temporary_credentials_are_passed_to_sdk(tmp_path, client):
    with patch('gpustack.artifacts.store.boto3.client', return_value=client) as sdk:
        store = get_artifact_store(
            config(
                tmp_path,
                artifact_storage_access_key_id='key',
                artifact_storage_secret_access_key='secret',
                artifact_storage_session_token='session',
            )
        )
        await store.check()
        assert sdk.call_args.kwargs['aws_session_token'] == 'session'
        assert sdk.call_args.kwargs['aws_access_key_id'] == 'key'


@pytest.mark.asyncio
async def test_factory_reuses_pool_and_concurrency_limit_within_event_loop(
    tmp_path, client
):
    cfg = config(tmp_path, artifact_storage_max_concurrency=1)
    with patch('gpustack.artifacts.store.boto3.client', return_value=client):
        first = get_artifact_store(cfg)
        second = get_artifact_store(cfg)
        assert first is second


@pytest.mark.asyncio
@pytest.mark.parametrize(
    'value', [{'invalid': float('nan')}, {'invalid': object()}, ['not-a-document']]
)
async def test_invalid_json_is_rejected_before_network(tmp_path, value):
    store = get_artifact_store(config(tmp_path))
    with pytest.raises(ArtifactStorageError, match='invalid_json'):
        await store.put_json('invalid', value)


@pytest.mark.asyncio
async def test_corrupt_gzip_closes_response_and_returns_safe_reason(tmp_path, client):
    with patch('gpustack.artifacts.store.boto3.client', return_value=client):
        store = get_artifact_store(config(tmp_path))
        await store.put_json('broken', {'value': 1})
        client.objects['private/broken']['Body'] = b'not-gzip'
        with pytest.raises(ArtifactStorageError, match='invalid_json'):
            await store.get_json('broken')


@pytest.mark.asyncio
async def test_concurrency_limit_remains_held_after_cancellation(tmp_path, client):
    entered = threading.Event()
    release = threading.Event()
    calls = []

    def slow_head(**kwargs):
        calls.append('entered')
        entered.set()
        assert release.wait(5)

    client.head_bucket = slow_head
    with patch('gpustack.artifacts.store.boto3.client', return_value=client):
        store = get_artifact_store(config(tmp_path, artifact_storage_max_concurrency=1))
        first = asyncio.create_task(store.check())
        assert await asyncio.to_thread(entered.wait, 2)
        first.cancel()
        second = asyncio.create_task(store.check())
        await asyncio.sleep(0.02)
        assert calls == ['entered']
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await first
        await second
        assert calls == ['entered', 'entered']


@pytest.mark.asyncio
async def test_racing_put_does_not_overwrite_conflicting_object(tmp_path, client):
    original = client.put_object

    def racing_put(**kwargs):
        conflicting = dict(kwargs)
        conflicting['Body'] = gzip.compress(b'{"other":1}')
        conflicting['Metadata'] = {'sha256': hashlib.sha256(b'{"other":1}').hexdigest()}
        original(**conflicting)
        return original(**kwargs)

    client.put_object = racing_put
    with patch('gpustack.artifacts.store.boto3.client', return_value=client):
        store = get_artifact_store(config(tmp_path))
        with pytest.raises(ArtifactStorageError, match='object_conflict'):
            await store.put_json('raced', {'value': 1})
        assert (
            gzip.decompress(client.objects['private/raced']['Body']) == b'{"other":1}'
        )


@pytest.mark.asyncio
async def test_first_write_does_not_require_list_bucket_permission(tmp_path, client):
    def denied_missing_head(**kwargs):
        raise ClientError({'Error': {'Code': 'AccessDenied'}}, 'HeadObject')

    client.head_object = denied_missing_head
    with patch('gpustack.artifacts.store.boto3.client', return_value=client):
        store = get_artifact_store(config(tmp_path))
        metadata = await store.put_json('first', {'value': 1})
        assert await store.get_json(metadata.key, expected_sha256=metadata.sha256) == {
            'value': 1
        }


@pytest.mark.asyncio
@pytest.mark.parametrize('conditional_writes', [True, False])
async def test_portable_s3_rejecting_condition_supports_unique_object_writes(
    tmp_path, client, conditional_writes
):
    original = client.put_object

    def portable_put(**kwargs):
        if 'IfNoneMatch' in kwargs:
            raise ClientError(
                {
                    'Error': {
                        'Code': 'NotImplemented',
                        'Message': 'If-None-Match is not supported',
                    }
                },
                'PutObject',
            )
        return original(**kwargs)

    client.put_object = portable_put
    with patch('gpustack.artifacts.store.boto3.client', return_value=client):
        store = get_artifact_store(
            config(tmp_path, artifact_storage_conditional_writes=conditional_writes)
        )
        first = await store.put_json('uuid/data', {'value': '测量'})
        second = await store.put_json('uuid/data', {'value': '测量'})
        assert first.sha256 == second.sha256
        assert await store.get_json(first.key, first.sha256) == {'value': '测量'}
        with pytest.raises(ArtifactStorageError, match='object_conflict'):
            await store.put_json(first.key, {'different': True})
        assert 'IfNoneMatch' not in client.calls[0][1]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    'code', ['AccessDenied', '403', '409', 'ConditionalRequestConflict']
)
async def test_permission_and_conflict_failures_do_not_fallback(tmp_path, client, code):
    def rejected_put(**kwargs):
        client.calls.append(('put', kwargs))
        raise ClientError({'Error': {'Code': code}}, 'PutObject')

    client.put_object = rejected_put
    with patch('gpustack.artifacts.store.boto3.client', return_value=client):
        store = get_artifact_store(config(tmp_path))
        with pytest.raises(ArtifactStorageError):
            await store.put_json('new', {'value': 1})
        assert len(client.calls) == 1
        assert client.calls[0][1]['IfNoneMatch'] == '*'
