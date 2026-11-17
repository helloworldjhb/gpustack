"""Bounded, thread-offloaded S3 JSON objects with immutable writes."""

import asyncio
import gzip
import hashlib
import io
import json
import logging
import os
import re
import tempfile
import threading
import zlib
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Optional

import boto3
from botocore.config import Config as BotoConfig
from botocore.exceptions import BotoCoreError, ClientError
from pydantic import ValidationError

from gpustack.artifacts.config import ArtifactStorageProfile

_SDK_CLIENT_LOCK = threading.Lock()


def _quiet_sdk_logs() -> None:
    # SDK DEBUG logs contain signed request headers and session tokens.
    for name in ('boto3', 'botocore', 's3transfer'):
        logging.getLogger(name).setLevel(logging.WARNING)


class ArtifactStorageError(Exception):
    """Storage failure with a safe, stable reason suitable for API responses."""

    def __init__(self, reason: str):
        self.reason = reason
        super().__init__(f'Artifact storage: {reason}')


@dataclass(frozen=True)
class ArtifactMetadata:
    profile_id: str
    key: str
    sha256: str
    size_bytes: int
    uncompressed_size_bytes: int
    etag: Optional[str] = None
    format: str = 'json.gz'


@dataclass(frozen=True)
class PreparedArtifact:
    """An encoded immutable object whose exact storage charge is known."""

    body: bytes
    sha256: str
    uncompressed_size_bytes: int
    format: str = 'json.gz'


def _safe_error(error: Exception) -> ArtifactStorageError:
    code = ''
    if isinstance(error, ClientError):
        code = str(error.response.get('Error', {}).get('Code', ''))
    reason = {
        '404': 'object_missing',
        'NoSuchKey': 'object_missing',
        'NoSuchBucket': 'bucket_missing',
        '403': 'access_denied',
        'AccessDenied': 'access_denied',
        'InvalidAccessKeyId': 'access_denied',
        'SignatureDoesNotMatch': 'access_denied',
        'ExpiredToken': 'credentials_expired',
        'PreconditionFailed': 'object_conflict',
        '412': 'object_conflict',
        'ConditionalRequestConflict': 'object_conflict',
    }.get(code, 'storage_unavailable')
    return ArtifactStorageError(reason)


def _relative_key(key: str) -> str:
    if (
        not isinstance(key, str)
        or not key
        or len(key.encode()) > 900
        or '\\' in key
        or any(ord(char) < 32 for char in key)
        or any(part in ('', '.', '..') for part in key.split('/'))
    ):
        raise ArtifactStorageError('invalid_key')
    return key


def _unsupported_condition(error: ClientError) -> bool:
    detail = error.response.get('Error', {})
    code = str(detail.get('Code', ''))
    if code in ('501', 'NotImplemented', 'UnsupportedHeader', 'UnsupportedOperation'):
        return True
    if code not in ('InvalidRequest', 'InvalidArgument'):
        return False
    message = str(detail.get('Message', '')).lower().replace('-', '')
    return 'ifnonematch' in message and any(
        text in message for text in ('unsupported', 'not supported', 'not implemented')
    )


class ArtifactStore:
    """Store independently compressed JSON documents through standard S3 APIs.

    Identical retries are allowed; conflicting content is rejected. Conditional
    writes protect concurrent writers when supported. Portable S3 requires unique
    object keys and fenced publication by the domain to prevent same-key races.
    """

    def __init__(self, profile_id: str, profile: ArtifactStorageProfile):
        self.profile_id = profile_id
        self._profile = profile
        self._client = None
        self._conditional_writes = profile.conditional_writes
        self._bound_directories = set()
        self._semaphore = asyncio.Semaphore(profile.max_concurrency)
        self.profile_fingerprint = hashlib.sha256(
            json.dumps(
                [profile.endpoint, profile.bucket, profile.prefix, profile.region],
                separators=(',', ':'),
            ).encode()
        ).hexdigest()

    def _get_client(self):
        with _SDK_CLIENT_LOCK:
            if self._client is None:
                self._client = self._create_client()
        return self._client

    def _create_client(self):
        _quiet_sdk_logs()
        profile = self._profile
        kwargs = {
            'endpoint_url': profile.endpoint,
            'region_name': profile.region,
            'verify': (
                profile.ca_file
                if profile.tls_verify and profile.ca_file
                else profile.tls_verify
            ),
            'config': BotoConfig(
                signature_version='s3v4',
                connect_timeout=profile.connect_timeout,
                read_timeout=profile.read_timeout,
                max_pool_connections=profile.max_connections,
                retries={
                    'mode': 'standard',
                    'total_max_attempts': profile.max_attempts,
                },
                s3={'addressing_style': profile.addressing_style},
                request_checksum_calculation='when_required',
                response_checksum_validation='when_required',
            ),
        }
        for field, name in (
            ('access_key_id', 'aws_access_key_id'),
            ('secret_access_key', 'aws_secret_access_key'),
            ('session_token', 'aws_session_token'),
        ):
            value = getattr(profile, field)
            if value is not None:
                kwargs[name] = value.get_secret_value()
        # Credential-chain discovery and refresh can perform blocking I/O.
        return boto3.client('s3', **kwargs)

    def _key(self, key: str) -> str:
        relative = _relative_key(key)
        full_key = (
            f'{self._profile.prefix}/{relative}' if self._profile.prefix else relative
        )
        if len(full_key.encode()) > 1024:
            raise ArtifactStorageError('invalid_key')
        return full_key

    async def _run(self, operation, *args):
        async with self._semaphore:
            task = asyncio.create_task(asyncio.to_thread(operation, *args))
            try:
                return await asyncio.shield(task)
            except asyncio.CancelledError:
                # Cancellation cannot interrupt a boto call; retain its concurrency
                # slot until the configured SDK timeouts/retries have completed.
                try:
                    await task
                except Exception:
                    pass
                raise
            except ArtifactStorageError:
                raise
            except (ClientError, BotoCoreError, OSError) as error:
                raise _safe_error(error) from None

    async def put_json(
        self, key: str, value: Dict[str, Any], *, format: str = 'json.gz'
    ) -> ArtifactMetadata:
        """Create immutable gzip JSON and return metadata for the domain index."""
        full_key = self._key(key)
        return await self._run(self._put_json, key, full_key, value, format)

    def _put_json(self, key, full_key, value, format='json.gz'):
        return self._put_prepared(key, full_key, self._prepare_json(value, format))

    async def prepare_json(
        self, value: Dict[str, Any], *, format: str = 'json.gz'
    ) -> PreparedArtifact:
        """Encode off-thread before reserving the exact compressed object size."""
        return await self._run(self._prepare_json, value, format)

    async def put_prepared(self, key: str, value: PreparedArtifact) -> ArtifactMetadata:
        """Upload an already encoded object without repeating compression."""
        return await self._run(self._put_prepared, key, self._key(key), value)

    def _prepare_json(self, value, format='json.gz'):
        if format not in ('json', 'json.gz'):
            raise ArtifactStorageError('unsupported_format')
        try:
            if not isinstance(value, dict):
                raise ValueError('object required')
            raw = json.dumps(
                value,
                ensure_ascii=False,
                sort_keys=True,
                separators=(',', ':') if format == 'json.gz' else None,
                indent=2 if format == 'json' else None,
                allow_nan=False,
            ).encode()
        except (TypeError, ValueError, OverflowError, RecursionError):
            raise ArtifactStorageError('invalid_json') from None
        if len(raw) > self._profile.max_object_bytes:
            raise ArtifactStorageError('object_too_large')
        digest = hashlib.sha256(raw).hexdigest()
        body = gzip.compress(raw, mtime=0) if format == 'json.gz' else raw
        return PreparedArtifact(body, digest, len(raw), format)

    def _put_prepared(self, key, full_key, value):
        body, digest = value.body, value.sha256
        if value.uncompressed_size_bytes > self._profile.max_object_bytes:
            raise ArtifactStorageError('object_too_large')
        parameters = {
            'Bucket': self._profile.bucket,
            'Key': full_key,
            'Body': body,
            'ContentType': (
                'application/json; charset=utf-8'
                if value.format == 'json'
                else 'application/json'
            ),
            'Metadata': {
                'sha256': digest,
                'format': value.format,
                'uncompressed-size': str(value.uncompressed_size_bytes),
            },
        }
        if value.format == 'json.gz':
            parameters['ContentEncoding'] = 'gzip'
        response = self._create_object(parameters, digest)
        return ArtifactMetadata(
            self.profile_id,
            key,
            digest,
            response.get('ContentLength', len(body)),
            value.uncompressed_size_bytes,
            response.get('ETag'),
            value.format,
        )

    def _create_object(self, parameters, digest):
        client = self._get_client()
        # Conditional creation needs only object grants, unlike HEAD on a
        # missing key, whose AWS response depends on ListBucket permission.
        if not self._conditional_writes:
            return self._portable_create(parameters, digest)
        try:
            return client.put_object(**parameters, IfNoneMatch='*')
        except ClientError as error:
            code = str(error.response.get('Error', {}).get('Code', ''))
            if code in ('412', 'PreconditionFailed'):
                return self._existing_object(parameters['Key'], digest)
            if not _unsupported_condition(error):
                raise
            self._conditional_writes = False
            return self._portable_create(parameters, digest)

    def _existing_object(self, key, digest):
        existing = self._get_client().head_object(Bucket=self._profile.bucket, Key=key)
        if existing.get('Metadata', {}).get('sha256') != digest:
            raise ArtifactStorageError('object_conflict')
        self._get_json(key, digest)
        return existing

    def _portable_create(self, parameters, digest):
        # This preflight prevents serial replacement. Atomicity is supplied by
        # unique domain object names and the database publication fence.
        try:
            return self._existing_object(parameters['Key'], digest)
        except ClientError as error:
            code = str(error.response.get('Error', {}).get('Code', ''))
            if code not in ('404', 'NoSuchKey'):
                raise
        return self._get_client().put_object(**parameters)

    async def get_json(
        self, key: str, expected_sha256: Optional[str] = None
    ) -> Dict[str, Any]:
        """Read with bounded compressed/decoded bytes and verify index checksum."""
        return await self._run(self._get_json, self._key(key), expected_sha256)

    def _get_json(self, key, expected_sha256):
        response = self._get_client().get_object(Bucket=self._profile.bucket, Key=key)
        stream = response['Body']
        limit = self._profile.max_object_bytes
        compressed_limit = limit + limit // 1000 + 65536
        try:
            if response.get('ContentLength', 0) > compressed_limit:
                raise ArtifactStorageError('object_too_large')
            compressed = stream.read(compressed_limit + 1)
            if len(compressed) > compressed_limit:
                raise ArtifactStorageError('object_too_large')
        finally:
            stream.close()
        try:
            format = response.get('Metadata', {}).get('format', 'json.gz')
            if format == 'json.gz':
                with gzip.GzipFile(fileobj=io.BytesIO(compressed)) as source:
                    raw = source.read(limit + 1)
            elif format == 'json':
                raw = compressed
            else:
                raise ArtifactStorageError('unsupported_format')
            if len(raw) > limit:
                raise ArtifactStorageError('object_too_large')
            digest = hashlib.sha256(raw).hexdigest()
            stored_digest = response.get('Metadata', {}).get('sha256')
            if (
                not stored_digest
                or digest != stored_digest
                or (expected_sha256 and digest != expected_sha256)
            ):
                raise ArtifactStorageError('checksum_mismatch')
            value = json.loads(raw)
            if not isinstance(value, dict):
                raise ValueError('object required')
            return value
        except (ValueError, EOFError, gzip.BadGzipFile, zlib.error, RecursionError):
            raise ArtifactStorageError('invalid_json') from None

    async def delete(self, key: str) -> None:
        """Delete one indexed key; a missing object is already deleted."""
        await self._run(self._delete, self._key(key))

    def _delete(self, key):
        self._get_client().delete_object(Bucket=self._profile.bucket, Key=key)

    async def check(self) -> Dict[str, Any]:
        """Check bucket reachability without creating or modifying resources."""
        await self._run(self._check)
        return {'available': True, 'profile_id': self.profile_id}

    def _check(self):
        self._get_client().head_bucket(Bucket=self._profile.bucket)

    async def health(self) -> Dict[str, Any]:
        return await self.check()


def _bind_profile(store: ArtifactStore, data_dir: Optional[str]) -> None:
    if not data_dir or data_dir in store._bound_directories:
        return
    directory = Path(data_dir) / 'artifacts' / 'profiles'
    directory.mkdir(parents=True, exist_ok=True, mode=0o700)
    path = directory / (hashlib.sha256(store.profile_id.encode()).hexdigest() + '.json')
    binding = json.dumps(
        {'profile_id': store.profile_id, 'fingerprint': store.profile_fingerprint}
    )
    if path.exists():
        _check_binding(path, store.profile_fingerprint)
        store._bound_directories.add(data_dir)
        return
    temporary = None
    try:
        with tempfile.NamedTemporaryFile(
            mode='w', dir=directory, delete=False
        ) as output:
            temporary = Path(output.name)
            output.write(binding)
            output.flush()
            os.fsync(output.fileno())
        try:
            os.link(temporary, path)
        except FileExistsError:
            _check_binding(path, store.profile_fingerprint)
        store._bound_directories.add(data_dir)
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)


def _check_binding(path, fingerprint):
    try:
        if json.loads(path.read_text()).get('fingerprint') != fingerprint:
            raise ArtifactStorageError('profile_binding_changed')
    except (ValueError, OSError):
        raise ArtifactStorageError('profile_unavailable') from None


def get_artifact_store(
    config=None, profile_id: Optional[str] = None
) -> Optional[ArtifactStore]:
    """Resolve the default write profile or a retained historical read profile."""
    if config is None:
        from gpustack.config.config import get_global_config

        config = get_global_config()
    if config is None:
        return None
    mode = getattr(config, 'artifact_storage_mode', 'disabled')
    default_id = getattr(config, 'artifact_storage_profile_id', 'default')
    profiles = getattr(config, 'artifact_storage_read_profiles', {})
    if profile_id and profile_id != default_id:
        if profile_id not in profiles:
            raise ArtifactStorageError('profile_unavailable')
        profile = profiles[profile_id]
    else:
        if mode == 'disabled':
            if profile_id and profile_id in profiles:
                profile = profiles[profile_id]
            elif profile_id:
                raise ArtifactStorageError('profile_unavailable')
            else:
                return None
        else:
            profile = {
                name: getattr(config, 'artifact_storage_' + name)
                for name in ArtifactStorageProfile.model_fields
                if hasattr(config, 'artifact_storage_' + name)
            }
            if mode == 'builtin':
                from gpustack.artifacts.builtin import builtin_profile

                try:
                    profile = builtin_profile(config, profile)
                except OSError:
                    raise ArtifactStorageError(
                        'builtin_credentials_unavailable'
                    ) from None
    try:
        settings = (
            profile
            if isinstance(profile, ArtifactStorageProfile)
            else ArtifactStorageProfile.model_validate(profile)
        )
        identity = profile_id or default_id
        if not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9_.-]{0,127}', identity):
            raise ArtifactStorageError('invalid_profile_id')
        store = _cached_store(identity, settings, getattr(config, 'data_dir', None))
        _bind_profile(store, getattr(config, 'data_dir', None))
        return store
    except (ValidationError, OSError):
        raise ArtifactStorageError('invalid_configuration') from None


def _cached_store(identity, settings, data_dir):
    # Share a connection pool and semaphore between domain calls in one runtime,
    # without carrying asyncio primitives across event loops or test sessions.
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        return ArtifactStore(identity, settings)
    values = settings.model_dump()
    for field in ('access_key_id', 'secret_access_key', 'session_token'):
        value = getattr(settings, field)
        values[field] = value.get_secret_value() if value else None
    digest = hashlib.sha256(json.dumps(values, sort_keys=True).encode()).hexdigest()
    key = (identity, data_dir, digest)
    cache = getattr(loop, '_gpustack_artifact_stores', None)
    if cache is None:
        cache = {}
        loop._gpustack_artifact_stores = cache
    if key not in cache:
        if len(cache) >= 64:
            cache.clear()
        cache[key] = ArtifactStore(identity, settings)
    return cache[key]
