"""Single-node packaged S3 service bootstrap; business code uses only S3."""

import json
import os
import secrets
import tempfile
import time
from pathlib import Path
from typing import Any, Dict

import boto3
from botocore.config import Config as BotoConfig
from botocore.exceptions import BotoCoreError, ClientError

from gpustack.artifacts.store import ArtifactStorageError, _quiet_sdk_logs


def _directory(config) -> Path:
    return Path(config.data_dir) / 'artifacts' / 'builtin'


def _write_private(path: Path, content: str) -> None:
    temporary = None
    try:
        with tempfile.NamedTemporaryFile(
            mode='w', dir=path.parent, delete=False
        ) as output:
            temporary = Path(output.name)
            output.write(content)
            output.flush()
            os.fsync(output.fileno())
        os.replace(temporary, path)
        temporary = None
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)


def _credentials(config) -> Dict[str, str]:
    directory = _directory(config)
    directory.mkdir(parents=True, exist_ok=True, mode=0o700)
    path = directory / 'credentials.json'
    if not path.exists():
        credentials = {
            'access_key_id': secrets.token_hex(16),
            'secret_access_key': secrets.token_urlsafe(48),
        }
        temporary = None
        try:
            with tempfile.NamedTemporaryFile(
                mode='w', dir=directory, delete=False
            ) as output:
                temporary = Path(output.name)
                json.dump(credentials, output)
                output.flush()
                os.fsync(output.fileno())
            # Publish a complete credential file without replacing credentials
            # concurrently created by another process.
            try:
                os.link(temporary, path)
            except FileExistsError:
                pass
        finally:
            if temporary is not None:
                temporary.unlink(missing_ok=True)
    try:
        os.chmod(path, 0o600)
        credentials = json.loads(path.read_text())
        if not isinstance(credentials, dict) or not all(
            isinstance(credentials.get(name), str) and credentials[name]
            for name in ('access_key_id', 'secret_access_key')
        ):
            raise ValueError('credentials required')
        return credentials
    except (OSError, ValueError):
        raise ArtifactStorageError('builtin_credentials_unavailable') from None


def builtin_profile(config, values: Dict[str, Any]) -> Dict[str, Any]:
    """Resolve persisted local credentials without exposing them in Config."""
    values = dict(values)
    values.update(_credentials(config))
    port = getattr(config, 'artifact_storage_builtin_port', 18333)
    endpoint = f'http://127.0.0.1:{port}'
    if values.get('endpoint') not in (None, endpoint):
        raise ArtifactStorageError('builtin_endpoint_conflict')
    values.update(endpoint=endpoint, addressing_style='path')
    values.pop('session_token', None)
    return values


def prepare_builtin(config) -> None:
    """Prepare private auth and persistent Filer metadata before s6 starts."""
    if config.artifact_storage_mode != 'builtin':
        return
    credentials = _credentials(config)
    directory = _directory(config)
    (directory / 'data').mkdir(exist_ok=True, mode=0o700)
    auth = {
        'identities': [
            {
                'name': 'gpustack',
                'credentials': [
                    {
                        'accessKey': credentials['access_key_id'],
                        'secretKey': credentials['secret_access_key'],
                    }
                ],
                'actions': ['Admin', 'Read', 'Write', 'List', 'Tagging'],
            }
        ]
    }
    _write_private(directory / 's3.json', json.dumps(auth))
    # LevelDB lives in the same persistent volume as master and object data.
    _write_private(
        directory / 'filer.toml', '[leveldb2]\nenabled = true\ndir = "./filerldb2"\n'
    )


def initialize_bucket(config) -> None:
    """Wait for the packaged service and create only its managed bucket."""
    if config.artifact_storage_mode != 'builtin':
        return
    _quiet_sdk_logs()
    values = builtin_profile(config, {})
    client = boto3.client(
        's3',
        endpoint_url=values['endpoint'],
        aws_access_key_id=values['access_key_id'],
        aws_secret_access_key=values['secret_access_key'],
        region_name='us-east-1',
        config=BotoConfig(
            connect_timeout=2,
            read_timeout=3,
            retries={'total_max_attempts': 1},
            s3={'addressing_style': 'path'},
        ),
    )
    bucket = getattr(config, 'artifact_storage_bucket', 'gpustack-artifacts')
    for attempt in range(30):
        try:
            client.head_bucket(Bucket=bucket)
            return
        except ClientError as error:
            code = str(error.response.get('Error', {}).get('Code', ''))
            if code in ('404', 'NoSuchBucket', 'NotFound'):
                try:
                    client.create_bucket(Bucket=bucket)
                    return
                except (ClientError, BotoCoreError):
                    pass
            elif code in (
                '403',
                'AccessDenied',
                'InvalidAccessKeyId',
                'SignatureDoesNotMatch',
            ):
                raise ArtifactStorageError('access_denied') from None
        except BotoCoreError:
            pass
        if attempt < 29:
            time.sleep(1)
    raise ArtifactStorageError('builtin_startup_timeout')


def main() -> None:
    """Initialize using the same parsed arguments as the packaged server."""
    import argparse
    from gpustack.cmd.start import start_cmd_options
    from gpustack.cmd.artifact_storage import load_maintenance_config

    parser = argparse.ArgumentParser()
    start_cmd_options(parser)
    arguments = Path('/run/gpustack/args').read_text().splitlines()
    initialize_bucket(load_maintenance_config(parser.parse_args(arguments)))


if __name__ == '__main__':
    main()
