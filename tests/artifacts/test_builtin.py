import json
import stat
from types import SimpleNamespace
from unittest.mock import patch

import pytest


from gpustack.artifacts import get_artifact_store
from gpustack.artifacts.builtin import prepare_builtin, initialize_bucket
from gpustack.config.config import Config
from gpustack.cmd.prerun import determine_enabled_services


def test_builtin_credentials_are_random_private_and_persistent(tmp_path):
    cfg = SimpleNamespace(
        data_dir=str(tmp_path),
        artifact_storage_mode='builtin',
        artifact_storage_profile_id='builtin',
        artifact_storage_bucket='gpustack-artifacts',
        artifact_storage_prefix='gpustack',
        artifact_storage_builtin_port=18333,
    )
    prepare_builtin(cfg)
    path = tmp_path / 'artifacts' / 'builtin' / 'credentials.json'
    first = json.loads(path.read_text())
    prepare_builtin(cfg)
    assert json.loads(path.read_text()) == first
    assert len(first['access_key_id']) >= 24
    assert len(first['secret_access_key']) >= 32
    assert stat.S_IMODE(path.stat().st_mode) == 0o600
    store = get_artifact_store(cfg)
    assert store._profile.endpoint == 'http://127.0.0.1:18333'
    assert store._profile.access_key_id.get_secret_value() == first['access_key_id']
    auth = json.loads((path.parent / 's3.json').read_text())
    assert (
        auth['identities'][0]['credentials'][0]['secretKey']
        == first['secret_access_key']
    )


def test_external_never_initializes_bucket(tmp_path):
    cfg = SimpleNamespace(data_dir=str(tmp_path), artifact_storage_mode='external')
    with patch('gpustack.artifacts.builtin.boto3.client') as client:
        initialize_bucket(cfg)
    client.assert_not_called()
    assert not (tmp_path / 'artifacts' / 'builtin').exists()


def test_config_redacts_all_artifact_credentials(tmp_path):
    cfg = Config(
        token='test',
        jwt_secret_key='test',
        data_dir=str(tmp_path),
        artifact_storage_access_key_id='secret-access',
        artifact_storage_secret_access_key='secret-password',
        artifact_storage_session_token='secret-session',
        artifact_storage_read_profiles={
            'old': {
                'bucket': 'old',
                'access_key_id': 'secret-old-key',
                'secret_access_key': 'secret-old-password',
            }
        },
    )
    exported = cfg.model_dump_json()
    shown = repr(cfg)
    for secret in [
        'secret-access',
        'secret-password',
        'secret-session',
        'secret-old-key',
        'secret-old-password',
    ]:
        assert secret not in exported
        assert secret not in shown


def test_nonpackaged_default_is_disabled_and_packaged_worker_stays_disabled(
    tmp_path, monkeypatch
):
    monkeypatch.delenv('GPUSTACK_PACKAGED', raising=False)
    cfg = Config(token='test', jwt_secret_key='test', data_dir=str(tmp_path / 'normal'))
    assert cfg.artifact_storage_mode == 'disabled'
    monkeypatch.setenv('GPUSTACK_PACKAGED', '1')
    server = Config(
        token='test', jwt_secret_key='test', data_dir=str(tmp_path / 'server')
    )
    assert server.artifact_storage_mode == 'builtin'
    worker = Config(
        token='test',
        jwt_secret_key='test',
        server_url='http://server',
        data_dir=str(tmp_path / 'worker'),
    )
    assert worker.artifact_storage_mode == 'disabled'
    assert 'seaweedfs' not in determine_enabled_services(worker)


def test_builtin_service_only_enabled_for_server(tmp_path):
    common = dict(token='test', jwt_secret_key='test', data_dir=str(tmp_path))
    cfg = Config(**common, artifact_storage_mode='builtin')
    assert 'seaweedfs' in determine_enabled_services(cfg)
    cfg.artifact_storage_mode = 'external'
    assert 'seaweedfs' not in determine_enabled_services(cfg)
    cfg.artifact_storage_mode = 'disabled'
    assert 'seaweedfs' not in determine_enabled_services(cfg)


def test_configuration_errors_hide_credential_inputs(tmp_path):
    import pytest

    with pytest.raises(ValueError) as error:
        Config(
            token='test',
            jwt_secret_key='test',
            data_dir=str(tmp_path),
            artifact_storage_mode='external',
            artifact_storage_endpoint='https://key:secret-password@s3.example.test',
            artifact_storage_access_key_id='secret-key',
            artifact_storage_secret_access_key='secret-password',
        )
    assert 'secret-password' not in str(error.value)
    assert 'secret-key' not in str(error.value)


def test_builtin_initialization_creates_only_managed_bucket(tmp_path):
    from unittest.mock import Mock
    from botocore.exceptions import ClientError

    cfg = SimpleNamespace(
        data_dir=str(tmp_path),
        artifact_storage_mode='builtin',
        artifact_storage_bucket='managed-only',
        artifact_storage_builtin_port=18333,
    )
    client = Mock()
    client.head_bucket.side_effect = ClientError(
        {'Error': {'Code': '404'}}, 'HeadBucket'
    )
    with patch('gpustack.artifacts.builtin.boto3.client', return_value=client):
        initialize_bucket(cfg)
    client.create_bucket.assert_called_once_with(Bucket='managed-only')


def test_builtin_initialization_never_creates_on_permission_failure(tmp_path):
    from unittest.mock import Mock
    from botocore.exceptions import ClientError
    import pytest
    from gpustack.artifacts import ArtifactStorageError

    cfg = SimpleNamespace(
        data_dir=str(tmp_path),
        artifact_storage_mode='builtin',
        artifact_storage_builtin_port=18333,
    )
    client = Mock()
    client.head_bucket.side_effect = ClientError(
        {'Error': {'Code': 'AccessDenied', 'Message': 'secret'}}, 'HeadBucket'
    )
    with patch('gpustack.artifacts.builtin.boto3.client', return_value=client):
        with pytest.raises(ArtifactStorageError, match='access_denied'):
            initialize_bucket(cfg)
    client.create_bucket.assert_not_called()


def test_packaged_init_oneshot_invokes_executable_script():
    from pathlib import Path

    root = Path(__file__).resolve().parents[2] / 'pack' / 'rootfs'
    up = root / 'etc' / 's6-overlay' / 's6-rc.d' / 'artifact-storage-init' / 'up'
    command = up.read_text().strip()
    assert command == '/etc/s6-overlay/scripts/artifact-storage-init.sh'
    script = root / command.lstrip('/')
    assert script.is_file()
    assert script.stat().st_mode & 0o111


@pytest.mark.parametrize(
    'options',
    [
        {'port': 18333},
        {'metrics_port': 19333},
        {'artifact_storage_builtin_port': 19333},
    ],
)
def test_builtin_storage_port_conflicts_are_rejected(tmp_path, options):
    import pytest
    from gpustack.cmd.prerun import ports_for_services

    config = Config(
        token='test',
        jwt_secret_key='test',
        data_dir=str(tmp_path),
        artifact_storage_mode='builtin',
        **options,
    )
    with pytest.raises(Exception, match='Port conflict'):
        ports_for_services(config)
