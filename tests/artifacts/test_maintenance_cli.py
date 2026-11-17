import argparse
from unittest.mock import patch

from gpustack.cmd.artifact_storage import (
    setup_artifact_storage_cmd,
    load_maintenance_config,
)


def parser():
    root = argparse.ArgumentParser()
    setup_artifact_storage_cmd(root.add_subparsers())
    return root


def test_cli_defaults_to_dry_run_and_supports_scoped_apply():
    args = parser().parse_args(
        [
            'artifact-storage',
            'migrate-profile',
            '--source-id',
            'old',
            '--target-id',
            'new',
        ]
    )
    assert args.apply is False
    args = parser().parse_args(
        [
            'artifact-storage',
            'migrate-profile',
            '--source-id',
            'old',
            '--target-id',
            'new',
            '--apply',
            '--benchmark-id',
            '7',
            '--run-id',
            'run',
        ]
    )
    assert args.apply is True
    assert args.benchmark_id == 7
    assert args.run_id == 'run'


def test_capacity_command_supports_read_and_explicit_unlimited_update():
    args = parser().parse_args(['artifact-storage', 'capacity'])
    assert args.max_bytes is None
    args = parser().parse_args(['artifact-storage', 'capacity', '--max-bytes', '0'])
    assert args.max_bytes == 0
    assert args.artifact_operation == 'capacity'


def test_loading_maintenance_config_never_initializes_auth_or_creates_directories(
    tmp_path,
):
    path = tmp_path / 'config.yaml'
    data_dir = tmp_path / 'uncreated'
    path.write_text(
        f'data_dir: {data_dir}\n'
        'artifact_storage_mode: external\n'
        'artifact_storage_profile_id: current\n'
        'artifact_storage_bucket: archive\n'
        'oidc_issuer: https://identity.example.test\n'
    )
    args = parser().parse_args(
        ['artifact-storage', '--config-file', str(path), 'audit']
    )
    with (
        patch('gpustack.config.config.Config.init_auth') as auth,
        patch('gpustack.config.config.Config.make_dirs') as dirs,
    ):
        config = load_maintenance_config(args)
    auth.assert_not_called()
    dirs.assert_not_called()
    assert config.artifact_storage_profile_id == 'current'
    assert not data_dir.exists()


def test_command_error_never_prints_driver_credentials(capsys):
    import pytest
    from gpustack.cmd.artifact_storage import run

    args = parser().parse_args(['artifact-storage', 'audit'])
    with patch(
        'gpustack.cmd.artifact_storage._run',
        side_effect=RuntimeError('postgresql://user:secret-password@host/db'),
    ):
        with pytest.raises(SystemExit) as caught:
            run(args)
    assert caught.value.code == 1
    output = capsys.readouterr()
    assert 'secret-password' not in output.err + output.out
    assert 'maintenance_unavailable' in output.err
