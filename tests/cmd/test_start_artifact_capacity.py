"""Capacity seeds can be supplied consistently through startup configuration."""

import argparse

import pytest
from pydantic import ValidationError

from gpustack.cmd.artifact_storage import MaintenanceConfig
from gpustack.cmd.start import set_server_options, setup_start_cmd


def parse(values):
    parser = argparse.ArgumentParser()
    setup_start_cmd(parser.add_subparsers(dest='command'))
    return parser.parse_args(['start', *values])


@pytest.mark.parametrize('maximum', [0, 2**40])
def test_capacity_flag_reaches_the_settings(maximum, monkeypatch):
    monkeypatch.delenv('GPUSTACK_ARTIFACT_STORAGE_MAX_TOTAL_BYTES', raising=False)
    data = {}
    set_server_options(
        parse(['--artifact-storage-max-total-bytes', str(maximum)]), data
    )
    assert data['artifact_storage_max_total_bytes'] == maximum
    assert MaintenanceConfig(**data).artifact_storage_max_total_bytes == maximum


def test_capacity_seed_supports_environment_and_unset_does_not_clobber_yaml(
    monkeypatch,
):
    monkeypatch.setenv('GPUSTACK_ARTIFACT_STORAGE_MAX_TOTAL_BYTES', str(2**40))
    data = {}
    set_server_options(parse([]), data)
    assert MaintenanceConfig(**data).artifact_storage_max_total_bytes == 2**40
    monkeypatch.delenv('GPUSTACK_ARTIFACT_STORAGE_MAX_TOTAL_BYTES')
    data = {'artifact_storage_max_total_bytes': 0}
    set_server_options(parse([]), data)
    assert MaintenanceConfig(**data).artifact_storage_max_total_bytes == 0


@pytest.mark.parametrize('maximum', [-1, 9007199254740992])
def test_invalid_capacity_seed_is_rejected(maximum):
    with pytest.raises(ValidationError):
        MaintenanceConfig(artifact_storage_max_total_bytes=maximum)
