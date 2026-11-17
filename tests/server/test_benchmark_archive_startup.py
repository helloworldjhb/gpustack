from types import SimpleNamespace

import pytest

from gpustack.server.coordinator import LocalCoordinator
from gpustack.server.server import Server


@pytest.mark.parametrize(
    'mode,local,rejected',
    [
        ('builtin', False, True),
        ('builtin', True, False),
        ('external', False, False),
        ('disabled', False, False),
    ],
)
def test_ha_requires_shared_artifact_endpoint(mode, local, rejected):
    server = object.__new__(Server)
    server._config = SimpleNamespace(artifact_storage_mode=mode)
    server._coordinator = object.__new__(LocalCoordinator) if local else object()
    if rejected:
        with pytest.raises(RuntimeError, match='shared S3'):
            server._validate_archive_coordinator()
    else:
        server._validate_archive_coordinator()
