"""Archive object names describe their scope while retaining immutable IDs."""

import pytest

from gpustack.server import benchmark_monitoring_archiver as archiver

OBJECT_ID = '02972e46-bdac-4d84-9007-481f45f159f0'


@pytest.mark.parametrize(
    'scope,prefix',
    [
        ('run/resources/all/aggregate', 'resources'),
        ('run/runtime/all/aggregate', 'runtime-aggregate'),
        ('run/runtime/all/instances', 'runtime-instances'),
        ('run/runtime/prefill/instances', 'runtime-prefill-instances'),
        ('run/runtime/router/aggregate', 'runtime-router-aggregate'),
        (
            'phase:1024:1/runtime/decode/instances',
            'phase-1024-1-runtime-decode-instances',
        ),
        ('phase:1024:1/resources/all/aggregate', 'phase-1024-1-resources'),
    ],
)
def test_object_name_exposes_range_kind_role_and_view(scope, prefix):
    assert archiver._object_filename(scope, OBJECT_ID) == (
        f'{prefix}-{OBJECT_ID}.json.gz'
    )


def test_long_phase_identifier_keeps_kind_role_and_view_readable():
    name = archiver._object_filename(
        'phase:' + 'x' * 1000 + '/runtime/decode/instances', OBJECT_ID
    )
    assert name.endswith(f'-runtime-decode-instances-{OBJECT_ID}.json.gz')
    assert len(name.encode()) <= 240


def test_prefix_is_a_safe_single_ascii_filename():
    name = archiver._object_filename(
        'phase:测试..\\\x00/runtime/all/aggregate', OBJECT_ID
    )
    assert name.isascii()
    assert '/' not in name and '\\' not in name and '\x00' not in name
    assert '..' not in name
    assert name.endswith(f'-{OBJECT_ID}.json.gz')
