from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from gpustack.api.exceptions import register_handlers
from gpustack.api.tenant import get_tenant_context
from gpustack.routes import artifact_storage


def usage(maximum=0):
    return {
        'max_bytes': maximum,
        'unlimited': maximum == 0,
        'used_bytes': 2**34,
        'reserved_bytes': 10,
        'total_bytes': 2**34 + 10,
        'remaining_bytes': None if not maximum else 0,
        'usage_percent': None if not maximum else 100,
        'state': 'unlimited' if not maximum else 'full',
        'warning_threshold_percent': 80,
    }


def application(admin):
    app = FastAPI()
    register_handlers(app)
    app.dependency_overrides[get_tenant_context] = lambda: SimpleNamespace(
        is_platform_admin=admin
    )
    app.include_router(artifact_storage.router)
    return app


@pytest.mark.parametrize('admin', [False, True])
@pytest.mark.parametrize('method', ['get', 'put'])
def test_capacity_read_and_update_require_platform_administrator(
    monkeypatch, admin, method
):
    manager = SimpleNamespace(
        usage=AsyncMock(return_value=usage()),
        set_limit=AsyncMock(return_value=usage()),
    )
    monkeypatch.setattr(artifact_storage, 'CapacityManager', lambda: manager)
    with TestClient(application(admin)) as client:
        response = (
            client.get('/capacity')
            if method == 'get'
            else client.put('/capacity', json={'max_bytes': 0})
        )
    assert response.status_code == (200 if admin else 403)
    operation = manager.usage if method == 'get' else manager.set_limit
    assert operation.await_count == int(admin)
    if admin:
        assert response.json()['used_bytes'] == 2**34
        assert response.json()['unlimited'] is True


@pytest.mark.parametrize('value', [-1, 1.5, True, '100', 9007199254740992])
def test_capacity_update_rejects_invalid_limits_before_side_effects(monkeypatch, value):
    manager = SimpleNamespace(set_limit=AsyncMock())
    monkeypatch.setattr(artifact_storage, 'CapacityManager', lambda: manager)
    with TestClient(application(True)) as client:
        response = client.put('/capacity', json={'max_bytes': value})
    assert response.status_code == 422
    assert manager.set_limit.await_count == 0


@pytest.mark.parametrize('method', ['get', 'put'])
def test_database_errors_are_safe_and_explicit(monkeypatch, method):
    manager = SimpleNamespace(
        usage=AsyncMock(side_effect=RuntimeError('postgresql://secret@private')),
        set_limit=AsyncMock(side_effect=RuntimeError('s3-secret-key')),
    )
    monkeypatch.setattr(artifact_storage, 'CapacityManager', lambda: manager)
    with TestClient(application(True)) as client:
        response = (
            client.get('/capacity')
            if method == 'get'
            else client.put('/capacity', json={'max_bytes': 0})
        )
    assert response.status_code == 503
    assert 'capacity_unavailable' in response.text
    assert 'secret' not in response.text
    assert 'private' not in response.text
