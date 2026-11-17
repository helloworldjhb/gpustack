from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from gpustack.api.exceptions import register_handlers
from gpustack.api.tenant import get_tenant_context
from gpustack.routes import artifact_storage


@pytest.mark.parametrize('admin', [False, True])
def test_storage_health_requires_platform_administrator(monkeypatch, admin):
    store = SimpleNamespace(
        health=AsyncMock(return_value={'available': True, 'profile_id': 'shared'})
    )
    monkeypatch.setattr(artifact_storage, 'get_artifact_store', lambda: store)
    app = FastAPI()
    register_handlers(app)
    app.dependency_overrides[get_tenant_context] = lambda: SimpleNamespace(
        is_platform_admin=admin
    )
    app.include_router(artifact_storage.router)
    with TestClient(app) as client:
        result = client.get('/health')
    assert result.status_code == (200 if admin else 403)
    assert store.health.await_count == (1 if admin else 0)


def test_disabled_storage_health_does_not_contact_s3(monkeypatch):
    monkeypatch.setattr(artifact_storage, 'get_artifact_store', lambda: None)
    app = FastAPI()
    app.dependency_overrides[get_tenant_context] = lambda: SimpleNamespace(
        is_platform_admin=True
    )
    app.include_router(artifact_storage.router)
    with TestClient(app) as client:
        result = client.get('/health')
    assert result.json() == {'available': False, 'state': 'disabled'}
