"""Platform-only storage health and installation capacity administration."""

from fastapi import APIRouter

from gpustack.api.exceptions import ServiceUnavailableException
from gpustack.artifacts import ArtifactStorageError, get_artifact_store
from gpustack.artifacts.capacity import CapacityManager
from gpustack.schemas.artifact_storage import (
    ArtifactCapacityUpdate,
    ArtifactCapacityUsage,
)
from gpustack.server.deps import PlatformAdminDep

router = APIRouter()


@router.get('/health')
async def artifact_storage_health(ctx: PlatformAdminDep):
    try:
        store = get_artifact_store()
        if store is None:
            return {'available': False, 'state': 'disabled'}
        return await store.health()
    except ArtifactStorageError as exc:
        raise ServiceUnavailableException(
            message='Artifact storage health check failed',
            details={'code': exc.reason},
        ) from None


@router.get('/capacity', response_model=ArtifactCapacityUsage)
async def artifact_storage_capacity(ctx: PlatformAdminDep):
    try:
        return await CapacityManager().usage()
    except Exception:
        raise ServiceUnavailableException(
            message='Artifact storage capacity is unavailable',
            details={'code': 'capacity_unavailable'},
        ) from None


@router.put('/capacity', response_model=ArtifactCapacityUsage)
async def update_artifact_storage_capacity(
    body: ArtifactCapacityUpdate, ctx: PlatformAdminDep
):
    try:
        return await CapacityManager().set_limit(body.max_bytes)
    except Exception:
        raise ServiceUnavailableException(
            message='Artifact storage capacity is unavailable',
            details={'code': 'capacity_unavailable'},
        ) from None
