"""Platform-only storage health and installation capacity administration."""

import logging

from fastapi import APIRouter

from gpustack.api.exceptions import InvalidException, ServiceUnavailableException
from gpustack.artifacts import ArtifactStorageError, get_artifact_store
from gpustack.artifacts.capacity import CapacityManager
from gpustack.schemas.artifact_storage import (
    ArtifactCapacityUpdate,
    ArtifactCapacityUsage,
)
from gpustack.server.deps import PlatformAdminDep

logger = logging.getLogger(__name__)

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
    except ArtifactStorageError as exc:
        raise ServiceUnavailableException(
            message='Artifact storage capacity is unavailable',
            details={'code': exc.reason},
        ) from None
    except Exception as exc:
        # Database exception messages may contain credentials.
        logger.error('Artifact storage capacity read failed (%s)', type(exc).__name__)
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
    except ArtifactStorageError as exc:
        if exc.reason == 'invalid_capacity_bytes':
            raise InvalidException(
                message='Invalid artifact storage capacity limit',
                details={'code': exc.reason},
            ) from None
        raise ServiceUnavailableException(
            message='Artifact storage capacity update failed',
            details={'code': exc.reason},
        ) from None
    except Exception as exc:
        logger.error('Artifact storage capacity update failed (%s)', type(exc).__name__)
        raise ServiceUnavailableException(
            message='Artifact storage capacity is unavailable',
            details={'code': 'capacity_unavailable'},
        ) from None
