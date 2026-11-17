"""Shared capacity policy and durable accounting for private S3 objects."""

from typing import Literal, Optional

from pydantic import BaseModel, ConfigDict, Field as PydanticField
from sqlalchemy import BigInteger, Column
from sqlmodel import Field, SQLModel

MAX_CAPACITY_BYTES = 9007199254740991


class ArtifactStorageCapacity(SQLModel, table=True):
    __tablename__ = 'artifact_storage_capacity'

    id: int = Field(default=1, primary_key=True)
    max_bytes: int = Field(default=0, sa_column=Column(BigInteger, nullable=False))
    used_bytes: int = Field(default=0, sa_column=Column(BigInteger, nullable=False))
    reserved_bytes: int = Field(default=0, sa_column=Column(BigInteger, nullable=False))
    initialized: bool = False


class ArtifactStorageObject(SQLModel, table=True):
    __tablename__ = 'artifact_storage_objects'

    id: str = Field(primary_key=True, max_length=64)
    archive_id: Optional[str] = Field(default=None, max_length=36, index=True)
    profile_id: str = Field(max_length=128)
    object_key: str = Field(max_length=1024)
    size_bytes: int = Field(sa_column=Column(BigInteger, nullable=False))
    state: str = Field(max_length=16)


class ArtifactCapacityUpdate(BaseModel):
    model_config = ConfigDict(extra='forbid')

    max_bytes: int = PydanticField(ge=0, le=MAX_CAPACITY_BYTES, strict=True)


class ArtifactCapacityUsage(BaseModel):
    max_bytes: int
    unlimited: bool
    used_bytes: int
    reserved_bytes: int
    total_bytes: int
    remaining_bytes: Optional[int]
    usage_percent: Optional[float]
    state: Literal['unlimited', 'normal', 'warning', 'full']
    warning_threshold_percent: int = 80
