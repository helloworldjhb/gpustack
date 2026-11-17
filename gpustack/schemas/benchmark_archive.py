"""Durable execution archives, immutable publications and object indexes.

Benchmark IDs deliberately have no cascading foreign key: deleting a benchmark
must leave a durable cleanup job until its objects have been deleted.
"""

from typing import Any, Dict, Optional

from sqlalchemy import Column, JSON, UniqueConstraint
from sqlmodel import Field, SQLModel


class BenchmarkMonitoringInstallation(SQLModel, table=True):
    __tablename__ = 'benchmark_monitoring_installation'

    id: int = Field(default=1, primary_key=True)
    namespace_id: str = Field(max_length=36)


class BenchmarkMonitoringArchive(SQLModel, table=True):
    __tablename__ = 'benchmark_monitoring_archives'
    __table_args__ = (UniqueConstraint('benchmark_id', 'run_id'),)

    id: str = Field(primary_key=True, max_length=36)
    benchmark_id: int = Field(index=True)
    run_id: str = Field(max_length=64)
    owner_principal_id: Optional[int] = None
    profile_id: str = Field(max_length=128)
    profile_fingerprint: str = Field(max_length=64)
    state: str = Field(default='capturing', max_length=32, index=True)
    revision: int = 0
    checkpoint: int = 0
    captured_until: Optional[float] = None
    snapshot: Dict[str, Any] = Field(default_factory=dict, sa_column=Column(JSON))
    plan: Dict[str, Any] = Field(default_factory=dict, sa_column=Column(JSON))
    lease_owner: Optional[str] = Field(default=None, max_length=36)
    lease_until: float = 0
    fencing_token: int = 0
    retry_at: float = Field(default=0, index=True)
    attempts: int = 0
    last_error: Optional[str] = Field(default=None, max_length=128)
    delete_requested: bool = False


class BenchmarkMonitoringRevision(SQLModel, table=True):
    __tablename__ = 'benchmark_monitoring_revisions'
    __table_args__ = (UniqueConstraint('archive_id', 'revision'),)

    id: str = Field(primary_key=True, max_length=36)
    archive_id: str = Field(index=True, max_length=36)
    revision: int
    state: str = Field(max_length=32)
    data_coverage: str = Field(default='partial', max_length=16)
    captured_until: Optional[float] = None
    context: Dict[str, Any] = Field(default_factory=dict, sa_column=Column(JSON))
    scopes: Dict[str, Any] = Field(default_factory=dict, sa_column=Column(JSON))
    created_at: float


class BenchmarkMonitoringScope(SQLModel, table=True):
    __tablename__ = 'benchmark_monitoring_scopes'

    id: str = Field(primary_key=True, max_length=36)
    archive_id: str = Field(index=True, max_length=36)
    data: Dict[str, Any] = Field(default_factory=dict, sa_column=Column(JSON))


class BenchmarkMonitoringChunk(SQLModel, table=True):
    __tablename__ = 'benchmark_monitoring_chunks'

    id: str = Field(primary_key=True, max_length=36)
    archive_id: str = Field(index=True, max_length=36)
    scope: str = Field(index=True, max_length=160)
    profile_id: str = Field(max_length=128)
    object_key: str = Field(max_length=1024)
    sha256: str = Field(max_length=64)
    size_bytes: int
    uncompressed_size_bytes: int
    start: Optional[float] = None
    end: Optional[float] = None
    created_at: float
    published: bool = False
