"""Capacity accounting upgrades an installed archive schema without altering it."""

import importlib.util
from pathlib import Path

import pytest
import sqlalchemy as sa
from alembic.migration import MigrationContext
from alembic.operations import Operations
from sqlalchemy.dialects import mysql, postgresql
from sqlalchemy.schema import CreateTable

from gpustack.schemas.artifact_storage import (
    ArtifactStorageCapacity,
    ArtifactStorageObject,
)


@pytest.mark.parametrize('dialect', [postgresql.dialect(), mysql.dialect()])
@pytest.mark.parametrize('model', [ArtifactStorageCapacity, ArtifactStorageObject])
def test_capacity_schema_uses_portable_64_bit_sizes(dialect, model):
    statement = str(CreateTable(model.__table__).compile(dialect=dialect))
    assert 'BIGINT' in statement
    assert 'FOREIGN KEY' not in statement
    assert model.__tablename__ in statement


def test_capacity_upgrade_replay_and_downgrade_preserve_archive_rows(monkeypatch):
    path = (
        Path(__file__).resolve().parents[2]
        / 'gpustack/migrations/versions'
        / '2026_11_17_0200-e7a9c3b50142_artifact_storage_capacity.py'
    )
    spec = importlib.util.spec_from_file_location('capacity_migration', path)
    migration = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(migration)
    assert migration.down_revision == 'd6f8b2a49031'
    engine = sa.create_engine('sqlite:///:memory:')
    with engine.begin() as connection:
        connection.execute(
            sa.text('CREATE TABLE benchmark_monitoring_archives (id VARCHAR(36))')
        )
        connection.execute(
            sa.text("INSERT INTO benchmark_monitoring_archives VALUES ('preserved')")
        )
        monkeypatch.setattr(
            migration, 'op', Operations(MigrationContext.configure(connection))
        )
        monkeypatch.setattr(
            migration,
            'table_exists',
            lambda name: sa.inspect(connection).has_table(name),
        )
        migration.upgrade()
        connection.execute(
            sa.text('UPDATE artifact_storage_capacity SET max_bytes = 17179869184')
        )
        migration.upgrade()
        assert (
            connection.execute(
                sa.text('SELECT max_bytes FROM artifact_storage_capacity WHERE id = 1')
            ).scalar_one()
            == 2**34
        )
        migration.downgrade()
        assert sa.inspect(connection).get_table_names() == [
            'benchmark_monitoring_archives'
        ]
        assert (
            connection.execute(
                sa.text('SELECT id FROM benchmark_monitoring_archives')
            ).scalar_one()
            == 'preserved'
        )
    engine.dispose()
