"""Archive DDL is idempotent and uses types shared by both SQL dialect families."""

import importlib.util
from pathlib import Path

import pytest
import sqlalchemy as sa
from alembic.migration import MigrationContext
from alembic.operations import Operations
from sqlalchemy.dialects import mysql, postgresql
from sqlalchemy.schema import CreateTable

from gpustack.schemas.benchmark_archive import (
    BenchmarkMonitoringArchive,
    BenchmarkMonitoringChunk,
    BenchmarkMonitoringInstallation,
    BenchmarkMonitoringRevision,
    BenchmarkMonitoringScope,
)


@pytest.mark.parametrize('dialect', [postgresql.dialect(), mysql.dialect()])
@pytest.mark.parametrize(
    'model',
    [
        BenchmarkMonitoringArchive,
        BenchmarkMonitoringRevision,
        BenchmarkMonitoringChunk,
        BenchmarkMonitoringInstallation,
        BenchmarkMonitoringScope,
    ],
)
def test_archive_schema_compiles_for_postgres_opengauss_mysql_oceanbase(dialect, model):
    statement = str(CreateTable(model.__table__).compile(dialect=dialect))
    assert model.__tablename__ in statement
    assert 'JSONB' not in statement
    assert 'FOREIGN KEY(benchmark_id)' not in statement


def test_archive_upgrade_replay_and_downgrade(monkeypatch):
    path = (
        Path(__file__).resolve().parents[2]
        / 'gpustack/migrations/versions'
        / '2026_11_17_0100-d6f8b2a49031_benchmark_monitoring_archives.py'
    )
    spec = importlib.util.spec_from_file_location('archive_migration', path)
    migration = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(migration)
    engine = sa.create_engine('sqlite:///:memory:')
    with engine.begin() as connection:
        connection.execute(sa.text('CREATE TABLE benchmarks (id INTEGER PRIMARY KEY)'))
        monkeypatch.setattr(
            migration, 'op', Operations(MigrationContext.configure(connection))
        )
        monkeypatch.setattr(
            migration,
            'table_exists',
            lambda name: sa.inspect(connection).has_table(name),
        )
        monkeypatch.setattr(
            migration,
            'column_exists',
            lambda table, column: column
            in {c['name'] for c in sa.inspect(connection).get_columns(table)},
        )
        migration.upgrade()
        migration.upgrade()
        assert len(sa.inspect(connection).get_table_names()) == 6
        assert (
            connection.execute(
                sa.text('SELECT COUNT(*) FROM benchmark_monitoring_installation')
            ).scalar()
            == 1
        )
        migration.downgrade()
        assert sa.inspect(connection).get_table_names() == ['benchmarks']
        assert [
            c['name'] for c in sa.inspect(connection).get_columns('benchmarks')
        ] == ['id']


def migration_config(url=None):
    from alembic.config import Config

    config = Config()
    config.set_main_option(
        'script_location',
        str(Path(__file__).resolve().parents[2] / 'gpustack/migrations'),
    )
    if url is not None:
        config.set_main_option('sqlalchemy.url', url)
    return config


def test_already_applied_c8_head_has_one_separately_ordered_upgrade():
    from alembic.script import ScriptDirectory

    scripts = ScriptDirectory.from_config(migration_config())
    assert scripts.get_heads() == ['e7a9c3b50142']
    pending = list(scripts.iterate_revisions('head', 'c8d3e6f1a902'))
    assert [step.revision for step in pending] == ['e7a9c3b50142', 'd6f8b2a49031']
    assert pending[-1].down_revision == 'c8d3e6f1a902'


def test_alembic_upgrade_from_existing_c8_creates_archive_schema_only(tmp_path):
    from alembic import command

    path = tmp_path / 'already_at_c8.db'
    engine = sa.create_engine(f'sqlite:///{path}')
    with engine.begin() as connection:
        connection.execute(
            sa.text(
                'CREATE TABLE benchmarks (id INTEGER PRIMARY KEY, name VARCHAR(64))'
            )
        )
        connection.execute(
            sa.text(
                'CREATE TABLE model_instances ('
                'id INTEGER PRIMARY KEY, cluster_id INTEGER, model_id INTEGER, '
                'model_config_snapshot JSON)'
            )
        )
        for field in ('cluster_id', 'model_id'):
            connection.execute(
                sa.text(
                    f'CREATE INDEX ix_model_instances_{field} '
                    f'ON model_instances ({field})'
                )
            )
        connection.execute(sa.text("INSERT INTO benchmarks VALUES (1, 'existing-run')"))
        connection.execute(
            sa.text(
                'INSERT INTO model_instances VALUES '
                "(1, 2, 3, '{\"source\":\"captured-before-upgrade\"}')"
            )
        )
    config = migration_config(str(engine.url))
    command.stamp(config, 'c8d3e6f1a902')
    with engine.connect() as connection:
        assert (
            connection.execute(
                sa.text('SELECT version_num FROM alembic_version')
            ).scalar_one()
            == 'c8d3e6f1a902'
        )
        assert 'run_id' not in {
            column['name']
            for column in sa.inspect(connection).get_columns('benchmarks')
        }
    command.upgrade(config, 'head')
    with engine.connect() as connection:
        inspector = sa.inspect(connection)
        assert (
            connection.execute(
                sa.text('SELECT version_num FROM alembic_version')
            ).scalar_one()
            == 'e7a9c3b50142'
        )
        archive_tables = {
            'benchmark_monitoring_installation',
            'benchmark_monitoring_archives',
            'benchmark_monitoring_revisions',
            'benchmark_monitoring_chunks',
            'benchmark_monitoring_scopes',
        }
        assert archive_tables <= set(inspector.get_table_names())
        assert 'run_id' in {
            column['name'] for column in inspector.get_columns('benchmarks')
        }
        assert connection.execute(
            sa.text('SELECT name, run_id FROM benchmarks')
        ).one() == ('existing-run', None)
        assert (
            connection.execute(
                sa.text('SELECT model_config_snapshot FROM model_instances')
            ).scalar_one()
            == '{"source":"captured-before-upgrade"}'
        )
        assert {
            index['name'] for index in inspector.get_indexes('model_instances')
        } == {
            'ix_model_instances_cluster_id',
            'ix_model_instances_model_id',
        }
        namespace = connection.execute(
            sa.text(
                'SELECT namespace_id FROM benchmark_monitoring_installation WHERE id = 1'
            )
        ).scalar_one()
    command.upgrade(config, 'head')
    with engine.connect() as connection:
        assert (
            connection.execute(
                sa.text(
                    'SELECT namespace_id FROM benchmark_monitoring_installation WHERE id = 1'
                )
            ).scalar_one()
            == namespace
        )
    command.downgrade(config, 'c8d3e6f1a902')
    with engine.connect() as connection:
        assert (
            connection.execute(
                sa.text('SELECT version_num FROM alembic_version')
            ).scalar_one()
            == 'c8d3e6f1a902'
        )
        assert not archive_tables & set(sa.inspect(connection).get_table_names())
        assert [
            column['name']
            for column in sa.inspect(connection).get_columns('benchmarks')
        ] == ['id', 'name']
        assert (
            connection.execute(
                sa.text('SELECT model_config_snapshot FROM model_instances')
            ).scalar_one()
            == '{"source":"captured-before-upgrade"}'
        )
    engine.dispose()


@pytest.mark.parametrize('dialect', [postgresql.dialect(), mysql.dialect()])
def test_new_revision_emits_portable_archive_ddl(monkeypatch, dialect):
    from unittest.mock import MagicMock

    path = (
        Path(__file__).resolve().parents[2]
        / 'gpustack/migrations/versions'
        / '2026_11_17_0100-d6f8b2a49031_benchmark_monitoring_archives.py'
    )
    spec = importlib.util.spec_from_file_location('archive_ddl_migration', path)
    migration = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(migration)
    operations = MagicMock()
    operations.get_bind.return_value.execute.return_value.first.return_value = None
    metadata = sa.MetaData()
    emitted = []

    def create_table(name, *columns):
        emitted.append(sa.Table(name, metadata, *columns))

    operations.create_table.side_effect = create_table
    inspector = MagicMock()
    inspector.get_indexes.return_value = []
    monkeypatch.setattr(migration, 'op', operations)
    monkeypatch.setattr(migration, 'table_exists', lambda name: name == 'benchmarks')
    monkeypatch.setattr(migration, 'column_exists', lambda *args: False)
    monkeypatch.setattr(migration.sa, 'inspect', lambda bind: inspector)
    migration.upgrade()
    assert len(emitted) == 5
    assert operations.add_column.call_count == 1
    table, column = operations.add_column.call_args.args
    assert table == 'benchmarks' and column.name == 'run_id'
    assert column.nullable
    from sqlalchemy.schema import CreateColumn

    assert 'run_id VARCHAR(64)' in str(CreateColumn(column).compile(dialect=dialect))
    for archive_table in emitted:
        statement = str(CreateTable(archive_table).compile(dialect=dialect))
        assert archive_table.name in statement
        assert 'JSONB' not in statement
        assert 'FOREIGN KEY' not in statement
    assert operations.create_index.call_count == 7
