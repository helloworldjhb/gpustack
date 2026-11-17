"""Persist benchmark execution identities and monitoring archives.

Revision ID: d6f8b2a49031
Revises: c8d3e6f1a902
Create Date: 2026-11-17 01:00:00.000000
"""

from typing import Sequence, Union
from uuid import uuid4

from alembic import op
import sqlalchemy as sa

from gpustack.migrations.utils import column_exists, table_exists

revision: str = 'd6f8b2a49031'
down_revision: Union[str, None] = 'c8d3e6f1a902'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    if table_exists('benchmarks') and not column_exists('benchmarks', 'run_id'):
        op.add_column('benchmarks', sa.Column('run_id', sa.String(64), nullable=True))
    tables = {
        'benchmark_monitoring_installation': [
            sa.Column('id', sa.Integer(), primary_key=True),
            sa.Column('namespace_id', sa.String(36), nullable=False),
        ],
        'benchmark_monitoring_archives': [
            sa.Column('id', sa.String(36), primary_key=True),
            sa.Column('benchmark_id', sa.Integer(), nullable=False),
            sa.Column('run_id', sa.String(64), nullable=False),
            sa.Column('owner_principal_id', sa.Integer(), nullable=True),
            sa.Column('profile_id', sa.String(128), nullable=False),
            sa.Column('profile_fingerprint', sa.String(64), nullable=False),
            sa.Column('state', sa.String(32), nullable=False),
            sa.Column('revision', sa.Integer(), nullable=False),
            sa.Column('checkpoint', sa.Integer(), nullable=False),
            sa.Column('captured_until', sa.Float(), nullable=True),
            sa.Column('snapshot', sa.JSON(), nullable=False),
            sa.Column('plan', sa.JSON(), nullable=False),
            sa.Column('lease_owner', sa.String(36), nullable=True),
            sa.Column('lease_until', sa.Float(), nullable=False),
            sa.Column('fencing_token', sa.Integer(), nullable=False),
            sa.Column('retry_at', sa.Float(), nullable=False),
            sa.Column('attempts', sa.Integer(), nullable=False),
            sa.Column('last_error', sa.String(128), nullable=True),
            sa.Column('delete_requested', sa.Boolean(), nullable=False),
            sa.UniqueConstraint(
                'benchmark_id', 'run_id', name='uq_benchmark_archive_run'
            ),
        ],
        'benchmark_monitoring_revisions': [
            sa.Column('id', sa.String(36), primary_key=True),
            sa.Column('archive_id', sa.String(36), nullable=False),
            sa.Column('revision', sa.Integer(), nullable=False),
            sa.Column('state', sa.String(32), nullable=False),
            sa.Column('data_coverage', sa.String(16), nullable=False),
            sa.Column('captured_until', sa.Float(), nullable=True),
            sa.Column('context', sa.JSON(), nullable=False),
            sa.Column('scopes', sa.JSON(), nullable=False),
            sa.Column('created_at', sa.Float(), nullable=False),
            sa.UniqueConstraint(
                'archive_id', 'revision', name='uq_benchmark_archive_revision'
            ),
        ],
        'benchmark_monitoring_chunks': [
            sa.Column('id', sa.String(36), primary_key=True),
            sa.Column('archive_id', sa.String(36), nullable=False),
            sa.Column('scope', sa.String(160), nullable=False),
            sa.Column('profile_id', sa.String(128), nullable=False),
            sa.Column('object_key', sa.String(1024), nullable=False),
            sa.Column('sha256', sa.String(64), nullable=False),
            sa.Column('size_bytes', sa.Integer(), nullable=False),
            sa.Column('uncompressed_size_bytes', sa.Integer(), nullable=False),
            sa.Column('start', sa.Float(), nullable=True),
            sa.Column('end', sa.Float(), nullable=True),
            sa.Column('created_at', sa.Float(), nullable=False),
            sa.Column('published', sa.Boolean(), nullable=False),
        ],
        'benchmark_monitoring_scopes': [
            sa.Column('id', sa.String(36), primary_key=True),
            sa.Column('archive_id', sa.String(36), nullable=False),
            sa.Column('data', sa.JSON(), nullable=False),
        ],
    }
    indexes = {
        'benchmark_monitoring_installation': (),
        'benchmark_monitoring_archives': ('benchmark_id', 'state', 'retry_at'),
        'benchmark_monitoring_revisions': ('archive_id',),
        'benchmark_monitoring_chunks': ('archive_id', 'scope'),
        'benchmark_monitoring_scopes': ('archive_id',),
    }
    for name, columns in tables.items():
        if not table_exists(name):
            op.create_table(name, *columns)
        names = {i['name'] for i in sa.inspect(op.get_bind()).get_indexes(name)}
        for field in indexes[name]:
            index = f'ix_{name}_{field}'
            if index not in names:
                op.create_index(index, name, [field])
    installation = sa.table(
        'benchmark_monitoring_installation', sa.column('id'), sa.column('namespace_id')
    )
    if (
        op.get_bind()
        .execute(sa.select(installation.c.id).where(installation.c.id == 1))
        .first()
        is None
    ):
        op.bulk_insert(installation, [{'id': 1, 'namespace_id': str(uuid4())}])


def downgrade() -> None:
    for name in (
        'benchmark_monitoring_chunks',
        'benchmark_monitoring_scopes',
        'benchmark_monitoring_revisions',
        'benchmark_monitoring_archives',
        'benchmark_monitoring_installation',
    ):
        if table_exists(name):
            op.drop_table(name)
    if table_exists('benchmarks') and column_exists('benchmarks', 'run_id'):
        op.drop_column('benchmarks', 'run_id')
