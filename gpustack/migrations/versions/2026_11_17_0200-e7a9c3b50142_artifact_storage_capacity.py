"""Account shared artifact capacity independently of Benchmark report metadata.

Revision ID: e7a9c3b50142
Revises: d6f8b2a49031
Create Date: 2026-11-17 02:00:00.000000
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

from gpustack.migrations.utils import table_exists

revision: str = 'e7a9c3b50142'
down_revision: Union[str, None] = 'd6f8b2a49031'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    if not table_exists('artifact_storage_capacity'):
        op.create_table(
            'artifact_storage_capacity',
            sa.Column('id', sa.Integer(), primary_key=True),
            sa.Column('max_bytes', sa.BigInteger(), nullable=False),
            sa.Column('used_bytes', sa.BigInteger(), nullable=False),
            sa.Column('reserved_bytes', sa.BigInteger(), nullable=False),
            sa.Column('initialized', sa.Boolean(), nullable=False),
        )
    if not table_exists('artifact_storage_objects'):
        op.create_table(
            'artifact_storage_objects',
            sa.Column('id', sa.String(64), primary_key=True),
            sa.Column('archive_id', sa.String(36), nullable=True),
            sa.Column('profile_id', sa.String(128), nullable=False),
            sa.Column('object_key', sa.String(1024), nullable=False),
            sa.Column('size_bytes', sa.BigInteger(), nullable=False),
            sa.Column('state', sa.String(16), nullable=False),
        )
    index = 'ix_artifact_storage_objects_archive_id'
    if index not in {
        item['name']
        for item in sa.inspect(op.get_bind()).get_indexes('artifact_storage_objects')
    }:
        op.create_index(index, 'artifact_storage_objects', ['archive_id'])
    table = sa.table(
        'artifact_storage_capacity',
        sa.column('id'),
        sa.column('max_bytes'),
        sa.column('used_bytes'),
        sa.column('reserved_bytes'),
        sa.column('initialized'),
    )
    if op.get_bind().execute(sa.select(table.c.id).where(table.c.id == 1)).first() is None:
        op.bulk_insert(
            table,
            [{'id': 1, 'max_bytes': 0, 'used_bytes': 0, 'reserved_bytes': 0, 'initialized': False}],
        )


def downgrade() -> None:
    # Object contents and the Benchmark indexes are retained on downgrade.
    for name in ('artifact_storage_objects', 'artifact_storage_capacity'):
        if table_exists(name):
            op.drop_table(name)
