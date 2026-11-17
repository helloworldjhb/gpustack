"""Explicit administrative archive migration and integrity checks."""

import argparse
import asyncio
import json
import logging
import os
from pathlib import Path
import sys

from pydantic_settings import BaseSettings
from sqlmodel.ext.asyncio.session import AsyncSession

from gpustack.artifacts import ArtifactStorageError
from gpustack.artifacts.capacity import CapacityManager
from gpustack.artifacts.maintenance import ArchiveMaintenance
from gpustack.cmd.start import load_config_from_yaml
from gpustack.config.config import Config
from gpustack.utils.envs import get_gpustack_env


class MaintenanceConfig(Config):
    """Validate settings without starting auth providers or creating server state."""

    def __init__(self, **values):
        BaseSettings.__init__(self, **values)
        if self.data_dir is None:
            self.data_dir = self.get_data_dir()


def setup_artifact_storage_cmd(subparsers: argparse._SubParsersAction):
    parser = subparsers.add_parser(
        'artifact-storage', help='Audit or migrate archived artifacts.'
    )
    parser.add_argument('--config-file', default=get_gpustack_env('CONFIG_FILE'))
    parser.add_argument('--database-url', default=get_gpustack_env('DATABASE_URL'))
    parser.add_argument('--data-dir', default=get_gpustack_env('DATA_DIR'))
    operations = parser.add_subparsers(dest='artifact_operation', required=True)
    migration = operations.add_parser(
        'migrate-profile',
        help='Copy and verify terminal archives; defaults to dry run.',
    )
    migration.add_argument('--source-id', required=True)
    migration.add_argument('--target-id', required=True)
    action = migration.add_mutually_exclusive_group()
    action.add_argument(
        '--apply',
        action='store_true',
        help='Copy objects and atomically switch verified archive indexes.',
    )
    action.add_argument(
        '--dry-run',
        action='store_true',
        help='Report selection without writes (the default).',
    )
    audit = operations.add_parser(
        'audit',
        aliases=['check'],
        help='Verify current indexed object checksums without writes.',
    )
    audit.add_argument('--profile-id')
    capacity = operations.add_parser(
        'capacity',
        help='Show shared capacity or set its byte limit (0 means unlimited).',
    )
    capacity.add_argument('--max-bytes', type=int)
    capacity.add_argument('--manifest')
    capacity.set_defaults(func=run)
    for command in (migration, audit):
        command.add_argument('--archive-id')
        command.add_argument('--benchmark-id', type=int)
        command.add_argument('--run-id')
        command.add_argument('--limit', type=int, default=100)
        command.add_argument(
            '--manifest',
            help='Write a private JSON operation report to this local path.',
        )
        command.set_defaults(func=run)


def load_maintenance_config(args) -> Config:
    """Load normal YAML/env settings with CLI connection overrides."""
    values = load_config_from_yaml(args.config_file) if args.config_file else {}
    if values is None:
        values = {}
    if not isinstance(values, dict):
        raise ArtifactStorageError('invalid_configuration')
    names = ['database_url', 'data_dir'] + [
        name for name in Config.model_fields if name.startswith('artifact_storage_')
    ]
    for name in names:
        value = getattr(args, name, None)
        if value is not None:
            values[name] = value
    return MaintenanceConfig(**values)


async def _run(args):
    # Connection setup can report URL query values. This administrator command
    # emits its own safe failure reason and never prints driver exceptions.
    logging.getLogger('gpustack.server.init_db').disabled = True
    from gpustack.server.init_db import init_db_engine

    config = load_maintenance_config(args)
    engine = await init_db_engine(config.get_database_url())
    try:
        if args.artifact_operation == 'capacity':
            manager = CapacityManager(
                config,
                session_factory=lambda: AsyncSession(engine, expire_on_commit=False),
            )
            if args.max_bytes is not None:
                return await manager.set_limit(args.max_bytes)
            return await manager.usage()
        maintenance = ArchiveMaintenance(
            config, session_factory=lambda: AsyncSession(engine, expire_on_commit=False)
        )
        filters = {
            name: getattr(args, name, None)
            for name in ('archive_id', 'benchmark_id', 'run_id', 'limit')
        }
        if args.artifact_operation == 'migrate-profile':
            return await maintenance.migrate_profile(
                args.source_id, args.target_id, dry_run=not args.apply, **filters
            )
        return await maintenance.audit(profile_id=args.profile_id, **filters)
    finally:
        await engine.dispose()


def _report(result, path):
    content = json.dumps(result, indent=2, sort_keys=True)
    if path:
        # Operation reports contain internal archive IDs and keys, so retain the
        # same private file mode used for other administrative local artifacts.
        output = Path(path)
        descriptor = os.open(
            output,
            os.O_WRONLY | os.O_CREAT | os.O_TRUNC | getattr(os, 'O_NOFOLLOW', 0),
            0o600,
        )
        with os.fdopen(descriptor, 'w') as destination:
            os.chmod(output, 0o600)
            destination.write(content + '\n')
    print(content)


def run(args):
    try:
        result = asyncio.run(_run(args))
        _report(result, args.manifest)
        if result.get('issues'):
            raise SystemExit(2)
    except ArtifactStorageError as error:
        print(f'Artifact storage maintenance failed: {error.reason}', file=sys.stderr)
        raise SystemExit(1) from None
    except Exception:
        print(
            'Artifact storage maintenance failed: maintenance_unavailable',
            file=sys.stderr,
        )
        raise SystemExit(1) from None
