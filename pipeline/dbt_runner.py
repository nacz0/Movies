"""Run the pinned dbt Core against one explicitly selected absolute database."""

import hashlib
import os
from pathlib import Path
import subprocess
import sys
import tempfile

from pipeline import ROOT


def run_dbt(db_path: Path, *arguments: str, target: str = 'dev') -> None:
    db_path = db_path.resolve()
    if target not in ('dev', 'ci'):
        raise ValueError('dbt target must be dev or ci')
    environment = dict(os.environ, MOVIES_DBT_PATH=str(db_path),
                       MOVIES_DBT_TARGET=target, DBT_SEND_ANONYMOUS_USAGE_STATS='false',
                       PYTHONIOENCODING='utf-8')
    identity = hashlib.sha256(str(db_path).encode()).hexdigest()[:12]
    if arguments and arguments[0] in ('build', 'test'):
        # Early phases must not run mart tests merely because they also read
        # a selected staging model. Tests explicitly tagged for a phase still run.
        arguments = (*arguments, '--indirect-selection', 'cautious')
    with tempfile.TemporaryDirectory(prefix='movies-dbt-profile-') as directory:
        Path(directory, 'profiles.yml').write_text(
            (ROOT / 'dbt/profiles.yml.example').read_text(encoding='utf-8'), encoding='utf-8')
        command = [sys.executable, '-c', 'from dbt.cli.main import cli; cli()',
                   '--no-use-colors', '--quiet', *arguments,
                   '--project-dir', str(ROOT / 'dbt'), '--profiles-dir', directory,
                   '--target', target,
                   '--log-path', str(ROOT / 'dbt/logs' / identity)]
        if not arguments or arguments[0] != 'debug':
            command += ['--target-path', str(ROOT / 'dbt/target' / identity)]
        result = subprocess.run(command, env=environment, cwd=ROOT, capture_output=True,
                                text=True, encoding='utf-8', errors='replace')
        if result.returncode:
            detail = (result.stdout + result.stderr)[-12000:]
            raise RuntimeError(f'dbt {" ".join(arguments)} failed (exit {result.returncode}):\n{detail}')
