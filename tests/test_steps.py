"""Check the stage boundary, offline guard, validation and failure propagation."""

from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

import duckdb

from pipeline import ROOT
from pipeline.steps import DEFAULT_DB, run_step, validate


class StepTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.db = Path(temporary.name) / 'steps.duckdb'

    def test_independent_steps_offline_and_validation(self):
        with patch.dict('os.environ', {'OMDB_API_KEY': 'must-never-be-used'}), patch(
            'urllib.request.urlopen', side_effect=AssertionError('Real HTTP forbidden')
        ) as http:
            self.assertEqual(run_step('ingest', self.db, csv_path=ROOT / 'tests/fixtures/revenues.csv'),
                             {'rows': 6, 'total_revenue': 335})
            run_step('dbt_prepare', self.db)
            enrichment = run_step('enrich_omdb', self.db)
            self.assertEqual(enrichment['requests'], 0)
            http.assert_not_called()
            run_step('dbt_build', self.db)
        expected = dict(expected_rows=6, expected_revenue=335, expected_periods=3)
        self.assertEqual(validate(self.db, **expected),
                         dict(fact_rows=6, total_revenue=335, periods=3, omdb_requests=0))
        with self.assertRaisesRegex(ValueError, 'Dataset validation failed'):
            validate(self.db)  # The fixture must not pass complete-dataset acceptance.
        with duckdb.connect(str(self.db)) as con:
            con.execute('UPDATE movies_state.raw_revenues SET revenue=revenue+1')
        with self.assertRaisesRegex(ValueError, 'Dataset validation failed'):
            validate(self.db, **expected)
        with duckdb.connect(str(self.db)) as con:
            con.execute('UPDATE movies_state.raw_revenues SET revenue=revenue-1')
            con.execute("INSERT INTO movies_state.omdb_requests VALUES "
                        "('test', now(), 'test', 2020, 'reserved')")
        with self.assertRaisesRegex(ValueError, 'OMDb request reservations'):
            validate(self.db, **expected)

    def test_dbt_failures_escape_each_stage(self):
        with patch('pipeline.steps.run_dbt', side_effect=RuntimeError('dbt failed')) as dbt:
            for step, arguments in [('dbt_prepare', ('run', '--select', '+int_omdb_candidates')),
                                    ('dbt_build', ('build',))]:
                with self.subTest(step=step), self.assertRaisesRegex(RuntimeError, 'dbt failed'):
                    run_step(step, self.db)
                dbt.assert_called_with(self.db, *arguments)

    def test_missing_input_cli_fails_without_creating_database(self):
        result = subprocess.run(
            [sys.executable, '-m', 'pipeline.steps', 'ingest', '--db', str(self.db),
             '--csv', str(self.db.with_suffix('.missing.csv'))],
            cwd=ROOT, capture_output=True, text=True)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn('FileNotFoundError', result.stderr)
        self.assertFalse(self.db.exists())

    def test_dedicated_default_and_unknown_stage(self):
        self.assertEqual(DEFAULT_DB, ROOT / 'data/movies_steps.duckdb')
        with self.assertRaisesRegex(ValueError, 'Unknown pipeline step'):
            run_step('typo', self.db)
        self.assertFalse(self.db.exists())
