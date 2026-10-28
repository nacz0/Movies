import csv
from pathlib import Path
import tempfile
import unittest

import duckdb

from pipeline.ingest import import_snapshot, initialize


class IngestTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.db = Path(temporary.name) / 'test.duckdb'
        self.csv = Path(temporary.name) / 'test.csv'
        self.good = [['00001', '2020-01-01', ' Exact title ', '9007199254740993', '', 'Studio']]
        self.write(self.good)

    def write(self, rows, header=None):
        with self.csv.open('w', newline='', encoding='utf-8') as handle:
            writer = csv.writer(handle)
            writer.writerow(header or ['id', 'date', 'title', 'revenue', 'theaters', 'distributor'])
            writer.writerows(rows)

    def query(self, sql):
        with duckdb.connect(str(self.db)) as con:
            return con.execute(sql).fetchall()

    def test_exact_values_and_repeated_snapshot(self):
        for _ in range(2):
            result = import_snapshot(self.csv, self.db)
            self.assertEqual(result, dict(rows=1, total_revenue=9007199254740993))
            self.assertEqual(self.query('SELECT source_id, source_title, revenue, theaters '
                                        'FROM movies_state.raw_revenues'),
                             [('00001', ' Exact title ', 9007199254740993, None)])

    def test_invalid_snapshots_do_not_change_saved_rows(self):
        import_snapshot(self.csv, self.db)
        before = self.query('SELECT * FROM movies_state.raw_revenues')
        invalid_rows = [[], self.good * 2]
        for column, value in [(0, ''), (1, '2020-02-30'), (1, '01/01/2020'),
                              (2, ' '), (3, '-1'), (3, '1.5'), (3, ''),
                              (3, '9223372036854775808'), (4, '0'), (4, '1.5'), (5, '')]:
            row = self.good[0].copy()
            row[column] = value
            invalid_rows.append([row])
        duplicate = self.good[0].copy()
        duplicate[0] = 'different-id'
        invalid_rows.append([self.good[0], duplicate])
        for rows in invalid_rows:
            with self.subTest(rows=rows):
                self.write(rows)
                with self.assertRaises((ValueError, duckdb.Error)):
                    import_snapshot(self.csv, self.db)
                self.assertEqual(self.query('SELECT * FROM movies_state.raw_revenues'), before)

    def test_header_empty_file_and_malformed_csv(self):
        import_snapshot(self.csv, self.db)
        for content in ['', 'unexpected,header\n1,2\n', 'id,date,title,revenue,theaters,distributor\n"unterminated']:
            with self.subTest(content=content):
                self.csv.write_text(content, encoding='utf-8')
                with self.assertRaises((ValueError, duckdb.Error)):
                    import_snapshot(self.csv, self.db)
                self.assertEqual(self.query('SELECT count(*) FROM movies_state.raw_revenues'), [(1,)])

    def test_corrections_deletions_and_zero_gross(self):
        import_snapshot(self.csv, self.db)
        self.write([['new-id', '2021-01-01', 'New', '0', '1', 'New studio']])
        import_snapshot(self.csv, self.db)
        self.assertEqual(self.query('SELECT source_id, revenue FROM movies_state.raw_revenues'), [('new-id', 0)])

    def test_existing_foreign_database_is_refused_without_changes(self):
        with duckdb.connect(str(self.db)) as con:
            con.execute('CREATE TABLE precious AS SELECT 42 AS value')
        before = self.db.read_bytes()
        with self.assertRaisesRegex(ValueError, 'NEW --db'):
            initialize(self.db)
        self.assertEqual(self.db.read_bytes(), before)
        self.assertEqual(self.query('SELECT * FROM precious'), [(42,)])
