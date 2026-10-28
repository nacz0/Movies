"""Network-free integration checks for dbt, migration and atomic publication."""

import csv
from datetime import date
import hashlib
from pathlib import Path
import shutil
import tempfile
import unittest
from unittest.mock import patch

import duckdb

from dashboard import queries
from pipeline import ROOT
from pipeline.dbt_runner import run_dbt
from pipeline.enrich_omdb import enrich
from pipeline.run import rebuild, restore, working_path
from pipeline.state import migrate

FIXTURE = ROOT / 'tests/fixtures/dbt_revenues.csv'


def payload(title, year=2020):
    return dict(Response='True', Type='movie', Title=title, Year=str(year),
                imdbID='tt1234567', Genre='Drama', imdbRating='7.2', Runtime='100 min')


class DbtTests(unittest.TestCase):
    def setUp(self):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        self.root = Path(temp.name)
        self.db = self.root / 'warehouse.duckdb'
        self.csv = self.root / 'source.csv'
        shutil.copyfile(FIXTURE, self.csv)

    def query(self, sql, db=None):
        with duckdb.connect(str(db or self.db)) as con:
            return con.execute(sql).fetchall()

    def edit(self, rows):
        with self.csv.open('w', newline='', encoding='utf-8') as handle:
            writer = csv.writer(handle)
            writer.writerow(['id','date','title','revenue','theaters','distributor'])
            writer.writerows(rows)

    def test_period_boundaries_full_refresh_and_dashboard(self):
        run_dbt(self.db, 'debug', target='ci')
        run_dbt(self.db, 'parse', target='ci')
        first = rebuild(self.csv, self.db, target='ci')
        self.assertEqual((first['fact_rows'], first['periods'], first['total_revenue']), (6, 3, 335))
        # Exactly 180 days stays in the period; 181 days starts another.
        self.assertEqual(self.query("SELECT run_start_date,run_end_date,display_title FROM dim_movie "
                                    "WHERE source_title='Same' ORDER BY run_start_date"), [
            (date(2020,1,1), date(2020,6,29), 'Same (2020-01-01)'),
            (date(2020,12,27), date(2021,1,1), 'Same (2020-12-27)')])
        before = self.query('SELECT * FROM dim_movie ORDER BY movie_key')
        distributors = self.query('SELECT * FROM dim_distributor ORDER BY distributor_key')
        second = rebuild(self.csv, self.db, full_refresh=True)
        self.assertEqual(first, second)
        self.assertEqual(before, self.query('SELECT * FROM dim_movie ORDER BY movie_key'))
        self.assertEqual(distributors, self.query('SELECT * FROM dim_distributor ORDER BY distributor_key'))
        with duckdb.connect(str(self.db)) as con:
            start,end = queries.date_bounds(con)
            self.assertEqual(queries.overview(con,start,end)['revenue'],335)
            self.assertEqual(queries.overview(con,start,end,['Studio Y'])['revenue'],70)
            trend=queries.monthly_trend(con,start,end)
            self.assertEqual(len(trend),13)
            self.assertIn(0,[r['revenue'] for r in trend])
            self.assertEqual(queries.movie_ranking(con,start,end,limit=1,sort='movie',direction='desc')[0]['revenue'],90)
            self.assertEqual(con.execute("SELECT table_schema FROM information_schema.tables "
                "WHERE table_name='fact_daily_revenue'").fetchone(),('main',))
        # Negative control: the published quality tests must actually detect
        # corrupt values and broken dimension relationships.
        self.query("UPDATE fact_daily_revenue SET revenue=-1,theaters=0 WHERE source_id='a'")
        self.query("UPDATE fact_daily_revenue SET movie_key=-999 WHERE source_id='b'")
        with self.assertRaisesRegex(RuntimeError,'dbt test') as failed:
            run_dbt(self.db,'test','--select','valid_values','reconcile_rows',
                    'reconcile_totals','relationships_fact_daily_revenue_movie_key*')
        for name in ('valid_values','reconcile_rows','reconcile_totals',
                     'relationships_fact_daily_revenue_movie_key'):
            self.assertIn('Failure in test '+name,str(failed.exception))

    def test_deletion_correction_history_and_empty_snapshot(self):
        rebuild(self.csv,self.db)
        old = self.query('SELECT * FROM dim_movie ORDER BY movie_key')
        self.edit([['a','2020-01-01','Same',123,'','Studio X']])
        result=rebuild(self.csv,self.db)
        self.assertEqual((result['fact_rows'], result['total_revenue']),(1,123))
        self.assertEqual(self.query('SELECT count(*) FROM dim_movie'),[(3,)])
        self.assertEqual(self.query('SELECT count(*) FROM dim_date'),[(4,)])
        self.assertEqual([r[0] for r in old], [r[0] for r in self.query('SELECT * FROM dim_movie ORDER BY movie_key')])
        self.edit([])
        with self.assertRaises(ValueError):
            rebuild(self.csv,self.db)
        self.assertEqual(self.query('SELECT sum(revenue) FROM fact_daily_revenue'),[(123,)])

    def test_enriched_assignment_guard_survives_deleted_marts(self):
        rebuild(self.csv,self.db)
        enrich(self.db,'fixture',limit=10,fetcher=lambda title,*_:payload(title))
        original=hashlib.sha256(self.db.read_bytes()).hexdigest()
        # Earlier date moves enriched source row a into a new natural period key.
        with self.csv.open(newline='',encoding='utf-8') as handle:
            rows=list(csv.reader(handle))[1:]
        self.edit(rows+[['new','2019-12-31','Same',1,1,'Studio X']])
        with self.assertRaisesRegex(ValueError,'Reporting period changed'):
            rebuild(self.csv,self.db)
        self.assertEqual(hashlib.sha256(self.db.read_bytes()).hexdigest(),original)
        work=working_path(self.db)
        self.query('DROP TABLE fact_daily_revenue; DROP TABLE dim_movie', work)
        with self.assertRaisesRegex(ValueError,'Reporting period changed'):
            rebuild(self.csv,self.db,resume=True)

    def test_failure_after_api_preserves_budget_and_resume(self):
        rebuild(self.csv,self.db)
        old=self.query('SELECT * FROM dim_movie ORDER BY movie_key')
        real=run_dbt
        def fail_marts(path,*args,**kwargs):
            if args == ('build',):
                raise RuntimeError('injected mart failure')
            return real(path,*args,**kwargs)
        with patch('pipeline.run.run_dbt',side_effect=fail_marts):
            with self.assertRaisesRegex(RuntimeError,'injected'):
                rebuild(None,self.db,enrichment=dict(api_key='fixture',limit=1,daily_cap=1,
                    fetcher=lambda title,*_:payload(title)))
        self.assertEqual(old,self.query('SELECT * FROM dim_movie ORDER BY movie_key'))
        work=working_path(self.db)
        self.assertEqual(self.query('SELECT count(*) FROM omdb_request_log',work),[(1,)])
        with self.assertRaisesRegex(RuntimeError,'Unfinished'):
            rebuild(None,self.db)
        result=rebuild(None,self.db,resume=True,enrichment=dict(api_key='fixture',limit=1,daily_cap=1,
            fetcher=lambda *_:self.fail('spent budget reused')))
        self.assertEqual(result['enrichment']['requests_last_24h'],1)
        self.assertEqual(self.query('SELECT count(*) FROM omdb_request_log'),[(1,)])
        self.assertTrue(self.db.with_name(self.db.name+'.backup.duckdb').exists())
        # Recovery must also retain a reservation that exists only in a failed
        # working copy, while restoring the earlier selected movie metadata.
        shutil.copy2(self.db,work)
        self.query("INSERT INTO omdb_request_log VALUES ('only-in-work',999,current_timestamp,'reserved',NULL,'title')",work)
        restore(self.db)
        self.assertEqual(self.query('SELECT count(*) FROM omdb_request_log'),[(2,)])
        self.assertEqual(self.query("SELECT count(*) FROM movie_metadata WHERE match_status='pending'"),[(3,)])
        self.assertFalse(work.exists())
        self.assertEqual(len(list(self.root.glob('warehouse.failed-*.duckdb'))),1)
        self.assertGreater(self.query('SELECT count(*) FROM omdb_query_cache')[0][0],0)

    def test_saved_database_migration_preserves_keys_statuses_payloads_and_ledger(self):
        with duckdb.connect(str(self.db)) as con:
            con.execute((ROOT/'tests/fixtures/pre_dbt.sql').read_text())
            con.execute((ROOT/'sql/omdb.sql').read_text().replace(
                'REFERENCES movie_registry(movie_key)','REFERENCES dim_movie(movie_key)'))
            con.execute("INSERT INTO dim_movie (movie_key,source_title,run_start_date,run_end_date,display_title,match_status) "
                        "VALUES (42,'Same','2020-01-01','2020-06-29','Same','retry_later')")
            con.execute("INSERT INTO dim_distributor VALUES (91,'Studio X')")
            con.execute("INSERT INTO dim_date VALUES (20200101,'2020-01-01',2020,1,1,3)")
            con.execute("INSERT INTO fact_daily_revenue VALUES ('a',20200101,42,91,100,10)")
            con.execute("INSERT INTO raw_revenues VALUES ('a','2020-01-01','Same',100,10,'Studio X')")
            con.execute("INSERT INTO omdb_lookup VALUES (42,'ambiguous','tt1234567','Same','review','{}',current_timestamp,2020)")
            con.execute("INSERT INTO omdb_request_log VALUES ('saved',42,current_timestamp,'reserved',NULL,'title')")
            con.execute("INSERT INTO omdb_query_cache VALUES ('Same',2020,'{}',current_timestamp)")
            con.execute('CREATE TABLE omdb_search_cache (movie_key BIGINT REFERENCES dim_movie(movie_key), note VARCHAR)')
            con.execute("INSERT INTO omdb_search_cache VALUES (42,'saved review data')")
            expected={t:con.execute(f'SELECT * FROM {t}').fetchall()
                      for t in ['omdb_lookup','omdb_request_log','omdb_query_cache','omdb_search_cache']}
            # The whole migration, including DDL, rolls back on a late failure.
            class FailingConnection:
                def execute(self, sql, *args):
                    if sql.startswith('CREATE SEQUENCE registry_'):
                        raise RuntimeError('injected migration failure')
                    return con.execute(sql,*args)
            with self.assertRaisesRegex(RuntimeError,'injected migration'):
                migrate(FailingConnection())
            self.assertEqual(con.execute('SELECT movie_key FROM dim_movie').fetchall(),[(42,)])
            migrate(con)
            migrate(con)
            for table, rows in expected.items():
                self.assertEqual(rows,con.execute(f'SELECT * FROM {table}').fetchall())
            self.assertEqual(con.execute('SELECT movie_key,match_status FROM movie_metadata').fetchall(),[(42,'retry_later')])
        rebuild(self.csv,self.db)
        self.assertEqual(self.query("SELECT movie_key FROM dim_movie WHERE source_title='Same' AND run_start_date='2020-01-01'"),[(42,)])
        self.assertEqual(self.query("SELECT distributor_key FROM dim_distributor WHERE distributor_name='Studio X'"),[(91,)])
        self.assertGreater(self.query('SELECT max(movie_key) FROM dim_movie')[0][0],42)
        run_dbt(self.db,'build','--full-refresh')
        for table, rows in expected.items():
            self.assertEqual(rows,self.query(f'SELECT * FROM {table}'))


if __name__ == '__main__':
    unittest.main()
