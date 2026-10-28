"""Optional full-source acceptance check: python -m tests.verify_full --db data/check.duckdb."""

import argparse
from pathlib import Path
from unittest.mock import patch

import duckdb

from dashboard import queries
from pipeline import ROOT
from pipeline.run import rebuild


def verify(db: Path) -> dict:
    with patch('urllib.request.urlopen', side_effect=AssertionError('Real HTTP forbidden')):
        first = rebuild(ROOT / 'revenues_per_day.csv', db)
        with duckdb.connect(str(db)) as con:
            # Keep exact comparison rows in memory; do not create operational tables.
            snapshots = {name: con.execute(f'SELECT * FROM {name} ORDER BY 1').fetchall()
                         for name in ('dim_movie', 'dim_date', 'dim_distributor', 'fact_daily_revenue')}
        second = rebuild(ROOT / 'revenues_per_day.csv', db)
        assert first == second, (first, second)
        assert first['fact_rows'] == 337818, first
        assert first['total_revenue'] == 205110995141, first
        assert first['enrichment']['requests'] == 0, first
        with duckdb.connect(str(db)) as con:
            for name, rows in snapshots.items():
                assert con.execute(f'SELECT * FROM {name} ORDER BY 1').fetchall() == rows, name
            assert con.execute('SELECT count(*) FROM movies_state.omdb_requests').fetchone()[0] == 0
            start, end = queries.date_bounds(con)
            assert queries.overview(con, start, end)['revenue'] == 205110995141
            assert sum(r['revenue'] for r in queries.monthly_trend(con, start, end)) == 205110995141
            assert sum(r['revenue'] for r in queries.distributor_ranking(con, start, end, limit=1000)) == 205110995141
            for function in (queries.movie_ranking, queries.film_ranking, queries.enrichment_status):
                function(con, start, end)
            stats = con.execute('SELECT count(DISTINCT source_title), count(DISTINCT distributor_name), '
                                'count(DISTINCT revenue_date), count(*) FILTER (WHERE theaters IS NULL) '
                                'FROM movies_state.raw_revenues').fetchone()
    return dict(first, source_titles=stats[0], distributors=stats[1], dates=stats[2], missing_theaters=stats[3])


if __name__ == '__main__':
    import json
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--db', type=Path, required=True, help='New isolated verification database')
    arguments = parser.parse_args()
    if arguments.db.exists():
        parser.error('Choose a new database path for verification')
    print(json.dumps(verify(arguments.db), indent=2))
