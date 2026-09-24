import csv
import tempfile
import unittest
from datetime import date
from pathlib import Path

import duckdb

from dashboard.queries import film_ranking, movie_ranking
from pipeline.enrich_omdb import enrich
from pipeline.film_identity import backfill
from pipeline.load_revenues import load
from pipeline.review_matches import decide, undo


class FilmIdentityTests(unittest.TestCase):
    def test_backfill_restores_old_run_model_without_changing_facts_or_history(self) -> None:
        with tempfile.TemporaryDirectory() as root:
            csv_path = Path(root) / "revenues.csv"
            db_path = Path(root) / "warehouse.duckdb"
            with csv_path.open("w", newline="", encoding="utf-8") as handle:
                writer = csv.writer(handle)
                writer.writerow(["id", "date", "title", "revenue", "theaters", "distributor"])
                writer.writerows([
                    ["1", "2020-01-01", "Film A", "100", "1", "Studio"],
                    ["2", "2023-01-01", "Film A", "50", "1", "Studio"],
                ])
            load(csv_path, db_path)

            def fake_fetch(title: str, api_key: str) -> dict:
                return {"Response": "True", "Type": "movie", "imdbID": "tt1111111",
                        "Title": title, "Year": "2020", "Genre": "Drama"}

            enrich(db_path, "fake-key", limit=1, daily_cap=1, fetcher=fake_fetch)
            with duckdb.connect(str(db_path)) as con:
                old_keys = con.execute("SELECT movie_key FROM dim_movie ORDER BY movie_key").fetchall()
                con.execute("DROP VIEW v_movie_match_review")
                con.execute("DROP VIEW v_run_film_match")
                con.execute("DROP TABLE dim_release_run")
            first = backfill(db_path)
            self.assertEqual((first["fact_rows"], first["omdb_requests"],
                              first["confirmed_runs"]), (2, 1, 1))
            with duckdb.connect(str(db_path)) as con:
                self.assertEqual(con.execute("""SELECT COUNT(*), SUM(revenue)
                    FROM fact_daily_revenue""").fetchone(), (2, 150))
                self.assertEqual(con.execute("""SELECT movie_key FROM dim_release_run
                    ORDER BY movie_key""").fetchall(), old_keys)
                self.assertEqual(film_ranking(con, date(2020, 1, 1),
                                              date(2023, 12, 31))[0]["revenue"], 100)
                con.execute("""UPDATE dim_release_run SET run_start_date = '2019-12-31'
                    WHERE movie_key = (SELECT min(movie_key) FROM dim_release_run)""")
            self.assertEqual(backfill(db_path), first)
            with duckdb.connect(str(db_path), read_only=True) as con:
                self.assertEqual(con.execute("""SELECT min(run_start_date)
                    FROM dim_release_run""").fetchone()[0], date(2019, 12, 31))

    def test_same_film_combines_runs_and_different_id_stays_separate(self) -> None:
        with tempfile.TemporaryDirectory() as root:
            csv_path = Path(root) / "revenues.csv"
            db_path = Path(root) / "warehouse.duckdb"
            with csv_path.open("w", newline="", encoding="utf-8") as handle:
                writer = csv.writer(handle)
                writer.writerow(["id", "date", "title", "revenue", "theaters", "distributor"])
                writer.writerows([
                    ["1", "2020-01-01", "Film A", "100", "10", "Studio"],
                    ["2", "2023-01-01", "Film A", "50", "5", "Studio"],
                ])
            load(csv_path, db_path)
            calls = []

            def fake_fetch(title: str, api_key: str) -> dict:
                calls.append(title)
                return {"Response": "True", "Type": "movie", "imdbID": "tt1111111",
                        "Title": "Film A", "Year": "2020", "Genre": "Drama",
                        "imdbRating": "7.0", "Runtime": "100 min"}

            enrich(db_path, "test-key", limit=1, daily_cap=1, fetcher=fake_fetch)
            enrich(db_path, "test-key", limit=1, daily_cap=1, fetcher=fake_fetch)
            self.assertEqual(calls, ["Film A"])
            with duckdb.connect(str(db_path), read_only=True) as con:
                second_key = con.execute("""
                    SELECT movie_key FROM dim_release_run ORDER BY run_start_date DESC LIMIT 1
                """).fetchone()[0]
                self.assertEqual(con.execute("SELECT COUNT(*) FROM dim_film").fetchone()[0], 1)

            decide(db_path, second_key, "accept_candidate", "Same production rereleased")
            with duckdb.connect(str(db_path), read_only=True) as con:
                films = film_ranking(con, date(2020, 1, 1), date(2023, 12, 31))
                self.assertEqual(len(films), 1)
                self.assertEqual((films[0]["imdb_id"], films[0]["revenue"],
                                  films[0]["periods"]), ("tt1111111", 150, 2))
                self.assertEqual(film_ranking(
                    con, date(2020, 1, 1), date(2023, 12, 31), genre="Drama"
                )[0]["revenue"], 150)
                self.assertEqual(len(movie_ranking(
                    con, date(2020, 1, 1), date(2023, 12, 31)
                )), 2)
                self.assertEqual(con.execute("SELECT COUNT(*) FROM v_run_film_match").fetchone()[0], 2)

            decide(db_path, second_key, "verify_id", "This is a remake", "tt2222222")
            with duckdb.connect(str(db_path), read_only=True) as con:
                films = film_ranking(con, date(2020, 1, 1), date(2023, 12, 31))
                self.assertEqual(len(films), 2)
                self.assertEqual({row["imdb_id"]: row["revenue"] for row in films},
                                 {"tt1111111": 100, "tt2222222": 50})
                self.assertEqual(con.execute("SELECT COUNT(*) FROM omdb_request_log").fetchone()[0], 1)
            before = backfill(db_path)
            after = backfill(db_path)
            self.assertEqual(before, after)
            self.assertEqual(after["confirmed_films"], 2)
            self.assertEqual(after["omdb_requests"], 1)
            undo(db_path, second_key)
            with duckdb.connect(str(db_path), read_only=True) as con:
                self.assertEqual(con.execute("SELECT COUNT(*) FROM v_run_film_match").fetchone()[0], 1)
                self.assertEqual(film_ranking(
                    con, date(2020, 1, 1), date(2023, 12, 31)
                )[0]["revenue"], 100)


if __name__ == "__main__":
    unittest.main()
