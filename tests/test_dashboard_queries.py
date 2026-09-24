import csv
import tempfile
import unittest
from datetime import date
from pathlib import Path

import duckdb

from dashboard.queries import distributor_ranking, monthly_trend, movie_ranking, overview
from pipeline.load_revenues import load


class DashboardQueryTests(unittest.TestCase):
    def test_rankings_and_coverage_use_full_fact(self) -> None:
        with tempfile.TemporaryDirectory() as root:
            csv_path = Path(root) / "revenues.csv"
            db_path = Path(root) / "warehouse.duckdb"
            with csv_path.open("w", newline="", encoding="utf-8") as handle:
                writer = csv.writer(handle)
                writer.writerow(["id", "date", "title", "revenue", "theaters", "distributor"])
                writer.writerows(
                    [
                        ["1", "2022-01-01", "Film A", "100", "10", "Studio X"],
                        ["2", "2022-01-02", "Film A", "50", "8", "Studio X"],
                        ["3", "2022-01-02", "Film B", "25", "2", "Studio Y"],
                    ]
                )
            load(csv_path, db_path)
            with duckdb.connect(str(db_path)) as con:
                con.execute(
                    "UPDATE dim_movie SET match_status = 'matched', "
                    "primary_genre = 'Drama' WHERE source_title = 'Film A'"
                )
                start, end = date(2022, 1, 1), date(2022, 1, 31)
                self.assertEqual(
                    overview(con, start, end),
                    {"revenue": 175, "matched_revenue": 150, "movies": 2, "days": 2},
                )
                self.assertEqual(
                    [(row["movie"], row["revenue"]) for row in movie_ranking(con, start, end)],
                    [("Film A", 150), ("Film B", 25)],
                )
                self.assertEqual(
                    overview(con, start, end, genre="Drama")["revenue"], 150
                )
                self.assertEqual(
                    [row["revenue"] for row in distributor_ranking(con, start, end, ["Studio Y"])],
                    [25],
                )
                self.assertEqual([row["revenue"] for row in monthly_trend(con, start, end)], [175])
                self.assertEqual(
                    [row["revenue"] for row in monthly_trend(con, start, date(2022, 3, 31))],
                    [175, 0, 0],
                )


if __name__ == "__main__":
    unittest.main()
