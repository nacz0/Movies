import csv
import tempfile
import unittest
from pathlib import Path

from pipeline.evaluate_matching import BOUNDARY_FIELDS, CASE_FIELDS, evaluate


class EvaluateMatchingTests(unittest.TestCase):
    def test_scores_only_verified_labels(self) -> None:
        with tempfile.TemporaryDirectory() as root:
            film_path = Path(root) / "films.csv"
            boundary_path = Path(root) / "boundaries.csv"
            with film_path.open("w", newline="", encoding="utf-8") as handle:
                writer = csv.DictWriter(handle, fieldnames=CASE_FIELDS)
                writer.writeheader()
                writer.writerows([
                    {"movie_key": "1", "source_title": "A", "run_start_date": "2020-01-01",
                     "run_end_date": "2020-01-02", "revenue": "100", "match_status": "matched",
                     "predicted_imdb_id": "tt111", "sample_group": "matched",
                     "expected_imdb_id": "tt111"},
                    {"movie_key": "2", "source_title": "B", "run_start_date": "2020-01-01",
                     "run_end_date": "2020-01-02", "revenue": "50", "match_status": "matched",
                     "predicted_imdb_id": "tt222", "sample_group": "matched",
                     "expected_imdb_id": "tt333"},
                    {"movie_key": "3", "source_title": "C", "run_start_date": "2020-01-01",
                     "run_end_date": "2020-01-02", "revenue": "500", "match_status": "pending",
                     "predicted_imdb_id": "", "sample_group": "pending",
                     "expected_imdb_id": ""},
                ])
            with boundary_path.open("w", newline="", encoding="utf-8") as handle:
                writer = csv.DictWriter(handle, fieldnames=BOUNDARY_FIELDS)
                writer.writeheader()
                writer.writerows([
                    {"source_title": "A", "previous_date": "2020-01-01",
                     "current_date": "2020-02-01", "gap_days": "31",
                     "previous_movie_key": "1", "current_movie_key": "1",
                     "predicted_same_run": "True", "expected_same_period": "false"},
                    {"source_title": "B", "previous_date": "2020-01-01",
                     "current_date": "2021-01-01", "gap_days": "366",
                     "previous_movie_key": "2", "current_movie_key": "3",
                     "predicted_same_run": "False", "expected_same_period": "true"},
                ])
            result = evaluate(film_path, boundary_path)
            self.assertEqual(result["film_cases_labeled"], 2)
            self.assertEqual(result["imdb_precision_on_labeled_predictions"], 0.5)
            self.assertEqual(result["imdb_recall_on_labeled_cases"], 0.5)
            self.assertAlmostEqual(result["correctly_assigned_revenue_share_in_labeled_cases"],
                                   2 / 3)
            self.assertEqual(result["false_merges"], 1)
            self.assertEqual(result["false_splits"], 1)


if __name__ == "__main__":
    unittest.main()
