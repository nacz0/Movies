"""Create review samples and score only labels verified by a human."""

from __future__ import annotations

import argparse
import csv
import json
import re
from pathlib import Path

import duckdb

from pipeline.load_revenues import ROOT


CASE_FIELDS = ["movie_key", "source_title", "run_start_date", "run_end_date",
               "revenue", "match_status", "predicted_imdb_id", "sample_group",
               "expected_imdb_id", "review_notes"]
BOUNDARY_FIELDS = ["source_title", "previous_date", "current_date", "gap_days",
                   "previous_movie_key", "current_movie_key", "predicted_same_run",
                   "expected_same_period", "review_notes"]


def _write_csv(path: Path, fields: list[str], rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8-sig") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def sample(db_path: Path, output_dir: Path, *, overwrite: bool = False) -> dict[str, int | str]:
    """Select diverse cases without claiming that their current IDs are correct."""
    if not db_path.is_file():
        raise FileNotFoundError(db_path)
    case_path = output_dir / "film_labels.csv"
    boundary_path = output_dir / "boundary_labels.csv"
    if not overwrite and (case_path.exists() or boundary_path.exists()):
        raise FileExistsError("Reference files already exist; use --force only after saving labels")
    with duckdb.connect(str(db_path), read_only=True) as con:
        rows = con.execute("""WITH active AS (
            SELECT m.movie_key, m.source_title, r.run_start_date, r.run_end_date,
                   SUM(f.revenue) AS revenue, m.match_status,
                   m.imdb_id AS predicted_imdb_id,
                   COUNT(*) OVER (PARTITION BY m.source_title) AS title_run_count
            FROM dim_movie m JOIN dim_release_run r USING (movie_key)
            JOIN fact_daily_revenue f USING (movie_key)
            GROUP BY m.movie_key, m.source_title, r.run_start_date,
                     r.run_end_date, m.match_status, m.imdb_id
        ) SELECT * FROM active ORDER BY revenue DESC, movie_key""").fetchall()
        names = [column[0] for column in con.description]
        active = [dict(zip(names, row)) for row in rows]
        selected: list[dict] = []
        seen: set[int] = set()
        groups = [
            ("ambiguous", 6, lambda row: row["match_status"] == "ambiguous"),
            ("multiple_periods", 9, lambda row: row["title_run_count"] > 1),
            ("matched", 8, lambda row: row["match_status"] == "matched"),
            ("pending", 7, lambda row: row["match_status"] == "pending"),
        ]
        for group, quota, predicate in groups:
            count = 0
            for row in active:
                if count >= quota:
                    break
                if row["movie_key"] in seen or not predicate(row):
                    continue
                seen.add(row["movie_key"])
                selected.append({field: row.get(field, "") for field in CASE_FIELDS})
                selected[-1].update(sample_group=group, expected_imdb_id="",
                                    review_notes="")
                count += 1

        pairs = con.execute("""WITH daily AS (
            SELECT r.source_title, r.revenue_date,
                   MIN(f.movie_key) AS movie_key,
                   SUM(r.revenue) AS revenue
            FROM raw_revenues r JOIN fact_daily_revenue f
              ON f.source_id = r.source_id
            GROUP BY r.source_title, r.revenue_date
        ), adjacent AS (
            SELECT source_title, revenue_date AS current_date,
                   LAG(revenue_date) OVER w AS previous_date,
                   movie_key AS current_movie_key,
                   LAG(movie_key) OVER w AS previous_movie_key,
                   revenue
            FROM daily WINDOW w AS (PARTITION BY source_title ORDER BY revenue_date)
        ), pairs AS (
          SELECT source_title, previous_date, current_date,
                 date_diff('day', previous_date, current_date) AS gap_days,
                 previous_movie_key, current_movie_key,
                 previous_movie_key = current_movie_key AS predicted_same_run,
                 revenue
          FROM adjacent WHERE previous_date IS NOT NULL
        ), ranked AS (
          SELECT *, ROW_NUMBER() OVER (
              PARTITION BY predicted_same_run
              ORDER BY ABS(gap_days - 180), revenue DESC, source_title
          ) AS choice FROM pairs
        ) SELECT source_title, previous_date, current_date, gap_days,
                 previous_movie_key, current_movie_key, predicted_same_run
          FROM ranked WHERE choice <= 10
          ORDER BY predicted_same_run DESC, choice""").fetchall()
        names = [column[0] for column in con.description]
        boundary_rows = [dict(zip(names, row)) for row in pairs]
        boundaries: list[dict] = []
        for same in (True, False):
            matching = [row for row in boundary_rows if row["predicted_same_run"] == same]
            for row in matching[:10]:
                boundaries.append({**row, "expected_same_period": "", "review_notes": ""})

    _write_csv(case_path, CASE_FIELDS, selected)
    _write_csv(boundary_path, BOUNDARY_FIELDS, boundaries)
    return {"film_cases": len(selected), "boundary_pairs": len(boundaries),
            "film_file": str(case_path), "boundary_file": str(boundary_path)}


def _read_csv(path: Path, fields: list[str]) -> list[dict[str, str]]:
    with path.open("r", newline="", encoding="utf-8-sig") as handle:
        reader = csv.DictReader(handle)
        if not set(fields).issubset(reader.fieldnames or []):
            raise ValueError(f"Missing expected columns in {path}")
        return list(reader)


def evaluate(film_path: Path, boundary_path: Path) -> dict:
    films = _read_csv(film_path, CASE_FIELDS)
    boundaries = _read_csv(boundary_path, BOUNDARY_FIELDS)
    labeled_films = []
    for row in films:
        expected = row["expected_imdb_id"].strip()
        if not expected:
            continue
        if not re.fullmatch(r"tt\d+", expected):
            raise ValueError(f"Invalid expected_imdb_id for movie_key {row['movie_key']}")
        labeled_films.append(row)
    correct = [row for row in labeled_films
               if row["predicted_imdb_id"].strip() == row["expected_imdb_id"].strip()]
    predicted = [row for row in labeled_films if row["predicted_imdb_id"].strip()]
    labeled_revenue = sum(int(row["revenue"]) for row in labeled_films)
    correct_revenue = sum(int(row["revenue"]) for row in correct)
    labeled_boundaries = []
    for row in boundaries:
        expected = row["expected_same_period"].strip().lower()
        if not expected:
            continue
        if expected not in ("true", "false"):
            raise ValueError("expected_same_period must be true, false or blank")
        labeled_boundaries.append((row["predicted_same_run"].strip().lower() == "true",
                                   expected == "true"))
    false_merges = sum(predicted_same and not expected_same
                       for predicted_same, expected_same in labeled_boundaries)
    false_splits = sum(not predicted_same and expected_same
                       for predicted_same, expected_same in labeled_boundaries)
    return {
        "film_cases_total": len(films),
        "film_cases_labeled": len(labeled_films),
        "imdb_precision_on_labeled_predictions":
            len(correct) / len(predicted) if predicted else None,
        "imdb_recall_on_labeled_cases":
            len(correct) / len(labeled_films) if labeled_films else None,
        "correctly_assigned_revenue_share_in_labeled_cases":
            correct_revenue / labeled_revenue if labeled_revenue else None,
        "boundary_pairs_total": len(boundaries),
        "boundary_pairs_labeled": len(labeled_boundaries),
        "false_merges": false_merges if labeled_boundaries else None,
        "false_splits": false_splits if labeled_boundaries else None,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    sampling = commands.add_parser("sample")
    sampling.add_argument("--db", type=Path, default=ROOT / "data" / "box_office.duckdb")
    sampling.add_argument("--output-dir", type=Path, default=ROOT / "quality")
    sampling.add_argument("--force", action="store_true",
                          help="Overwrite existing reference files and their labels")
    scoring = commands.add_parser("evaluate")
    scoring.add_argument("--film-file", type=Path,
                         default=ROOT / "quality" / "film_labels.csv")
    scoring.add_argument("--boundary-file", type=Path,
                         default=ROOT / "quality" / "boundary_labels.csv")
    args = parser.parse_args()
    result = (sample(args.db, args.output_dir, overwrite=args.force)
              if args.command == "sample"
              else evaluate(args.film_file, args.boundary_file))
    print(json.dumps(result, ensure_ascii=False, default=str, indent=2))


if __name__ == "__main__":
    main()
