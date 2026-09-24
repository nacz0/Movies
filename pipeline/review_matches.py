"""Review title runs and record reversible human matching decisions offline."""

from __future__ import annotations

import argparse
import json
import re
from datetime import datetime, timezone
from pathlib import Path

import duckdb

from pipeline.enrich_omdb import classify, set_movie_metadata
from pipeline.film_identity import sync_films
from pipeline.load_revenues import ROOT


DEFAULT_DB = ROOT / "data" / "box_office.duckdb"
ACTIONS = ("accept_candidate", "reject_candidate", "verify_id")


def _connection(db_path: Path, *, read_only: bool = False) -> duckdb.DuckDBPyConnection:
    if not db_path.is_file():
        raise FileNotFoundError(db_path)
    return duckdb.connect(str(db_path), read_only=read_only)


def _records(cursor: duckdb.DuckDBPyConnection) -> list[dict]:
    names = [column[0] for column in cursor.description]
    return [dict(zip(names, row)) for row in cursor.fetchall()]


def list_cases(db_path: Path, *, limit: int = 30, unresolved_only: bool = False) -> list[dict]:
    if not 1 <= limit <= 500:
        raise ValueError("limit must be between 1 and 500")
    with _connection(db_path, read_only=True) as con:
        where = ("WHERE decision IS NULL AND "
                 "(match_status IN ('ambiguous', 'not_found') OR "
                 "(match_status = 'pending' AND title_run_count > 1) OR "
                 "boundary_changed_at_utc IS NOT NULL)"
                 if unresolved_only else "")
        return _records(con.execute(f"""
            SELECT * FROM v_movie_match_review {where}
            ORDER BY CASE WHEN match_status = 'ambiguous' THEN 0
                          WHEN match_status IN ('verified_id', 'rejected') THEN 1
                          ELSE 2 END,
                     total_revenue DESC, source_title, run_start_date
            LIMIT ?
        """, [limit]))


def _clear_metadata(con: duckdb.DuckDBPyConnection, movie_key: int, status: str,
                    imdb_id: str | None = None) -> None:
    con.execute("""
        UPDATE dim_movie SET imdb_id = ?, omdb_title = NULL,
            release_year = NULL, primary_genre = NULL, imdb_rating = NULL,
            runtime_minutes = NULL, match_status = ? WHERE movie_key = ?
    """, [imdb_id, status, movie_key])


def _candidate(con: duckdb.DuckDBPyConnection, movie_key: int) -> tuple[dict | None, str | None]:
    row = con.execute("""
        SELECT response_json, lookup_status FROM omdb_lookup WHERE movie_key = ?
    """, [movie_key]).fetchone()
    return (json.loads(row[0]) if row and row[0] else None,
            row[1] if row else None)


def decide(db_path: Path, movie_key: int, action: str, reason: str,
           imdb_id: str | None = None) -> dict:
    if action not in ACTIONS:
        raise ValueError(f"action must be one of: {', '.join(ACTIONS)}")
    reason = reason.strip()
    if not reason:
        raise ValueError("A review reason is required")
    if action == "verify_id":
        if not imdb_id or not re.fullmatch(r"tt\d+", imdb_id):
            raise ValueError("verify_id requires a valid IMDb ID such as tt1234567")
    elif imdb_id is not None:
        raise ValueError("--imdb-id is only valid with verify_id")

    with _connection(db_path) as con:
        con.execute("BEGIN TRANSACTION")
        try:
            run = con.execute("""
                SELECT m.source_title, r.run_start_date
                FROM dim_movie m JOIN dim_release_run r USING (movie_key)
                WHERE m.movie_key = ?
            """, [movie_key]).fetchone()
            if run is None:
                raise ValueError(f"Unknown movie_key: {movie_key}")
            payload, _ = _candidate(con, movie_key)
            if action == "accept_candidate":
                if (not payload or payload.get("Response") != "True"
                        or payload.get("Type") != "movie"
                        or not re.fullmatch(r"tt\d+", str(payload.get("imdbID", "")))
                        or not re.fullmatch(r"\d{4}", str(payload.get("Year", "")))):
                    raise ValueError("No valid cached OMDb movie candidate to accept")
                set_movie_metadata(con, movie_key, payload)
                imdb_id = payload["imdbID"]
                status = "matched"
            elif action == "verify_id":
                saved_detail = con.execute("""SELECT detail_json FROM omdb_candidate
                    WHERE movie_key = ? AND imdb_id = ?""", [movie_key, imdb_id]).fetchone()
                selected_payload = (json.loads(saved_detail[0])
                                    if saved_detail and saved_detail[0] else payload)
                if (selected_payload and selected_payload.get("Response") == "True"
                        and selected_payload.get("Type") == "movie"
                        and selected_payload.get("imdbID") == imdb_id
                        and re.fullmatch(r"\d{4}", str(selected_payload.get("Year", "")))):
                    set_movie_metadata(con, movie_key, selected_payload)
                    status = "matched"
                else:
                    _clear_metadata(con, movie_key, "verified_id", imdb_id)
                    status = "verified_id"
            else:
                _clear_metadata(con, movie_key, "rejected")
                status = "rejected"

            now = datetime.now(timezone.utc).replace(tzinfo=None)
            con.execute("""
                INSERT INTO movie_match_decision VALUES (?, ?, ?, ?, ?)
                ON CONFLICT (movie_key) DO UPDATE SET
                    decision = excluded.decision, imdb_id = excluded.imdb_id,
                    reason = excluded.reason, reviewed_at_utc = excluded.reviewed_at_utc
            """, [movie_key, action, imdb_id, reason, now])
            sync_films(con)
            con.execute("COMMIT")
        except Exception:
            con.execute("ROLLBACK")
            raise
    return {"movie_key": movie_key, "title": run[0],
            "run_start_date": str(run[1]), "action": action,
            "match_status": status, "imdb_id": imdb_id}


def undo(db_path: Path, movie_key: int) -> dict:
    with _connection(db_path) as con:
        con.execute("BEGIN TRANSACTION")
        try:
            run = con.execute("""
                SELECT m.source_title, r.run_start_date, r.run_end_date
                FROM dim_movie m JOIN dim_release_run r USING (movie_key)
                WHERE m.movie_key = ?
            """, [movie_key]).fetchone()
            if run is None:
                raise ValueError(f"Unknown movie_key: {movie_key}")
            if not con.execute("SELECT 1 FROM movie_match_decision WHERE movie_key = ?",
                               [movie_key]).fetchone():
                raise ValueError("This run has no manual decision")
            payload, lookup_status = _candidate(con, movie_key)
            if payload:
                status, _ = classify(payload, run[0], run[1].year, run[2].year)
            else:
                status = lookup_status or "pending"
                if status in ("quota_exhausted", "invalid_key"):
                    status = "pending"
            _clear_metadata(con, movie_key, status)
            if status == "matched" and payload:
                set_movie_metadata(con, movie_key, payload)
            con.execute("DELETE FROM movie_match_decision WHERE movie_key = ?", [movie_key])
            sync_films(con)
            con.execute("COMMIT")
        except Exception:
            con.execute("ROLLBACK")
            raise
    return {"movie_key": movie_key, "title": run[0],
            "run_start_date": str(run[1]), "match_status": status}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", type=Path, default=DEFAULT_DB)
    commands = parser.add_subparsers(dest="command", required=True)
    listing = commands.add_parser("list", help="List title runs needing review")
    listing.add_argument("--limit", type=int, default=30)
    listing.add_argument("--unresolved-only", action="store_true")
    decision = commands.add_parser("decide", help="Record or replace a human decision")
    decision.add_argument("--movie-key", type=int, required=True)
    decision.add_argument("--action", choices=ACTIONS, required=True)
    decision.add_argument("--reason", required=True)
    decision.add_argument("--imdb-id")
    reversal = commands.add_parser("undo", help="Restore the automatic result")
    reversal.add_argument("--movie-key", type=int, required=True)
    args = parser.parse_args()
    if args.command == "list":
        result = list_cases(args.db, limit=args.limit,
                            unresolved_only=args.unresolved_only)
    elif args.command == "decide":
        result = decide(args.db, args.movie_key, args.action, args.reason, args.imdb_id)
    else:
        result = undo(args.db, args.movie_key)
    print(json.dumps(result, ensure_ascii=False, default=str, indent=2))


if __name__ == "__main__":
    main()
