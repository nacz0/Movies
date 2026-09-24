"""Apply or remove a reviewed split/join exception to the 180-day rule."""

from __future__ import annotations

import argparse
import json
from datetime import date
from pathlib import Path

import duckdb

from pipeline.load_revenues import ROOT, load


DEFAULT_DB = ROOT / "data" / "box_office.duckdb"
DEFAULT_CSV = ROOT / "revenues_per_day.csv"


def list_rules(db_path: Path) -> list[dict]:
    with duckdb.connect(str(db_path), read_only=True) as con:
        rows = con.execute("""SELECT source_title, boundary_date, action,
            reason, reviewed_at_utc FROM title_boundary_rule
            ORDER BY source_title, boundary_date""").fetchall()
    return [dict(zip(("source_title", "boundary_date", "action", "reason",
                      "reviewed_at_utc"), row)) for row in rows]


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", type=Path, default=DEFAULT_DB)
    parser.add_argument("--csv", type=Path, default=DEFAULT_CSV)
    commands = parser.add_subparsers(dest="command", required=True)
    for name in ("split", "join", "remove"):
        command = commands.add_parser(name)
        command.add_argument("--source-title", required=True)
        command.add_argument("--boundary-date", type=date.fromisoformat, required=True)
        command.add_argument("--reason", required=True)
    commands.add_parser("list")
    args = parser.parse_args()
    if args.command == "list":
        result = list_rules(args.db)
    else:
        result = load(args.csv, args.db,
                      boundary_change=(args.source_title, args.boundary_date,
                                       None if args.command == "remove" else args.command,
                                       args.reason))
    print(json.dumps(result, ensure_ascii=False, default=str, indent=2))


if __name__ == "__main__":
    main()
