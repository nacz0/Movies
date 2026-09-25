"""Serve the local ranking dashboard without additional web dependencies."""

from __future__ import annotations

import argparse
import json
from contextlib import closing
from datetime import date
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlsplit

import duckdb

from dashboard.queries import (
    date_bounds,
    distributor_options,
    distributor_ranking,
    enrichment_status,
    film_ranking,
    genre_options,
    monthly_trend,
    movie_ranking,
    overview,
)


ROOT = Path(__file__).resolve().parents[1]
INDEX = (Path(__file__).parent / "index.html").read_bytes()


class DashboardServer(ThreadingHTTPServer):
    def __init__(self, address: tuple[str, int], db_path: Path):
        super().__init__(address, DashboardHandler)
        self.db_path = db_path


class DashboardHandler(BaseHTTPRequestHandler):
    server: DashboardServer

    def _send(self, status: int, body: bytes, content_type: str) -> None:
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def _json(self, status: int, value: dict) -> None:
        body = json.dumps(
            value, ensure_ascii=False,
            default=lambda item: item.isoformat() if isinstance(item, date) else str(item),
        ).encode("utf-8")
        self._send(status, body, "application/json; charset=utf-8")

    def do_GET(self) -> None:
        url = urlsplit(self.path)
        if url.path == "/":
            self._send(200, INDEX, "text/html; charset=utf-8")
            return
        if url.path == "/favicon.ico":
            self._send(204, b"", "image/x-icon")
            return
        if url.path not in ("/api/meta", "/api/dashboard"):
            self._json(404, {"error": "Not found"})
            return

        try:
            with closing(duckdb.connect(str(self.server.db_path), read_only=True)) as con:
                first, last = date_bounds(con)
                if url.path == "/api/meta":
                    self._json(200, {
                        "first_date": first,
                        "last_date": last,
                        "distributors": distributor_options(con),
                        "genres": genre_options(con),
                    })
                    return

                query = parse_qs(url.query)
                start = date.fromisoformat(query.get("start", [first.isoformat()])[0])
                end = date.fromisoformat(query.get("end", [last.isoformat()])[0])
                if start < first or end > last or start > end:
                    raise ValueError("Date range is outside the available data")
                distributors = query.get("distributor", [])
                if not set(distributors).issubset(set(distributor_options(con))):
                    raise ValueError("Unknown distributor")
                genre = query.get("genre", [None])[0] or None
                if genre and genre not in genre_options(con):
                    raise ValueError("Unknown genre")
                limit = int(query.get("limit", ["20"])[0])
                if limit not in (10, 20, 50, 100):
                    raise ValueError("Ranking size must be 10, 20, 50, or 100")

                movie_sort = query.get("movie_sort", ["revenue"])[0]
                movie_dir = query.get("movie_dir", ["desc"])[0]
                film_sort = query.get("film_sort", ["revenue"])[0]
                film_dir = query.get("film_dir", ["desc"])[0]
                distributor_sort = query.get("distributor_sort", ["revenue"])[0]
                distributor_dir = query.get("distributor_dir", ["desc"])[0]

                base = overview(con, start, end, distributors)
                selected = overview(con, start, end, distributors, genre)
                self._json(200, {
                    "base": base,
                    "selected": selected,
                    "coverage": base["matched_revenue"] / base["revenue"] if base["revenue"] else 0,
                    "enrichment": enrichment_status(con, start, end, distributors),
                    "movies": movie_ranking(con, start, end, distributors, genre, limit,
                                             movie_sort, movie_dir),
                    "bar_movies": movie_ranking(con, start, end, distributors, genre, 10),
                    "films": film_ranking(con, start, end, distributors, genre, limit,
                                          film_sort, film_dir),
                    "distributors": distributor_ranking(con, start, end, distributors,
                                                        genre, limit, distributor_sort,
                                                        distributor_dir),
                    "trend": monthly_trend(con, start, end, distributors, genre),
                })
        except (ValueError, TypeError) as exc:
            self._json(400, {"error": str(exc)})
        except Exception:
            self._json(500, {"error": "Dashboard query failed"})


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", type=Path, default=ROOT / "data" / "box_office.duckdb")
    parser.add_argument("--port", type=int, default=8501)
    args = parser.parse_args()
    if not args.db.is_file():
        parser.error(f"Database missing: {args.db}. Run the CSV loader first.")
    with DashboardServer(("127.0.0.1", args.port), args.db.resolve()) as server:
        print(f"Dashboard: http://127.0.0.1:{server.server_port}/", flush=True)
        try:
            server.serve_forever()
        except KeyboardInterrupt:
            pass


if __name__ == "__main__":
    main()
