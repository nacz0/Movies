"""Queries against the dimensional model; no API calls are made here."""

from __future__ import annotations

from datetime import date

import duckdb


def _records(cursor: duckdb.DuckDBPyConnection) -> list[dict]:
    columns = [column[0] for column in cursor.description]
    return [dict(zip(columns, row)) for row in cursor.fetchall()]


def filters(
    start: date,
    end: date,
    distributors: list[str] | None = None,
    genre: str | None = None,
) -> tuple[str, list]:
    clauses = ["d.calendar_date BETWEEN ? AND ?"]
    parameters: list = [start, end]
    if distributors:
        clauses.append("x.distributor_name IN (" + ", ".join("?" for _ in distributors) + ")")
        parameters.extend(distributors)
    if genre:
        clauses.append("m.primary_genre = ?")
        parameters.append(genre)
    return " AND ".join(clauses), parameters


def date_bounds(con: duckdb.DuckDBPyConnection) -> tuple[date, date]:
    return con.execute("SELECT MIN(calendar_date), MAX(calendar_date) FROM dim_date").fetchone()


def distributor_options(con: duckdb.DuckDBPyConnection) -> list[str]:
    return [row[0] for row in con.execute(
        "SELECT distributor_name FROM dim_distributor ORDER BY distributor_name"
    ).fetchall()]


def genre_options(con: duckdb.DuckDBPyConnection) -> list[str]:
    return [row[0] for row in con.execute(
        "SELECT DISTINCT primary_genre FROM dim_movie "
        "WHERE primary_genre IS NOT NULL ORDER BY primary_genre"
    ).fetchall()]


def overview(
    con: duckdb.DuckDBPyConnection,
    start: date,
    end: date,
    distributors: list[str] | None = None,
    genre: str | None = None,
) -> dict[str, int]:
    where, parameters = filters(start, end, distributors, genre)
    row = con.execute(
        f"""
        SELECT COALESCE(SUM(f.revenue), 0),
               COALESCE(SUM(f.revenue) FILTER (WHERE m.match_status = 'matched'), 0),
               COUNT(DISTINCT m.movie_key),
               COUNT(DISTINCT d.calendar_date)
        FROM fact_daily_revenue f
        JOIN dim_date d ON d.date_key = f.date_key
        JOIN dim_movie m ON m.movie_key = f.movie_key
        JOIN dim_distributor x ON x.distributor_key = f.distributor_key
        WHERE {where}
        """,
        parameters,
    ).fetchone()
    return dict(zip(("revenue", "matched_revenue", "movies", "days"), row))


def enrichment_status(
    con: duckdb.DuckDBPyConnection,
    start: date,
    end: date,
    distributors: list[str] | None = None,
) -> list[dict]:
    """Period and revenue coverage for the selected dates and distributors."""
    where, parameters = filters(start, end, distributors)
    return _records(con.execute(f"""
        SELECT m.match_status AS status,
               COUNT(DISTINCT m.movie_key) AS periods,
               SUM(f.revenue) AS revenue
        FROM fact_daily_revenue f
        JOIN dim_date d ON d.date_key = f.date_key
        JOIN dim_movie m ON m.movie_key = f.movie_key
        JOIN dim_distributor x ON x.distributor_key = f.distributor_key
        WHERE {where}
        GROUP BY m.match_status
        ORDER BY m.match_status
    """, parameters))


def movie_ranking(
    con: duckdb.DuckDBPyConnection,
    start: date,
    end: date,
    distributors: list[str] | None = None,
    genre: str | None = None,
    limit: int = 20,
):
    if limit < 1 or limit > 100:
        raise ValueError("limit must be between 1 and 100")
    where, parameters = filters(start, end, distributors, genre)
    return _records(con.execute(
        f"""
        SELECT m.display_title AS movie,
               SUM(f.revenue) AS revenue,
               COUNT(*) AS reported_days,
               m.primary_genre AS genre,
               m.imdb_rating,
               m.match_status
        FROM fact_daily_revenue f
        JOIN dim_date d ON d.date_key = f.date_key
        JOIN dim_movie m ON m.movie_key = f.movie_key
        JOIN dim_distributor x ON x.distributor_key = f.distributor_key
        WHERE {where}
        GROUP BY m.movie_key, m.display_title, m.primary_genre,
                 m.imdb_rating, m.match_status
        ORDER BY revenue DESC, movie
        LIMIT ?
        """,
        [*parameters, limit],
    ))


def film_ranking(
    con: duckdb.DuckDBPyConnection,
    start: date,
    end: date,
    distributors: list[str] | None = None,
    genre: str | None = None,
    limit: int = 20,
) -> list[dict]:
    """Rank matched IMDb IDs, combining their reporting periods."""
    if limit < 1 or limit > 100:
        raise ValueError("limit must be between 1 and 100")
    where, parameters = filters(start, end, distributors)
    if genre:
        where += " AND m.primary_genre = ?"
        parameters.append(genre)
    return _records(con.execute(f"""
        SELECT max(m.omdb_title) || COALESCE(' (' || CAST(max(m.release_year) AS VARCHAR) || ')', '')
                   AS film,
               m.imdb_id, max(m.release_year) AS release_year,
               SUM(f.revenue) AS revenue,
               COUNT(DISTINCT m.movie_key) AS periods,
               COUNT(DISTINCT d.calendar_date) AS reported_days,
               max(m.primary_genre) AS genre, max(m.imdb_rating) AS imdb_rating
        FROM fact_daily_revenue f
        JOIN dim_date d ON d.date_key = f.date_key
        JOIN dim_movie m ON m.movie_key = f.movie_key
        JOIN dim_distributor x ON x.distributor_key = f.distributor_key
        WHERE {where} AND m.match_status = 'matched' AND m.imdb_id IS NOT NULL
        GROUP BY m.imdb_id
        ORDER BY revenue DESC, m.imdb_id
        LIMIT ?
    """, [*parameters, limit]))


def distributor_ranking(
    con: duckdb.DuckDBPyConnection,
    start: date,
    end: date,
    distributors: list[str] | None = None,
    genre: str | None = None,
    limit: int = 20,
):
    where, parameters = filters(start, end, distributors, genre)
    return _records(con.execute(
        f"""
        SELECT x.distributor_name AS distributor,
               SUM(f.revenue) AS revenue,
               COUNT(DISTINCT m.movie_key) AS movies
        FROM fact_daily_revenue f
        JOIN dim_date d ON d.date_key = f.date_key
        JOIN dim_movie m ON m.movie_key = f.movie_key
        JOIN dim_distributor x ON x.distributor_key = f.distributor_key
        WHERE {where}
        GROUP BY x.distributor_name
        ORDER BY revenue DESC, distributor
        LIMIT ?
        """,
        [*parameters, limit],
    ))


def monthly_trend(
    con: duckdb.DuckDBPyConnection,
    start: date,
    end: date,
    distributors: list[str] | None = None,
    genre: str | None = None,
):
    where, parameters = filters(start, end, distributors, genre)
    return _records(con.execute(
        f"""
        WITH monthly AS (
            SELECT CAST(date_trunc('month', d.calendar_date) AS DATE) AS month,
                   SUM(f.revenue) AS revenue
            FROM fact_daily_revenue f
            JOIN dim_date d ON d.date_key = f.date_key
            JOIN dim_movie m ON m.movie_key = f.movie_key
            JOIN dim_distributor x ON x.distributor_key = f.distributor_key
            WHERE {where}
            GROUP BY 1
        )
        SELECT CAST(s.month AS DATE) AS month, COALESCE(m.revenue, 0) AS revenue
        FROM generate_series(
            date_trunc('month', ?::DATE), date_trunc('month', ?::DATE),
            INTERVAL 1 MONTH
        ) AS s(month)
        LEFT JOIN monthly m ON m.month = CAST(s.month AS DATE)
        ORDER BY 1
        """,
        [*parameters, start, end],
    ))
