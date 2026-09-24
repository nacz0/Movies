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


def review_cases(con: duckdb.DuckDBPyConnection, limit: int = 50) -> dict:
    if limit < 1 or limit > 500:
        raise ValueError("limit must be between 1 and 500")
    rows = _records(con.execute("""
        SELECT * FROM v_movie_match_review
        ORDER BY CASE WHEN match_status = 'ambiguous' THEN 0
                      WHEN match_status IN ('verified_id', 'rejected') THEN 1
                      ELSE 2 END,
                 total_revenue DESC, source_title, run_start_date
        LIMIT ?
    """, [limit]))
    total = con.execute("SELECT COUNT(*) FROM v_movie_match_review").fetchone()[0]
    return {"total": total, "cases": rows}


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
        SELECT r.display_title AS movie,
               SUM(f.revenue) AS revenue,
               COUNT(*) AS reported_days,
               m.primary_genre AS genre,
               m.imdb_rating,
               m.match_status
        FROM fact_daily_revenue f
        JOIN dim_date d ON d.date_key = f.date_key
        JOIN dim_movie m ON m.movie_key = f.movie_key
        JOIN dim_release_run r ON r.movie_key = m.movie_key
        JOIN dim_distributor x ON x.distributor_key = f.distributor_key
        WHERE {where}
        GROUP BY m.movie_key, r.display_title, m.primary_genre,
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
    """Rank confirmed productions, combining every linked revenue period."""
    if limit < 1 or limit > 100:
        raise ValueError("limit must be between 1 and 100")
    where, parameters = filters(start, end, distributors)
    if genre:
        where += " AND fd.primary_genre = ?"
        parameters.append(genre)
    return _records(con.execute(f"""
        SELECT fd.film_title || COALESCE(' (' || CAST(fd.release_year AS VARCHAR) || ')', '')
                   AS film,
               film.imdb_id, fd.release_year,
               SUM(f.revenue) AS revenue,
               COUNT(DISTINCT match.movie_key) AS periods,
               COUNT(DISTINCT d.calendar_date) AS reported_days,
               fd.primary_genre AS genre, fd.imdb_rating
        FROM fact_daily_revenue f
        JOIN dim_date d ON d.date_key = f.date_key
        JOIN dim_movie m ON m.movie_key = f.movie_key
        JOIN dim_distributor x ON x.distributor_key = f.distributor_key
        JOIN v_run_film_match match ON match.movie_key = f.movie_key
        JOIN dim_film film ON film.film_key = match.film_key
        JOIN film_details fd ON fd.film_key = film.film_key
        WHERE {where}
        GROUP BY film.film_key, film.imdb_id, fd.film_title,
                 fd.release_year, fd.primary_genre, fd.imdb_rating
        ORDER BY revenue DESC, film.imdb_id
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
